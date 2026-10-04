"""Avro + Schema Registry. Producer KHÔNG tự đăng ký schema (auto.register.schemas=False);
việc đăng ký làm bằng lệnh riêng: python -m ride_pipeline.admin schema
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, TypeVar

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from .config import Config, registry_conf
from .errors import BadMessage, FatalError, TransientError
from .models import Ride

T = TypeVar("T")
_NETWORK_PKGS = {"httpx", "httpcore", "requests", "urllib3", "socket", "ssl"}


def subject_for(topic: str) -> str:
    return f"{topic}-value"  # TopicNameStrategy (mặc định của serializer)


def is_transient_registry_error(exc: BaseException) -> bool:
    """Lỗi do mạng/Registry tạm thời (khác với dữ liệu hỏng)."""
    if isinstance(exc, SchemaRegistryError):
        code = exc.http_status_code
        return code is None or code >= 500 or code in (408, 429)
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    return type(exc).__module__.split(".")[0] in _NETWORK_PKGS


def call_registry(fn: Callable[[], T]) -> T:
    """Chạy lời gọi tới Registry; lỗi tạm thời đổi thành TransientError để retry."""
    try:
        return fn()
    except Exception as exc:
        if is_transient_registry_error(exc):
            raise TransientError(f"schema registry: {exc!r}") from exc
        raise


class RideSerializer:
    """Ride -> bytes Avro (magic byte + schema id + payload)."""

    def __init__(self, cfg: Config, schema_str: str):
        self.subject = subject_for(cfg.topic)
        self._schema = Schema(schema_str, "AVRO")
        self._ctx = SerializationContext(cfg.topic, MessageField.VALUE)
        self._client = SchemaRegistryClient(registry_conf(cfg))
        self._avro = AvroSerializer(
            self._client, schema_str,
            to_dict=lambda ride, ctx: ride.to_avro_dict(),
            conf={"auto.register.schemas": False},
        )

    def verify(self) -> None:
        """Gọi lúc khởi động: schema phải có sẵn trong Registry; làm ấm cache để về sau
        không phải gọi mạng (lỗi Registry không bị nhầm thành 'dữ liệu hỏng')."""
        def _check():
            self._client.lookup_schema(self.subject, self._schema)
            sample = Ride.create(ride_id="verify", rider_id="verify", route="verify",
                                 fare=Decimal("1.00"), status="requested",
                                 event_time=datetime.now(timezone.utc))
            self._avro(sample, self._ctx)
        try:
            call_registry(_check)
        except SchemaRegistryError as exc:
            raise FatalError(
                f"schema chưa đăng ký hoặc không khớp cho subject {self.subject!r} "
                f"({exc.error_message}). Chạy: python -m ride_pipeline.admin schema"
            ) from exc

    def __call__(self, ride: Ride) -> bytes:
        try:
            return self._avro(ride, self._ctx)
        except (SerializationError, ValueError, TypeError) as exc:
            raise BadMessage("serialization", str(exc)) from exc
        except Exception as exc:
            if is_transient_registry_error(exc):
                raise TransientError(str(exc)) from exc
            raise


class RideDeserializer:
    """bytes Avro -> Ride. Dữ liệu hỏng -> BadMessage; Registry lỗi tạm -> TransientError."""

    def __init__(self, cfg: Config, schema_str: str):
        self._client = SchemaRegistryClient(registry_conf(cfg))
        # schema_str là reader schema: consumer đọc được message ghi bằng schema cũ/mới tương thích
        self._avro = AvroDeserializer(self._client, schema_str)

    def __call__(self, msg) -> Ride:
        value = msg.value()
        if not value:
            raise BadMessage("empty_value", "message không có giá trị")
        try:
            record = self._avro(value, SerializationContext(msg.topic(), MessageField.VALUE))
        except SerializationError as exc:
            raise BadMessage("avro_decode", str(exc)) from exc
        except Exception as exc:
            if is_transient_registry_error(exc):
                raise TransientError(str(exc)) from exc
            if isinstance(exc, SchemaRegistryError):
                raise BadMessage("schema_lookup", f"{exc.http_status_code}: {exc.error_message}") from exc
            raise BadMessage("avro_decode", f"{type(exc).__name__}: {exc}") from exc
        try:
            return Ride.from_avro_dict(record)
        except ValueError as exc:
            raise BadMessage("validation", str(exc)) from exc
