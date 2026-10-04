"""Cấu hình đọc từ biến môi trường và kiểm tra ngay lúc khởi động (fail fast)."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_CONFIG = 78  # EX_CONFIG

_ROOT = Path(__file__).resolve().parent.parent


class ConfigError(Exception):
    pass


def _raw(name: str) -> str | None:
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else None


def _str(name: str, default: str) -> str:
    return _raw(name) or default


def _required(name: str, errors: list[str]) -> str:
    v = _raw(name)
    if v is None:
        errors.append(f"thiếu biến bắt buộc {name}")
        return ""
    return v


def _num(name: str, default, cast, minimum, errors: list[str]):
    raw = _raw(name)
    if raw is None:
        return default
    try:
        v = cast(raw)
    except ValueError:
        errors.append(f"{name}={raw!r} không phải số hợp lệ")
        return default
    if minimum is not None and v < minimum:
        errors.append(f"{name}={v} phải >= {minimum}")
    return v


def _extra(name: str, errors: list[str]) -> dict[str, str]:
    """KAFKA_EXTRA_CONFIG="security.protocol=SASL_SSL,sasl.mechanism=PLAIN,..." """
    raw = _raw(name)
    out: dict[str, str] = {}
    if raw is None:
        return out
    for pair in raw.split(","):
        if "=" not in pair:
            errors.append(f"{name}: mục {pair.strip()!r} phải có dạng key=value")
            continue
        k, v = pair.split("=", 1)
        out[k.strip()] = v.strip()
    return out


@dataclass(frozen=True)
class Config:
    bootstrap_servers: str
    schema_registry_url: str
    database_url: str
    topic: str
    dlq_topic: str
    consumer_group: str
    replay_group: str
    batch_size: int
    max_poll_interval_ms: int
    db_retry_deadline_s: float
    retry_base_s: float
    retry_cap_s: float
    produce_rate: float
    topic_partitions: int
    topic_replication_factor: int
    topic_min_isr: int
    topic_retention_ms: int
    dlq_retention_ms: int
    schema_path: str
    log_level: str
    schema_registry_auth: str | None = None
    kafka_extra: dict = field(default_factory=dict, repr=False)  # có thể chứa mật khẩu


def load_config(*, need_db: bool = False) -> Config:
    e: list[str] = []
    cfg = Config(
        bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS", e),
        schema_registry_url=_required("SCHEMA_REGISTRY_URL", e),
        database_url=_required("DATABASE_URL", e) if need_db else (_raw("DATABASE_URL") or ""),
        topic=_str("KAFKA_TOPIC", "rides"),
        dlq_topic=_str("KAFKA_DLQ_TOPIC", "rides.dlq"),
        consumer_group=_str("KAFKA_CONSUMER_GROUP", "ride-writer"),
        replay_group=_str("REPLAY_GROUP", "ride-replay"),
        batch_size=_num("CONSUMER_BATCH_SIZE", 500, int, 1, e),
        max_poll_interval_ms=_num("CONSUMER_MAX_POLL_INTERVAL_MS", 300_000, int, 10_000, e),
        db_retry_deadline_s=_num("DB_RETRY_DEADLINE_S", 120.0, float, 1, e),
        retry_base_s=_num("RETRY_BASE_S", 0.5, float, 0.01, e),
        retry_cap_s=_num("RETRY_CAP_S", 10.0, float, 0.1, e),
        produce_rate=_num("PRODUCE_RATE", 200.0, float, 0, e),
        topic_partitions=_num("TOPIC_PARTITIONS", 3, int, 1, e),
        topic_replication_factor=_num("TOPIC_REPLICATION_FACTOR", 1, int, 1, e),
        topic_min_isr=_num("TOPIC_MIN_ISR", 1, int, 1, e),
        topic_retention_ms=_num("TOPIC_RETENTION_MS", 7 * 86_400_000, int, 1, e),
        dlq_retention_ms=_num("DLQ_RETENTION_MS", 30 * 86_400_000, int, 1, e),
        schema_path=_str("SCHEMA_PATH", str(_ROOT / "schemas" / "ride.avsc")),
        log_level=_str("LOG_LEVEL", "INFO").upper(),
        schema_registry_auth=_raw("SCHEMA_REGISTRY_AUTH"),
        kafka_extra=_extra("KAFKA_EXTRA_CONFIG", e),
    )
    if cfg.topic == cfg.dlq_topic:
        e.append("KAFKA_TOPIC và KAFKA_DLQ_TOPIC không được trùng nhau")
    if cfg.topic_min_isr > cfg.topic_replication_factor:
        e.append("TOPIC_MIN_ISR không được lớn hơn TOPIC_REPLICATION_FACTOR")
    if cfg.db_retry_deadline_s * 1000 > cfg.max_poll_interval_ms * 0.6:
        e.append(
            "DB_RETRY_DEADLINE_S phải <= 60% CONSUMER_MAX_POLL_INTERVAL_MS, "
            "nếu không consumer bị đá khỏi group khi đang retry"
        )
    if cfg.log_level not in logging.getLevelNamesMapping():
        e.append(f"LOG_LEVEL={cfg.log_level!r} không hợp lệ")
    if e:
        raise ConfigError("\n  - ".join(["cấu hình không hợp lệ:"] + e))
    return cfg


def kafka_conf(cfg: Config) -> dict:
    """Cấu hình librdkafka dùng chung (thêm SASL/SSL qua KAFKA_EXTRA_CONFIG)."""
    return {"bootstrap.servers": cfg.bootstrap_servers, **cfg.kafka_extra}


def registry_conf(cfg: Config) -> dict:
    conf = {"url": cfg.schema_registry_url}
    if cfg.schema_registry_auth:
        conf["basic.auth.user.info"] = cfg.schema_registry_auth
    return conf


def load_schema(cfg: Config) -> str:
    try:
        return Path(cfg.schema_path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"không đọc được schema {cfg.schema_path}: {exc}") from exc


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
