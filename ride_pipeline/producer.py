"""Producer: nguồn (synthetic | stdin/JR) -> validate + serialize Avro -> produce -> poll -> flush.

Chạy:
  python -m ride_pipeline.producer --count 10000
  jr run ... | python -m ride_pipeline.producer --source stdin

Thoát khi: hết dữ liệu / đủ --count / SIGINT, SIGTERM / error_cb báo lỗi fatal
(idempotent producer hỏng thì không chạy tiếp ở trạng thái hỏng, để được khởi động lại).
"""
from __future__ import annotations

import argparse
import json
import logging
import queue
import signal
import sys
import threading
import time
from typing import Iterator

from confluent_kafka import KafkaError, Producer

from . import config
from .config import EXIT_CONFIG, EXIT_ERROR, EXIT_OK, ConfigError
from .errors import BadMessage, FatalError, TransientError
from .models import Ride, generate_rides
from .retry import retry_with_backoff
from .serde import RideSerializer

log = logging.getLogger("producer")
_stop = threading.Event()
_fatal = threading.Event()


def _request_stop(*_):
    _stop.set()


class DeliveryStats:
    def __init__(self):
        self.delivered = 0
        self.failed = 0

    def on_delivery(self, err: KafkaError | None, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed key=%s: %s", msg.key(), err)
        else:
            self.delivered += 1


def on_error(err: KafkaError) -> None:
    # Lỗi mức kết nối: librdkafka tự thử lại. Lỗi fatal thì phải thoát.
    log.error("kafka error: %s", err)
    if err.fatal():
        _fatal.set()


def produce_blocking(producer: Producer, topic: str, key: bytes, value: bytes, cb) -> bool:
    """Hàng đợi đầy (BufferError) thì poll để giải phóng chỗ rồi thử lại (backpressure).
    Trả về False nếu bị yêu cầu dừng / có lỗi fatal trong lúc chờ."""
    while True:
        try:
            producer.produce(topic=topic, key=key, value=value, on_delivery=cb)
            return True
        except BufferError:
            if _stop.is_set() or _fatal.is_set():
                return False
            producer.poll(0.5)


class StdinSource:
    """Đọc từng dòng JSON (ví dụ do JR sinh). Đọc stdin là thao tác chặn nên chạy trong
    thread riêng; vòng chính vẫn bắt được Ctrl+C/SIGTERM và vẫn flush().
    Dòng không hợp lệ: ghi log, đếm, bỏ qua. Yield None khi chờ lâu để vòng chính còn poll()."""

    def __init__(self):
        self.invalid = 0
        self._q: queue.Queue[str | None] = queue.Queue(maxsize=10_000)  # đầy thì chặn đọc (backpressure)
        threading.Thread(target=self._read, name="stdin-reader", daemon=True).start()

    def _read(self) -> None:
        try:
            for line in sys.stdin:
                self._q.put(line)
        finally:
            self._q.put(None)  # EOF

    def __iter__(self) -> Iterator[Ride | None]:
        while not _stop.is_set():
            try:
                line = self._q.get(timeout=0.5)
            except queue.Empty:
                yield None
                continue
            if line is None:
                return
            if not line.strip():
                continue
            try:
                yield Ride.from_json_dict(json.loads(line))
            except ValueError as exc:  # JSONDecodeError cũng là ValueError
                self.invalid += 1
                if self.invalid <= 20 or self.invalid % 1000 == 0:
                    log.warning("bỏ qua dòng không hợp lệ #%d: %s | %.200s",
                                self.invalid, exc, line.strip())


def main(argv=None) -> int:
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Gửi ride events vào Kafka (Avro)")
    ap.add_argument("--source", choices=["synthetic", "stdin"], default="synthetic")
    ap.add_argument("--count", type=int, default=0, help="số message cần gửi (0 = không giới hạn)")
    ap.add_argument("--rate", type=float, default=None,
                    help="message/giây, chỉ áp dụng cho synthetic (0 = tối đa)")
    args = ap.parse_args(argv)

    try:
        cfg = config.load_config()
        schema_str = config.load_schema(cfg)
    except ConfigError as exc:
        log.error("%s", exc)
        return EXIT_CONFIG
    rate = cfg.produce_rate if args.rate is None else args.rate

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_stop)

    serializer = RideSerializer(cfg, schema_str)
    try:
        retry_with_backoff(serializer.verify, retriable=(TransientError,), deadline_s=60,
                           base_s=cfg.retry_base_s, cap_s=cfg.retry_cap_s,
                           what="schema registry", stop=_stop.is_set, log=log)
    except FatalError as exc:
        log.error("%s", exc)
        return EXIT_ERROR
    except Exception:
        log.exception("không kiểm tra được schema")
        return EXIT_ERROR

    producer = Producer({
        **config.kafka_conf(cfg),
        "client.id": "ride-producer",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
        "delivery.timeout.ms": 120_000,
        "error_cb": on_error,
        "retry.backoff.ms": 500
    })
    stats = DeliveryStats()
    source = StdinSource() if args.source == "stdin" else generate_rides()
    sent = invalid = 0
    rc = EXIT_OK
    t0 = time.monotonic()
    next_t = t0
    try:
        for ride in source:
            if _stop.is_set() or _fatal.is_set() or (args.count and sent >= args.count):
                break
            if ride is None:  # stdin đang chờ dữ liệu
                producer.poll(0)
                continue
            try:
                value = serializer(ride)
            except BadMessage as exc:
                invalid += 1
                log.warning("bỏ qua ride không serialize được: %s", exc)
                continue
            # key = ride_id: cùng ride luôn vào cùng partition (giữ đúng thứ tự)
            if not produce_blocking(producer, cfg.topic, ride.ride_id.encode(), value, stats.on_delivery):
                break
            sent += 1
            producer.poll(0)  # chạy các on_delivery đang chờ
            if rate and args.source == "synthetic":
                next_t += 1.0 / rate
                delay = next_t - time.monotonic()
                if delay > 0:
                    _stop.wait(delay)
                else:
                    next_t = time.monotonic()  # chậm hơn nhịp thì không dồn bù
    except Exception:
        log.exception("producer gặp lỗi")
        rc = EXIT_ERROR
    finally:
        remaining = producer.flush(30)  # chờ gửi hết trước khi thoát
        invalid += getattr(source, "invalid", 0)
        log.info("sent=%d delivered=%d failed=%d invalid=%d unflushed=%d in %.1fs",
                 sent, stats.delivered, stats.failed, invalid, remaining, time.monotonic() - t0)
    if _fatal.is_set():
        log.error("kafka báo lỗi fatal, thoát để được khởi động lại")
        rc = EXIT_ERROR
    if stats.failed or remaining:
        rc = EXIT_ERROR
    return rc


if __name__ == "__main__":
    sys.exit(main())
