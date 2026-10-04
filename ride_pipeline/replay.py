"""Replay: đọc DLQ và gửi lại vào topic chính (giữ nguyên key và bytes gốc).

  python -m ride_pipeline.replay --limit 100 --dry-run   # chỉ xem, không gửi, không commit
  python -m ride_pipeline.replay --limit 100             # gửi thật rồi commit offset replay

Chỉ replay SAU KHI đã sửa nguyên nhân (schema/code/dữ liệu); message vẫn hỏng sẽ quay lại DLQ.
Offset replay lưu theo group REPLAY_GROUP nên lần chạy sau tiếp tục từ chỗ dừng.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from . import config
from .config import EXIT_CONFIG, EXIT_ERROR, EXIT_OK, ConfigError

log = logging.getLogger("replay")
_stop = threading.Event()


def main(argv=None) -> int:
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Replay DLQ vào topic chính")
    ap.add_argument("--limit", type=int, required=True, help="số message tối đa")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--idle-timeout", type=float, default=15.0, help="dừng khi không còn message mới (giây)")
    args = ap.parse_args(argv)
    try:
        cfg = config.load_config()
    except ConfigError as exc:
        log.error("%s", exc)
        return EXIT_CONFIG

    signal.signal(signal.SIGINT, lambda *_: _stop.set())
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_: _stop.set())

    consumer = Consumer({
        **config.kafka_conf(cfg),
        "group.id": cfg.replay_group,
        "client.id": "ride-replay",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = None if args.dry_run else Producer({
        **config.kafka_conf(cfg), "client.id": "ride-replay-producer",
        "acks": "all", "enable.idempotence": True,
    })
    state = {"failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            state["failed"] += 1
            log.error("replay delivery failed: %s", err)

    processed = 0
    last_seen = time.monotonic()
    rc = EXIT_OK
    try:
        consumer.subscribe([cfg.dlq_topic])
        while not _stop.is_set() and processed < args.limit:
            msgs = consumer.consume(num_messages=min(100, args.limit - processed), timeout=1.0)
            if not msgs:
                if time.monotonic() - last_seen > args.idle_timeout:
                    break
                continue
            last_seen = time.monotonic()
            offsets: dict[tuple[str, int], int] = {}
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consume error: %s", msg.error())
                    continue
                hdrs = {k: (v or b"").decode("utf-8", "replace") for k, v in (msg.headers() or [])}
                log.info("%s dlq@%d kind=%s from=%s[%s]@%s: %s",
                         "DRY" if args.dry_run else "REPLAY", msg.offset(),
                         hdrs.get("dlq.error.kind"), hdrs.get("dlq.source.topic"),
                         hdrs.get("dlq.source.partition"), hdrs.get("dlq.source.offset"),
                         hdrs.get("dlq.error.message", "")[:200])
                if producer is not None:
                    headers = list(msg.headers() or []) + [
                        ("replay.at", datetime.now(timezone.utc).isoformat())]
                    while True:
                        try:
                            producer.produce(cfg.topic, key=msg.key(), value=msg.value(),
                                             headers=headers, on_delivery=on_delivery)
                            break
                        except BufferError:
                            producer.poll(0.5)
                    producer.poll(0)
                processed += 1
                key = (msg.topic(), msg.partition())
                offsets[key] = max(offsets.get(key, -1), msg.offset() + 1)
            if producer is not None:
                remaining = producer.flush(30)
                if remaining or state["failed"]:
                    log.error("replay thất bại (còn %d chưa gửi, %d lỗi): không commit", remaining, state["failed"])
                    rc = EXIT_ERROR
                    break
                if offsets:
                    consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()],
                                    asynchronous=False)
    except Exception:
        log.exception("replay gặp lỗi")
        rc = EXIT_ERROR
    finally:
        consumer.close()
    log.info("%s xong: %d message", "dry-run" if args.dry_run else "replay", processed)
    return rc


if __name__ == "__main__":
    sys.exit(main())
