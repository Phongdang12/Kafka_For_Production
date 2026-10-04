"""Dead Letter Queue: giữ NGUYÊN bytes gốc của message hỏng + header mô tả lỗi và vị trí gốc.

Phải flush() thành công TRƯỚC khi commit offset; nếu không, message hỏng vừa mất khỏi
pipeline vừa chưa vào DLQ.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Producer

from .errors import DLQError

log = logging.getLogger("dlq")

H_KIND = "dlq.error.kind"
H_MESSAGE = "dlq.error.message"
H_TOPIC = "dlq.source.topic"
H_PARTITION = "dlq.source.partition"
H_OFFSET = "dlq.source.offset"
H_TIME = "dlq.failed_at"


class DeadLetterQueue:
    def __init__(self, kafka_conf: dict, topic: str):
        self._topic = topic
        self._failed = 0
        self._fatal = False
        self._producer = Producer({
            **kafka_conf,
            "client.id": "ride-dlq-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
            "delivery.timeout.ms": 60_000,
            "error_cb": self._on_error,
        })

    def _on_error(self, err: KafkaError) -> None:
        log.error("dlq kafka error: %s", err)
        if err.fatal():
            self._fatal = True

    def _on_delivery(self, err: KafkaError | None, msg) -> None:
        if err is not None:
            self._failed += 1
            log.error("dlq delivery failed: %s", err)

    def send(self, msg, kind: str, error: str) -> None:
        """Gửi bytes gốc của `msg` (confluent Message) vào DLQ, giữ nguyên key."""
        headers = [
            (H_KIND, kind),
            (H_MESSAGE, str(error)[:1000]),
            (H_TOPIC, msg.topic()),
            (H_PARTITION, str(msg.partition())),
            (H_OFFSET, str(msg.offset())),
            (H_TIME, datetime.now(timezone.utc).isoformat()),
        ]
        while True:
            try:
                self._producer.produce(self._topic, key=msg.key(), value=msg.value(),
                                       headers=headers, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout)
        failed, self._failed = self._failed, 0
        if self._fatal or remaining or failed:
            raise DLQError(f"không ghi được DLQ: còn {remaining} chưa gửi, {failed} lỗi, fatal={self._fatal}")
