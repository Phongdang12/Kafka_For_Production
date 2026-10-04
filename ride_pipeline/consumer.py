"""Consumer: consume(batch) -> kiểm tra lỗi -> deserialize (Avro) -> ghi DB -> DLQ -> commit.

Bất biến an toàn:
  * Chỉ commit offset của batch ĐÃ xử lý xong (ghi DB xong, message hỏng đã vào DLQ và flush).
    Commit theo offset tường minh, không dùng vị trí đọc hiện tại của librdkafka, nên khi lỗi
    giữa chừng thì batch dở dang không bao giờ bị commit.
  * Lỗi DB tạm thời -> retry backoff (tổng thời gian < max.poll.interval.ms); quá hạn thì thoát.
  * DLQ không ghi được -> không commit, thoát.
  * Xử lý lại sau restart/rebalance an toàn nhờ upsert theo ride_id.

Chạy: python -m ride_pipeline.consumer
"""
from __future__ import annotations

import logging
import signal
import sys
import threading

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from . import config
from .config import EXIT_CONFIG, EXIT_ERROR, EXIT_OK, ConfigError
from .db import DATA_DB_ERRORS, TRANSIENT_DB_ERRORS, RideSink
from .dlq import DeadLetterQueue
from .errors import BadMessage, FatalError, TransientError
from .retry import retry_with_backoff
from .serde import RideDeserializer

log = logging.getLogger("consumer")


class ConsumerApp:
    def __init__(self, cfg: config.Config, schema_str: str):
        self.cfg = cfg
        self._stop = threading.Event()
        self._fatal: KafkaError | None = None
        self._pending: dict[tuple[str, int], int] = {}  # (topic, partition) -> offset kế tiếp cần commit
        self.written = self.dead = 0

        self.deserialize = RideDeserializer(cfg, schema_str)
        self.sink = RideSink(cfg.database_url)
        self.dlq = DeadLetterQueue(config.kafka_conf(cfg), cfg.dlq_topic)
        self.consumer = Consumer({
            **config.kafka_conf(cfg),
            "group.id": cfg.consumer_group,
            "client.id": "ride-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 45_000,
            "max.poll.interval.ms": cfg.max_poll_interval_ms,
            "partition.assignment.strategy": "cooperative-sticky",
            "error_cb": self._on_error,
        })

    # ---- điều khiển ----
    def request_stop(self, *_):
        self._stop.set()

    def _retry(self, fn, retriable, what):
        return retry_with_backoff(
            fn, retriable=retriable, what=what, stop=self._stop.is_set, log=log,
            deadline_s=self.cfg.db_retry_deadline_s,
            base_s=self.cfg.retry_base_s, cap_s=self.cfg.retry_cap_s,
        )

    # ---- callback của librdkafka (chạy bên trong consume(), giữa 2 batch) ----
    def _on_error(self, err: KafkaError) -> None:
        log.error("kafka error: %s", err)
        if err.fatal():
            self._fatal = err

    def _on_assign(self, consumer, partitions):
        log.info("assigned: %s", [(p.topic, p.partition) for p in partitions])

    def _on_revoke(self, consumer, partitions):
        keys = {(p.topic, p.partition) for p in partitions}
        log.info("revoked: %s", sorted(keys))
        self._commit_pending(only=keys)  # dọn offset còn treo (nếu lần commit trước thất bại)
        for k in keys:
            self._pending.pop(k, None)

    def _on_lost(self, consumer, partitions):
        # Mất partition (đã bị group thu hồi): không được commit nữa, người khác sẽ xử lý lại.
        keys = {(p.topic, p.partition) for p in partitions}
        log.warning("partitions lost: %s", sorted(keys))
        for k in keys:
            self._pending.pop(k, None)

    # ---- xử lý một batch ----
    def _deserialize(self, msg):
        return self._retry(lambda: self.deserialize(msg), (TransientError,), "schema registry")

    def _upsert(self, params):
        self._retry(lambda: self.sink.upsert_batch(params), TRANSIENT_DB_ERRORS, "postgres")

    def _write(self, rows):
        """Ghi DB. Trả về danh sách (msg, kind, detail) của các dòng hỏng về dữ liệu."""
        params = [(ride, m.partition(), m.offset()) for ride, m in rows]
        try:
            self._upsert(params)
            return []
        except DATA_DB_ERRORS as exc:
            log.warning("batch %d dòng lỗi dữ liệu (%s), tách từng dòng để tìm dòng hỏng", len(rows), exc)
        bad = []
        for ride, m in rows:
            try:
                self._upsert([(ride, m.partition(), m.offset())])
            except DATA_DB_ERRORS as exc:
                bad.append((m, "db_data_error", f"{type(exc).__name__}: {exc}"))
        return bad

    def process_batch(self, messages) -> None:
        handled, rows, dead = [], [], []
        for msg in messages:
            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                if err.fatal():
                    raise FatalError(f"kafka fatal: {err}")
                log.warning("consume error: %s", err)
                continue
            handled.append(msg)
            try:
                rows.append((self._deserialize(msg), msg))
            except BadMessage as exc:
                dead.append((msg, exc.kind, exc.detail))

        if rows:
            dead.extend(self._write(rows))  # lỗi hạ tầng kéo dài -> RetryExhausted -> không commit

        for msg, kind, detail in dead:
            log.error("dead-letter %s[%d]@%d kind=%s: %s",
                      msg.topic(), msg.partition(), msg.offset(), kind, detail)
            self.dlq.send(msg, kind, detail)
        if dead:
            self.dlq.flush()  # PHẢI xong trước khi commit

        for msg in handled:
            key = (msg.topic(), msg.partition())
            self._pending[key] = max(self._pending.get(key, -1), msg.offset() + 1)
        self.written += len(rows) - sum(1 for d in dead if d[1] == "db_data_error")
        self.dead += len(dead)
        log.info(
            "processed batch=%d written_total=%d dead_lettered_total=%d",
            len(messages),
            self.written,
            self.dead,
        )

    # ---- commit ----
    def _commit_pending(self, only: set[tuple[str, int]] | None = None) -> None:
        items = {k: v for k, v in self._pending.items() if only is None or k in only}
        if not items:
            return
        offsets = [TopicPartition(t, p, o) for (t, p), o in items.items()]
        try:
            result = self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:
            # Ví dụ đang rebalance: giữ nguyên offset treo, lần sau commit lại.
            # Nếu phải xử lý lại thì upsert vẫn cho kết quả đúng.
            log.warning("commit failed: %s", exc)
            return
        for tp in result or []:
            key = (tp.topic, tp.partition)
            if tp.error:
                log.warning("commit failed for %s: %s", key, tp.error)
            elif self._pending.get(key) == tp.offset:
                del self._pending[key]

    # ---- vòng lặp chính ----
    def run(self) -> int:
        rc = EXIT_OK
        try:
            self._retry(self.sink.connect, TRANSIENT_DB_ERRORS, "postgres connect")
            self.consumer.subscribe([self.cfg.topic], on_assign=self._on_assign,
                                    on_revoke=self._on_revoke, on_lost=self._on_lost)
            log.info("started topic=%s group=%s batch=%d", self.cfg.topic,
                     self.cfg.consumer_group, self.cfg.batch_size)
            while not self._stop.is_set():
                if self._fatal is not None:
                    raise FatalError(f"kafka fatal: {self._fatal}")
                messages = self.consumer.consume(num_messages=self.cfg.batch_size, timeout=1.0)
                if not messages:
                    continue
                self.process_batch(messages)
                self._commit_pending()
        except Exception:
            log.exception("lỗi không thể phục hồi, thoát mà KHÔNG commit batch đang xử lý dở")
            rc = EXIT_ERROR
        finally:
            self._shutdown()
        return rc

    def _shutdown(self) -> None:
        log.info("shutting down: written=%d dead_lettered=%d", self.written, self.dead)
        try:
            self._commit_pending()  # chỉ gồm batch đã xử lý xong
        except Exception:
            log.exception("final commit failed")
        try:
            self.consumer.close()  # rời group ngay, không phải chờ session.timeout.ms
        except Exception:
            log.exception("consumer close failed")
        self.sink.close()


def main() -> int:
    config.setup_logging()
    try:
        cfg = config.load_config(need_db=True)
        schema_str = config.load_schema(cfg)
    except ConfigError as exc:
        log.error("%s", exc)
        return EXIT_CONFIG
    app = ConsumerApp(cfg, schema_str)
    signal.signal(signal.SIGINT, app.request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, app.request_stop)
    return app.run()


if __name__ == "__main__":
    sys.exit(main())
