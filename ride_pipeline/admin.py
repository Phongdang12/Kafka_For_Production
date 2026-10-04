"""Tạo topic (có cấu hình rõ ràng) và đăng ký schema. Chạy một lần khi triển khai.

  python -m ride_pipeline.admin topics   # rides + rides.dlq
  python -m ride_pipeline.admin schema   # đăng ký schemas/ride.avsc, đặt tương thích BACKWARD
  python -m ride_pipeline.admin setup    # cả hai
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaRegistryError

from . import config
from .config import EXIT_CONFIG, EXIT_ERROR, EXIT_OK, ConfigError
from .errors import FatalError, TransientError
from .retry import retry_with_backoff
from .serde import call_registry, subject_for

log = logging.getLogger("admin")


def _retry(cfg, fn, retriable, what, deadline_s=90):
    return retry_with_backoff(fn, retriable=retriable, deadline_s=deadline_s, base_s=cfg.retry_base_s,
                              cap_s=cfg.retry_cap_s, what=what, log=log)


def create_topics(cfg: config.Config) -> None:
    admin = AdminClient(config.kafka_conf(cfg))
    _retry(cfg, lambda: admin.list_topics(timeout=10), (KafkaException,), "kafka broker")
    common = {"min.insync.replicas": str(cfg.topic_min_isr), "cleanup.policy": "delete"}
    specs = [
        NewTopic(cfg.topic, num_partitions=cfg.topic_partitions,
                 replication_factor=cfg.topic_replication_factor,
                 config={**common, "retention.ms": str(cfg.topic_retention_ms)}),
        # DLQ ít message, retention dài hơn topic chính để còn thời gian điều tra và replay
        NewTopic(cfg.dlq_topic, num_partitions=1,
                 replication_factor=cfg.topic_replication_factor,
                 config={**common, "retention.ms": str(cfg.dlq_retention_ms)}),
    ]
    for topic, fut in admin.create_topics(specs, request_timeout=30).items():
        try:
            fut.result()
            log.info("đã tạo topic %s", topic)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("topic %s đã tồn tại (giữ nguyên cấu hình hiện có)", topic)
            else:
                raise


def register_schema(cfg: config.Config) -> None:
    schema_str = config.load_schema(cfg)
    json.loads(schema_str)  # fail sớm nếu file không phải JSON
    client = SchemaRegistryClient(config.registry_conf(cfg))
    subject = subject_for(cfg.topic)
    _retry(cfg, lambda: call_registry(client.get_subjects), (TransientError,), "schema registry")
    call_registry(lambda: client.set_compatibility(subject_name=subject, level="BACKWARD"))
    try:
        schema_id = call_registry(lambda: client.register_schema(subject, Schema(schema_str, "AVRO")))
    except SchemaRegistryError as exc:
        if exc.http_status_code == 409:
            raise FatalError(f"schema KHÔNG tương thích BACKWARD với phiên bản đã đăng ký: "
                             f"{exc.error_message}") from exc
        raise
    log.info("subject=%s schema_id=%s compatibility=BACKWARD", subject, schema_id)


def main(argv=None) -> int:
    config.setup_logging()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["topics", "schema", "setup"])
    args = ap.parse_args(argv)
    try:
        cfg = config.load_config()
    except ConfigError as exc:
        log.error("%s", exc)
        return EXIT_CONFIG
    try:
        if args.command in ("topics", "setup"):
            create_topics(cfg)
        if args.command in ("schema", "setup"):
            register_schema(cfg)
    except FatalError as exc:
        log.error("%s", exc)
        return EXIT_ERROR
    except Exception:
        log.exception("admin thất bại")
        return EXIT_ERROR
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
