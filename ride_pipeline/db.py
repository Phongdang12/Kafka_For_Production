"""Ghi Postgres bằng upsert theo ride_id (xử lý lại cùng message không tạo bản ghi trùng).

Phân loại lỗi DB:
  TRANSIENT_DB_ERRORS : mất kết nối, DB đang khởi động lại... -> retry + tự kết nối lại
  DATA_DB_ERRORS      : dữ liệu không hợp lệ với DB (số quá lớn, ký tự NUL, vi phạm ràng buộc)
                        -> thử lại vô ích, consumer cô lập dòng hỏng và đẩy vào DLQ
  lỗi khác            : (ví dụ thiếu bảng) -> ném ra, consumer thoát
"""
from __future__ import annotations

import psycopg

from .models import Ride

TRANSIENT_DB_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)
DATA_DB_ERRORS = (psycopg.DataError, psycopg.IntegrityError)

UPSERT_SQL = """
INSERT INTO rides (ride_id, rider_id, route, fare, status, event_time,
                   kafka_partition, kafka_offset)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ride_id) DO UPDATE SET
    rider_id = EXCLUDED.rider_id,
    route = EXCLUDED.route,
    fare = EXCLUDED.fare,
    status = EXCLUDED.status,
    event_time = EXCLUDED.event_time,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset = EXCLUDED.kafka_offset,
    ingested_at = now()
"""


class RideSink:
    def __init__(self, url: str, connect_timeout: int = 5, statement_timeout_ms: int = 30_000):
        self._url = url
        self._kwargs = dict(
            connect_timeout=connect_timeout,
            options=f"-c statement_timeout={statement_timeout_ms}",
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
        )
        self._conn: psycopg.Connection | None = None

    def connect(self) -> None:
        """Mở kết nối nếu chưa có (ném OperationalError nếu DB chưa sẵn sàng)."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._url, **self._kwargs)

    def _drop(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None

    def upsert_batch(self, rows: list[tuple[Ride, int, int]]) -> None:
        """rows = [(ride, partition, offset)]. Cả batch nằm trong 1 transaction."""
        params = [
            (r.ride_id, r.rider_id, r.route, r.fare, r.status, r.event_time, p, o)
            for r, p, o in rows
        ]
        try:
            self.connect()
            with self._conn.transaction():
                with self._conn.cursor() as cur:
                    cur.executemany(UPSERT_SQL, params)
        except TRANSIENT_DB_ERRORS:
            self._drop()  # lần retry sau sẽ kết nối lại
            raise

    def close(self) -> None:
        self._drop()
