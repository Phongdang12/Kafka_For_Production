"""Định nghĩa `Ride`, kiểm tra dữ liệu, chuyển đổi từ/sang JSON, Avro và bộ sinh dữ liệu giả."""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Iterator

STATUSES = frozenset({"requested", "started", "completed", "cancelled"})
ROUTES = ["Q1-Q7", "Q3-Q10", "BT-TD", "GV-PN", "TB-Q12"]

_CENT = Decimal("0.01")
MAX_FARE = Decimal("99999999.99")  # khớp NUMERIC(10,2) và decimal(10,2) trong Avro
MAX_LEN = 128


def _text(name: str, v: Any) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{name} phải là chuỗi không rỗng")
    if len(v) > MAX_LEN:
        raise ValueError(f"{name} dài quá {MAX_LEN} ký tự")
    return v


def _fare(v: Any) -> Decimal:
    if v is None or isinstance(v, bool):
        raise ValueError("fare không hợp lệ")
    try:
        d = v if isinstance(v, Decimal) else Decimal(str(v))
        if not d.is_finite():
            raise ValueError("fare phải là số hữu hạn")
        d = d.quantize(_CENT, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError(f"fare không hợp lệ: {v!r}") from exc
    if d < 0 or d > MAX_FARE:
        raise ValueError(f"fare ngoài khoảng cho phép [0, {MAX_FARE}]: {d}")
    return d


def _event_time(v: Any) -> datetime:
    if isinstance(v, str):
        v = datetime.fromisoformat(v.replace("Z", "+00:00"))  # ValueError nếu sai
    if not isinstance(v, datetime):
        raise ValueError("event_time không hợp lệ")
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.astimezone(timezone.utc)


@dataclass(frozen=True)
class Ride:
    ride_id: str
    rider_id: str
    route: str
    fare: Decimal
    status: str
    event_time: datetime  # UTC, có timezone

    @classmethod
    def create(cls, *, ride_id, rider_id, route, fare, status, event_time) -> "Ride":
        """Kiểm tra + chuẩn hoá. Ném ValueError nếu dữ liệu không hợp lệ."""
        status = _text("status", status)
        if status not in STATUSES:
            raise ValueError(f"status lạ: {status!r}")
        return cls(
            ride_id=_text("ride_id", ride_id),
            rider_id=_text("rider_id", rider_id),
            route=_text("route", route),
            fare=_fare(fare),
            status=status,
            event_time=_event_time(event_time),
        )

    @classmethod
    def from_json_dict(cls, d: Any) -> "Ride":
        """Từ một dòng JSON (ví dụ do JR sinh). Thiếu event_time thì lấy giờ hiện tại."""
        if not isinstance(d, dict):
            raise ValueError("dòng JSON phải là object")
        try:
            return cls.create(
                ride_id=d["ride_id"], rider_id=d["rider_id"], route=d["route"],
                fare=d["fare"], status=d["status"],
                event_time=d.get("event_time") or datetime.now(timezone.utc),
            )
        except KeyError as exc:
            raise ValueError(f"thiếu field {exc}") from exc

    @classmethod
    def from_avro_dict(cls, d: Any) -> "Ride":
        if not isinstance(d, dict):
            raise ValueError("giá trị Avro không phải record")
        return cls.create(
            ride_id=d.get("ride_id"), rider_id=d.get("rider_id"), route=d.get("route"),
            fare=d.get("fare"), status=d.get("status"), event_time=d.get("event_time"),
        )

    def to_avro_dict(self) -> dict:
        return {
            "ride_id": self.ride_id, "rider_id": self.rider_id, "route": self.route,
            "fare": self.fare, "status": self.status, "event_time": self.event_time,
        }


def generate_rides() -> Iterator[Ride]:
    """Sinh vô hạn các ride giả để demo."""
    while True:
        yield Ride.create(
            ride_id=str(uuid.uuid4()),
            rider_id=f"rider-{random.randint(1, 5000)}",
            route=random.choice(ROUTES),
            fare=Decimal(random.randint(1000, 15000)) / 100,
            status=random.choice(sorted(STATUSES)),
            event_time=datetime.now(timezone.utc),
        )
