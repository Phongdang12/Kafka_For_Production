"""Retry có backoff mũ + jitter, chặn tổng thời gian."""
from __future__ import annotations

import logging
import random
import time
from typing import Callable, TypeVar

from .errors import RetryExhausted

T = TypeVar("T")


def _sleep(delay: float, stop: Callable[[], bool] | None) -> None:
    end = time.monotonic() + delay
    while (remaining := end - time.monotonic()) > 0:
        if stop is not None and stop():
            return
        time.sleep(min(0.2, remaining))


def retry_with_backoff(
    fn: Callable[[], T],
    *,
    retriable: tuple[type[BaseException], ...],
    deadline_s: float,
    base_s: float,
    cap_s: float,
    what: str,
    stop: Callable[[], bool] | None = None,
    log: logging.Logger | None = None,
) -> T:
    """Gọi fn(); lỗi thuộc `retriable` thì chờ rồi thử lại.

    - Chờ = backoff mũ (base*2^n, trần cap) với "equal jitter" (một nửa cố định, một nửa ngẫu nhiên).
    - Tổng thời gian bị chặn bởi deadline_s -> ném RetryExhausted (FatalError).
    - Có yêu cầu dừng (stop()) thì ném lại lỗi gốc ngay để tắt máy nhanh.
    - Lỗi không thuộc `retriable` được ném ra ngay.
    """
    log = log or logging.getLogger("retry")
    start = time.monotonic()
    attempt = 0
    while True:
        try:
            return fn()
        except retriable as exc:
            elapsed = time.monotonic() - start
            ceiling = min(cap_s, base_s * (2 ** min(attempt, 30)))
            delay = ceiling / 2 + random.uniform(0, ceiling / 2)
            attempt += 1
            if stop is not None and stop():
                raise
            if elapsed + delay > deadline_s:
                raise RetryExhausted(
                    f"{what}: quá {deadline_s:.0f}s sau {attempt} lần thử; lỗi cuối: {exc!r}"
                ) from exc
            log.warning("%s lỗi tạm thời (lần %d, đã %.1fs), thử lại sau %.1fs: %r",
                        what, attempt, elapsed, delay, exc)
            _sleep(delay, stop)
