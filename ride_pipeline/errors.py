"""Các loại lỗi của pipeline (xem bảng "phân loại lỗi" trong README)."""


class BadMessage(Exception):
    """Dữ liệu hỏng vĩnh viễn: thử lại bao nhiêu lần cũng thất bại -> đưa vào DLQ."""

    def __init__(self, kind: str, detail: str):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


class TransientError(Exception):
    """Hạ tầng lỗi tạm thời (Schema Registry, mạng...) -> retry có backoff."""


class FatalError(Exception):
    """Không thể tiếp tục an toàn -> không commit, thoát với mã lỗi."""


class RetryExhausted(FatalError):
    """Retry quá thời hạn cho phép."""


class DLQError(FatalError):
    """Không ghi được vào DLQ (nếu cứ đi tiếp thì mất dữ liệu)."""
