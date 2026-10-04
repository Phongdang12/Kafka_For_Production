FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ride_pipeline ride_pipeline
COPY schemas schemas

# không chạy bằng root
RUN useradd --create-home --uid 10001 app
USER app

# docker-compose đặt entrypoint riêng cho từng dịch vụ
CMD ["python", "-m", "ride_pipeline.consumer"]
