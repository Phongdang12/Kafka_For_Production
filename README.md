# Ride-events-pipeline

Nguồn (JR hoặc bộ sinh giả) → **Kafka (Avro + Schema Registry)** → Consumer → **PostgreSQL**, kèm Dead Letter Queue.
Viết theo các nguyên tắc production của producer và consumer (confluent-kafka-python / librdkafka).



## Luồng dữ liệu

### Producer
![Luồng dữ liệu của producer](docs/producer-flow.svg)

- `produce()` **không** có nghĩa là message đã tới broker. Chỉ khi `on_delivery` báo thành công mới chắc chắn.
- Serialize (Avro) làm trong Python; nén và gom batch là việc của librdkafka.

### Consumer
![Luồng dữ liệu của consumer](docs/consumer-flow.svg)

- Luồng dữ liệu đi từ **broker → librdkafka → Python**; commit đi ngược lại về `__consumer_offsets`.
- Chỉ commit offset của batch **đã xử lý xong** (DB đã ghi, message hỏng đã vào DLQ và flush).

## Phân loại lỗi 

| Loại lỗi | Ví dụ | Xử lý |
|---|---|---|
| Dữ liệu hỏng (thử lại vô ích) | Avro sai, thiếu field, status lạ, giá trị DB không hợp lệ | Gửi vào `rides.dlq`, đi tiếp |
| Hạ tầng tạm thời | DB mất kết nối, Schema Registry chập chờn | Retry backoff + jitter, tự kết nối lại, không vào DLQ |
| Hạ tầng kéo dài | DB chết quá `DB_RETRY_DEADLINE_S` | Thoát mã 1, **không commit**, Docker khởi động lại |
| DLQ không ghi được | Kafka lỗi | Không commit, thoát (đi tiếp sẽ mất dữ liệu) |
| Lỗi fatal của Kafka | Idempotent producer hỏng | Thoát để được khởi động lại |
| Cấu hình sai | Thiếu biến, giá trị vô lý | Thoát ngay mã 78 với thông báo rõ |

Hai ràng buộc quan trọng:
- Tổng thời gian retry (`DB_RETRY_DEADLINE_S`, mặc định 120s) phải ≤ 60% `CONSUMER_MAX_POLL_INTERVAL_MS` (300s), nếu không consumer bị đá khỏi group. Cấu hình vi phạm sẽ bị từ chối lúc khởi động.
- DLQ được `flush()` **trước** khi commit. Nếu crash giữa hai bước, batch được xử lý lại: DB không trùng (upsert), DLQ có thể có bản sao (header `dlq.source.*` cho phép khử trùng).

## Chạy thử (Docker)

Yêu cầu: Docker Compose v2.

```bash
docker compose up -d --build        # kafka, schema-registry, postgres, init (tạo topic + đăng ký schema), consumer

docker compose run --rm producer --count 10000     # gửi 10.000 message synthetic rồi thoát

docker compose exec postgres psql -U rides -c "SELECT count(*), count(DISTINCT ride_id) FROM rides;"
docker compose logs -f consumer
```

Chạy producer liên tục: `docker compose --profile producer up -d producer`.
Thêm consumer: `docker compose up -d --scale consumer=3` (không nên vượt số partition, dư sẽ ngồi không).
Dừng: `docker compose stop` → SIGTERM → producer `flush()`, consumer commit lần cuối rồi `close()`.



## Nguồn dữ liệu JR

```bash
docker compose run --rm -T jr | docker compose run --rm -T producer --source stdin
```

Chỉnh tải bằng `JR_NUM` (số message mỗi lượt), `JR_FREQUENCY` (chu kỳ), `JR_DURATION` (tổng thời gian).
Tốc độ = JR_NUM / JR_FREQUENCY; tổng số message = tốc độ × JR_DURATION.
Đừng dùng `-o kafka` của JR: nó ghi JSON thẳng vào topic, consumer sẽ đẩy toàn bộ vào DLQ.

## Cấu hình (biến môi trường)

| Biến | Mặc định | Ghi chú |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | — (bắt buộc) | |
| `SCHEMA_REGISTRY_URL` | — (bắt buộc) | |
| `DATABASE_URL` | — (bắt buộc cho consumer) | |
| `KAFKA_TOPIC` / `KAFKA_DLQ_TOPIC` | `rides` / `rides.dlq` | |
| `KAFKA_CONSUMER_GROUP` / `REPLAY_GROUP` | `ride-writer` / `ride-replay` | |
| `CONSUMER_BATCH_SIZE` | `500` | |
| `CONSUMER_MAX_POLL_INTERVAL_MS` | `300000` | |
| `DB_RETRY_DEADLINE_S` | `120` | tổng thời gian retry tối đa |
| `RETRY_BASE_S` / `RETRY_CAP_S` | `0.5` / `10` | backoff mũ + jitter |
| `PRODUCE_RATE` | `200` | message/giây, chỉ cho synthetic, 0 = tối đa |
| `TOPIC_PARTITIONS` | `3` | dùng khi tạo topic |
| `TOPIC_REPLICATION_FACTOR` / `TOPIC_MIN_ISR` | `1` / `1` | production: `3` / `2` |
| `TOPIC_RETENTION_MS` / `DLQ_RETENTION_MS` | 7 ngày / 30 ngày | |
| `KAFKA_EXTRA_CONFIG` | — | `key=value,key=value` (SASL/SSL...) |
| `SCHEMA_REGISTRY_AUTH` | — | `user:password` |
| `SCHEMA_PATH` | `schemas/ride.avsc` | |
| `LOG_LEVEL` | `INFO` | |

## Avro và Schema Registry

- Schema nằm ở [`schemas/ride.avsc`](schemas/ride.avsc), subject `rides-value`, tương thích **BACKWARD**.
- Producer **không** tự đăng ký (`auto.register.schemas=False`). Schema phải được đăng ký bằng `python -m ride_pipeline.admin schema` (compose đã chạy sẵn qua service `init`); producer kiểm tra lúc khởi động và thoát nếu chưa có.
- `fare` dùng `decimal(10,2)` (đúng cho tiền, khớp `NUMERIC(10,2)`), `event_time` dùng `timestamp-millis`.
- Quy tắc tiến hoá: thêm field mới phải có `default`. Với BACKWARD, **nâng consumer trước, producer sau** (consumer schema mới đọc được dữ liệu cũ). Schema không tương thích sẽ bị Registry từ chối khi đăng ký.


## DLQ và replay

Mỗi message trong `rides.dlq` giữ **nguyên bytes gốc và key**, kèm header:

| Header | Nội dung |
|---|---|
| `dlq.error.kind` | `avro_decode`, `validation`, `empty_value`, `schema_lookup`, `db_data_error` |
| `dlq.error.message` | thông báo lỗi (tối đa 1000 ký tự) |
| `dlq.source.topic/partition/offset` | vị trí gốc |
| `dlq.failed_at` | thời điểm ISO 8601 UTC |

```bash
python -m ride_pipeline.replay --limit 100 --dry-run   # chỉ xem
python -m ride_pipeline.replay --limit 100             # gửi lại vào topic chính, giữ key
# trong Docker: docker compose run --rm --entrypoint python consumer -m ride_pipeline.replay --limit 100 --dry-run
```

Chỉ replay sau khi đã sửa nguyên nhân; message vẫn hỏng sẽ quay lại DLQ. Offset replay lưu theo group `ride-replay` nên lần sau chạy tiếp từ chỗ dừng.

## Kiểm tra từng tiêu chí: 

### 2.1 Chịu lỗi DB: phải có message đi qua trong lúc Postgres tắt

```bash
# Terminal 1: theo dõi consumer
docker compose logs -f consumer

# Terminal 2: tắt Postgres
docker compose stop postgres

# Terminal 3: bắn message khi Postgres đang tắt
docker compose run --rm producer --count 1000

# Chờ ít nhất 30 giây, sau đó bật Postgres lại
sleep 30
docker compose start postgres

# Kiểm tra log retry và kết quả trong DB
docker compose logs --since 5m consumer | grep -i "lỗi tạm thời"
docker compose exec postgres psql -U rides -c "SELECT count(*), count(DISTINCT ride_id) FROM rides;"

```

**Kết quả đã kiểm chứng (2026-10-04): đạt.** Trong khi PostgreSQL tắt, producer
đã gửi `773` message vào Kafka. Consumer ghi nhận lỗi kết nối tạm thời và retry
với backoff:

```text
postgres lỗi tạm thời (lần 1): AdminShutdown
postgres lỗi tạm thời (lần 2): Temporary failure in name resolution
postgres lỗi tạm thời (lần 3): Temporary failure in name resolution
```

Sau khi PostgreSQL hoạt động lại, consumer tiếp tục xử lý và producer xác nhận:

```text
sent=773 delivered=773 failed=0 invalid=0 unflushed=0
```

Kiểm tra PostgreSQL cho kết quả:

```text
count  | count
-------+-------
773    | 773
```

Kết luận: consumer không dừng, message không bị đưa vào DLQ do lỗi hạ tầng,
toàn bộ message được ghi thành công và không phát sinh duplicate `ride_id`.

Kịch bản trên xác nhận khả năng retry và phục hồi khi DB tạm thời mất kết nối.
Kịch bản DB chết quá `DB_RETRY_DEADLINE_S` để xác nhận container tự khởi động
lại là một bài kiểm thử riêng và chưa nằm trong kết quả này.

### 2.2 DLQ: gửi một message không phải Avro

Trình tự dưới đây là bản ghi đúng của lần kiểm chứng thực tế ngày
2026-10-04. Các offset cụ thể phụ thuộc trạng thái Kafka tại thời điểm chạy.

```powershell
# 1. Offset DLQ trước khi kiểm thử
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic rides.dlq

# 2. Gửi message không phải Avro vào topic rides
echo 'bad-key-1:not-avro' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic rides --property parse.key=true --property key.separator=:

# 3. Gửi thêm dữ liệu hợp lệ từ JR
$env:JR_DURATION="5s"; docker compose run --rm -T jr | docker compose run --rm -T producer --source stdin; Remove-Item Env:JR_DURATION

# 4. Kiểm tra log consumer
docker compose logs consumer --tail 15

# 5. Kiểm tra message lỗi ở topic rides
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rides --partition 1 --offset 18604 --max-messages 1

# 6. Kiểm tra consumer group và lag
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ride-writer

# 7. Đọc DLQ và headers
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rides.dlq --from-beginning --timeout-ms 10000 --property print.key=true --property print.headers=true

# 8. Replay dry-run
docker compose run --rm --entrypoint python producer -m ride_pipeline.replay --limit 5 --dry-run

# 9. Kiểm tra offset sau dry-run
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic rides.dlq

# 10. Replay thật
docker compose run --rm --entrypoint python producer -m ride_pipeline.replay --limit 5

# 11. Kiểm tra consumer xử lý lại message replay
docker compose logs consumer --tail 10

# 12. Kiểm tra offset DLQ cuối cùng
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic rides.dlq
```

**Kết quả đã kiểm chứng (2026-10-04): đạt.** Trước lần kiểm thử, DLQ có
`7` message. Đã gửi thêm một message không phải Avro vào topic `rides`, với
key `bad-key-1` và value `not-avro`. Consumer không bị dừng và ghi nhận:

```text
dead-letter rides[0]@18653 kind=avro_decode: Invalid magic byte
processed batch=1 written_total=873 dead_lettered_total=8
```

Message được ghi vào `rides.dlq` với đầy đủ thông tin:

```text
dlq.error.kind=avro_decode
dlq.error.message=Invalid magic byte
dlq.source.topic=rides
dlq.source.partition=0
dlq.source.offset=18653
dlq.failed_at=2026-10-04T09:21:38.224916+00:00
key=bad-key-1    value=not-avro
```

Kết quả xác nhận:

- Message không hợp lệ không được ghi vào PostgreSQL.
- Bytes gốc và Kafka key được giữ nguyên trong DLQ.
- Header lỗi và vị trí message gốc được lưu đầy đủ.
- Consumer tiếp tục xử lý message hợp lệ và commit offset.
- Consumer group `ride-writer` có `LAG=0` tại thời điểm kiểm tra.

### Replay DLQ

Đã kiểm tra `--dry-run` với cùng replay group: lệnh đọc được 3 message
(`dlq@5`, `dlq@6`, `dlq@7`) nhưng không gửi lại và không commit offset.
Sau đó replay thật đọc và gửi 5 message:

```text
REPLAY dlq@5 ...
REPLAY dlq@6 ...
REPLAY dlq@7 ...
REPLAY dlq@8 ...
REPLAY dlq@9 ...
replay xong: 5 message
```

Vì nguyên nhân chưa được sửa (`not-avro` vẫn không phải Avro), các message
replay quay lại DLQ là đúng thiết kế. Consumer ghi nhận tổng cộng
`dead_lettered_total=13` và offset cuối của `rides.dlq` tăng lên `13`.

Kết quả này xác nhận replay đọc được message từ DLQ, giữ nguyên bytes/key và
gửi lại vào topic chính. Nó chưa chứng minh khả năng phục hồi nghiệp vụ vào
PostgreSQL; muốn chứng minh phần đó cần replay payload đã được sửa thành Avro
hợp lệ. Khi replay dữ liệu vẫn hỏng, DLQ có thể tạo vòng lặp thêm message mới.

Lỗi `TimeoutException` của Kafka console consumer sau khi đọc hết message là
bình thường do lệnh được chạy với timeout; tại thời điểm đó đã đọc đủ 8 message.

### 2.3 Schema compatibility: từ chối field bắt buộc không có `default`

Schema được copy vào Docker image bởi `Dockerfile`, vì vậy phải build lại
image `init` sau khi sửa file schema.

Trên PowerShell, chạy theo thứ tự sau:

```powershell
# 1. Backup schema hợp lệ
Copy-Item .\schemas\ride.avsc .\schemas\ride.avsc.bak

# 2. Sửa schemas/ride.avsc, thêm field mới không có default:
# {"name": "test_without_default", "type": "string"}

# 3. Kiểm tra file vẫn là JSON hợp lệ
Get-Content .\schemas\ride.avsc -Raw | ConvertFrom-Json | Out-Null

# 4. Build image để init dùng schema mới
docker compose build init

# 5. Đăng ký schema mới, dự kiến thất bại với HTTP 409
docker compose run --rm init schema
$LASTEXITCODE

# 6. Khôi phục schema hợp lệ
Move-Item -Force .\schemas\ride.avsc.bak .\schemas\ride.avsc

# 7. Build lại image sau khi khôi phục
docker compose build init

# 8. Xác nhận schema hợp lệ hoạt động lại
docker compose run --rm init schema
$LASTEXITCODE
```

**Kết quả đã kiểm chứng (2026-10-04): đạt.** Khi schema có field
`test_without_default`, Schema Registry trả về:

```text
HTTP/1.1 409 Conflict
READER_FIELD_MISSING_DEFAULT_VALUE
The field 'test_without_default' ... has no default value
```

Lệnh `init schema` thất bại đúng thiết kế vì schema mới không tương thích
`BACKWARD`. Sau khi khôi phục schema gốc, lệnh chạy thành công:

```text
HTTP/1.1 200 OK
subject=rides-value schema_id=1 compatibility=BACKWARD
exit code: 0
```

Một lần chạy `Move-Item` thứ hai có thể báo không tìm thấy file `.bak`; điều đó
chỉ có nghĩa file đã được khôi phục ở lần chạy đầu tiên, không phải lỗi của
Schema Registry. File backup tạm thời không được commit vào repository.

### 2.4 Nguồn dữ liệu JR

Consumer phải đang chạy trước khi thực hiện kiểm thử. Trên PowerShell, chạy:

```powershell
# Ghi nhận số bản ghi trước khi test
$before = [int](docker compose exec postgres psql -U rides -d rides -tA -c "SELECT count(*) FROM rides;")

# JR sinh JSON, producer đọc qua stdin rồi serialize thành Avro
$env:JR_DURATION="5s"; docker compose run --rm -T jr | docker compose run --rm -T producer --source stdin; Remove-Item Env:JR_DURATION

# Kiểm tra số bản ghi sau khi consumer xử lý
$after = [int](docker compose exec postgres psql -U rides -d rides -tA -c "SELECT count(*) FROM rides;")
"before=$before after=$after delta=$($after - $before)"

# Kiểm tra không có consumer lag
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ride-writer
```

**Kết quả đã kiểm chứng (2026-10-04): đạt.** JR sinh `100` JSON object trong
5 giây. Producer xác nhận:

```text
sent=100 delivered=100 failed=0 invalid=0 unflushed=0
```

Consumer xử lý thêm `100` record:

```text
before=973 after=1073 delta=100
```

Consumer group `ride-writer` có `LAG=0` trên cả ba partition. Kết quả xác nhận
đầy đủ luồng:

```text
JR JSON → producer stdin → Ride validation → Avro → Kafka → consumer → PostgreSQL
```

Test này chứng minh JR được dùng làm nguồn JSON qua stdin; producer vẫn là nơi
serialize Avro trước khi ghi vào Kafka. Không dùng JR với `-o kafka`, vì cách đó
bỏ qua producer và gửi JSON thẳng vào topic, khiến consumer đưa message vào DLQ.

## Cấu trúc

```
ride_pipeline/
  config.py     cấu hình + kiểm tra lúc khởi động      retry.py   backoff + jitter, chặn tổng thời gian
  models.py     Ride, validate, bộ sinh dữ liệu giả     errors.py  phân loại lỗi
  serde.py      Avro + Schema Registry                  db.py      upsert + phân loại lỗi DB
  producer.py   synthetic | stdin (JR)                  dlq.py     Dead Letter Queue
  consumer.py   vòng lặp chính                          replay.py  DLQ → topic chính
  admin.py      tạo topic, đăng ký schema
schemas/ride.avsc   sql/init.sql   jr/ride.json   docs/*.svg   Dockerfile   docker-compose.yml
```

## Triển khai production (lưu ý)

- Cluster ≥ 3 broker: `TOPIC_REPLICATION_FACTOR=3`, `TOPIC_MIN_ISR=2` (đi cùng `acks=all`). Compose trong repo chỉ là hạ tầng dev.
- Bảo mật qua `KAFKA_EXTRA_CONFIG` và `SCHEMA_REGISTRY_AUTH`; không commit mật khẩu, đừng dùng `postgres:rides` ngoài dev.
- Chạy consumer với `restart: unless-stopped` (hoặc Kubernetes tương đương) và `stop_grace_period` ≥ 40s.
- Số consumer trong group không nên vượt số partition.

## Giới hạn còn lại

- Chưa có test, CI (v0.3), metric và dashboard (v0.4), kịch bản chaos (v0.5).
- Producer chưa có DLQ riêng: delivery thất bại chỉ được log, đếm và làm tiến trình thoát với mã khác 0.
- Upsert theo ride_id ghi đè bản mới nhất theo thứ tự xử lý; chưa so sánh event_time để chống bản cũ ghi đè bản mới khi dữ liệu đến lộn xộn giữa nhiều partition.