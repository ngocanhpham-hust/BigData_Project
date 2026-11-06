# Cập nhật dự án và phần việc của Linh

Ngày cập nhật: 06/11/2026. Người phụ trách: **Linh (`tlinh`)**.
Nhánh làm việc: `feat/gd2-3-spark-iceberg`, base `origin/main` tại commit `b1d3014`.

## Mốc nộp theo phân công của TLINH

README phân công ghi tuần tương đối, không có ngày bắt đầu từng tuần. Bản nộp
này được giới hạn theo **GĐ 2–3, phần Spark + Iceberg**; ngày commit là ngày làm
việc thực tế, không dùng để suy ra nhóm đã hoàn thành các mốc sau.

| Mốc trong timeline | Phần việc của Linh trong nhánh này | Trạng thái nghiệm thu |
| --- | --- | --- |
| Tuần 1 | Image, ServiceAccount/RBAC, Spark driver/executor trên K8s | Đã kiểm thử trong cluster riêng |
| Tuần 2 | Schema Iceberg, batch từ raw-zone, streaming từ Kafka, checkpoint MinIO | Đã kiểm thử sample; Kafka dùng fixture riêng |
| Tuần 3 | Bàn giao bảng cho TD/Anh Đạt, nối Kafka/NiFi và DAG chung | Chờ tích hợp, không đánh dấu hoàn tất |
| Tuần 4 | Hỗ trợ benchmark và báo cáo | Chưa thực hiện trong bản nộp này |

Kafka fixture và Dockerfile MinIO nằm trong `tests/spark/` chỉ phục vụ kiểm thử
Spark độc lập. Chúng không thay thế phần triển khai Kafka/NiFi của NAP hoặc hạ
tầng chung. Nhánh này không thay đổi Trino/dbt, Airflow/OpenMetadata, README
hay Makefile chung và không bao gồm kết quả benchmark 30 triệu dòng.

## 1. Tiến độ chung theo repository mới nhất

Đã đối chiếu README phân công bạn cung cấp với code trên `main` sau PR #3.
Không dùng các checkbox cũ để suy ra rằng dịch vụ đã chạy.

| Phần việc | Bằng chứng trong main | Đánh giá |
| --- | --- | --- |
| Hạ tầng nền | k3d, manifest MinIO/PostgreSQL, bucket initialization | Có code; triển khai lại cần giải quyết image MinIO cũ |
| Dữ liệu | Crawler, dữ liệu mẫu, generator, Release `data-v1` | Đã có dữ liệu và script; 30 triệu nến là synthetic |
| Kafka/NiFi của NAP | Chưa có manifest 30/50 trên main | Còn phụ thuộc, chưa xác nhận service của nhóm |
| Spark/Iceberg của TLINH | Batch local và streaming skeleton | Đã được hoàn thiện trong nhánh này, xem kết quả bên dưới |
| Replay của TD | `etl/replay_to_kafka.py` đã merge | Có code tạo message đúng schema, không còn là việc chưa viết |
| Trino/dbt của TD | Manifest 60/61 và staging/intermediate/marts đã merge PR #3 | Code đã có; việc chạy Trino/dbt chưa được xác minh trong lượt làm này |
| Airflow/OpenMetadata của PGB | Chưa có manifest 70/80/81 trên main | Chưa thấy triển khai trong main |
| Demo/benchmark của Anh Đạt | Chưa thấy kết quả benchmark trong main | Cần đo sau khi tích hợp stack |

README chung của repository còn ghi Trino “mới ở thiết kế”, trong khi manifest/model
đã merge. Báo cáo này cập nhật theo file thực tế và giữ nguyên README/Makefile chung
để tránh xung đột với các thành viên.

## 2. Nhiệm vụ được giao cho Linh

Theo bảng phân công, Linh sở hữu **Spark + Iceberg, GĐ 2–3**:

- [x] Image Spark 3.5.1, Java 17, Python và Iceberg 1.5.2.
- [x] Đóng gói cả JAR S3A, AWS SDK, PostgreSQL, Spark Kafka connector và các dependency cần thiết.
- [x] ServiceAccount, RBAC, cấu hình driver/executor trên Kubernetes.
- [x] Schema/partition cho bảng giá ngày, Kafka raw envelope và nến phút.
- [x] Batch raw-zone → validate/clean → MERGE Iceberg trong cleansed-zone.
- [x] Streaming Kafka → raw envelope → validate/watermark/deduplicate → nến phút.
- [x] Checkpoint riêng cho từng query trên MinIO.
- [x] SELECT/count/snapshots từ JDBC catalog trong một Spark job khác.
- [x] Test thực tế trên cluster k3d riêng, driver và executor là các pod khác nhau.
- [ ] Tích hợp vào cluster của cả nhóm và Kafka/NiFi do NAP cung cấp.
- [ ] Chạy dataset đầy đủ và benchmark 30 triệu dòng cùng Anh Đạt.

Tiêu chí “spark-submit chạy trên K8s và SELECT ra dữ liệu” đã được kiểm chứng với sample.
Việc này không đồng nghĩa toàn bộ stack của nhóm đã hoàn tất hoặc đã benchmark tải lớn.

## 3. Những thay đổi đã làm

**Batch:** thay `createOrReplace` bằng `MERGE` theo `(symbol, trade_date)`, nên input
theo ngày không làm mất lịch sử. Chuẩn hóa dữ liệu, kiểm tra OHLC và numeric,
chọn bản ghi trùng theo hash ổn định, báo lỗi nếu không còn dòng hợp lệ.

**Streaming:** giữ JSON gốc trong `quotes_raw`, thêm topic/partition/offset để truy vết.
Một query tiếp theo đọc append snapshots từ raw và tạo `stock_quotes_minute`.
Watermark 10 phút và deduplicate theo `(symbol, ts, source)` giới hạn trong state window.
Checkpoint nằm trên MinIO; khởi động pod mới với cùng checkpoint tiếp tục từ vị trí đã lưu.

**Kubernetes:** image có sẵn JAR nên khi chạy không tải Maven. Driver nằm trong Job,
Spark tạo executor pod qua ServiceAccount. Không cần Spark Operator hoặc Spark Master/Worker riêng.

**Bàn giao:** script chạy riêng, README kỹ thuật, SQL kiểm chứng, test fixtures và integration tests.
Không sửa file của TD/NAP/PGB hoặc shared Makefile.

## 4. Kết quả kiểm thử đã quan sát

### Kiểm thử Spark/Iceberg local trong container

Ba integration tests chạy thành công bằng Spark 3.5.1, Java 17, Iceberg 1.5.2:

1. Dữ liệu lỗi, NaN/Infinity, volume sai, OHLC sai và duplicate mâu thuẫn.
2. Batch chạy lại không nhân đôi; update một khóa cũ và thêm một khóa mới;
   input không hợp lệ không làm mất dữ liệu đã có.
3. Streaming giữ 4 raw envelopes nhưng chỉ 2 nến hợp lệ; restart cùng checkpoint giữ nguyên;
   thêm dữ liệu trễ/duplicate/mới cho kết quả 7 raw và 3 nến, múi giờ Việt Nam đúng.

### Kiểm thử trên Kubernetes

Cluster test: `tlinh-lakehouse`, k3s `v1.30.14+k3s1`, ba node trên Docker Desktop.
Một executor chạy trên agent, driver chạy trong Kubernetes Job.

| Bước | Bằng chứng |
| --- | --- |
| Tạo raw sample trên MinIO | `spark-sample-qh42g`: 100 dòng Parquet |
| Batch dùng JDBC catalog + S3A | `spark-batch-gmgc9`: raw=100, accepted=100, table_rows=100 |
| Kafka streaming vào Iceberg | `spark-stream-9h44w`: Job Complete |
| SELECT ở job độc lập | `spark-verify-m59q2`: daily=100, raw=100, minute=100, PASS |
| Chạy lại batch bằng pod mới | `spark-batch-s7z9c`: table_rows vẫn bằng 100 |
| Restart streaming cùng checkpoint MinIO | `spark-stream-nz9hj`: Complete; không replay 100 message đã commit |
| SELECT sau cả hai lần chạy lại | `spark-verify-cqj5k`: daily=100, raw=100, minute=100, PASS |
| Catalog PostgreSQL | Có ba bảng thuộc catalog `iceberg`, namespace `stock` |

Lỗi RBAC cleanup PVC/collection phát hiện trong lần chạy đầu đã được sửa trong manifest.
Streaming và job verify tiếp theo không còn lỗi Forbidden.

Kết thúc kiểm thử, cluster được dừng để trả RAM cho máy; không xóa cluster/PVC.
Trên máy đã có cluster test, mở lại bằng kubeconfig riêng của môi trường đó:

```bash
export KUBECONFIG=/path/to/tlinh-test-kubeconfig.yaml
k3d cluster start tlinh-lakehouse
kubectl -n lakehouse get jobs,pods
```

Các số liệu ở đây là smoke test correctness; không dùng thời gian chạy sample làm benchmark.
Chưa tuyên bố recovery khi crash đúng giữa commit hoặc khả năng chịu lỗi host.

### Kiểm tra trước khi push nhánh

Ngày 06/11/2026 đã chạy lại Docker build và ba integration tests, tất cả thành công.
Build context đã được giới hạn bằng `spark/Dockerfile.dockerignore`, không thêm
dockerignore dùng chung ở root repository.

Sau khi mở lại cluster test, Kafka fixture không có PVC bị mất topic. Vì vậy lần
smoke test mới dùng database `metastore_tlinh_publish_20261106`, warehouse
`s3a://cleansed-zone/tlinh-publish-20261106/warehouse` và checkpoint riêng.
Kết quả cũ không bị ghi đè. Đây là test trong môi trường tách biệt, không phải
nghiệm thu tích hợp Kafka/NiFi hoặc benchmark của nhóm.

| Job kiểm tra trước push | Kết quả |
| --- | --- |
| `spark-batch-4bw7m` | raw=100, accepted=100, table_rows=100 |
| `spark-stream-sps6f` | Complete, Kafka sang Iceberg với checkpoint MinIO riêng |
| `spark-verify-ggwrb` | daily=100, raw=100, minute=100, PASS |

## 5. Tài nguyên và phụ thuộc còn cần

Phần code và smoke test không yêu cầu thêm tài khoản hay API key. Docker Desktop đã có,
môi trường test dùng khoảng 8 GB RAM của Docker; k3d tải riêng vào thư mục runtime của workspace.

Để triển khai vào môi trường **của nhóm**, cần:

1. Đường dẫn kubeconfig/context của đúng cluster nếu nằm ở máy khác. Không cần gửi nội dung secret vào chat.
2. Xác nhận bootstrap Kafka/topic của NAP và schema JSON thống nhất với replay của TD.
3. Image MinIO có thể pull được hoặc image đã cache/import vào cluster.
4. Raw Parquet đã upload vào `raw-zone` để chạy full dataset; có thể lấy từ Release `data-v1`.

Khi benchmark toàn bộ stack, nên dành 11 GB RAM cho Docker như kế hoạch của nhóm
hoặc dùng máy/cloud riêng. Chưa cần cloud để chạy sample và xác minh phần của Linh.

### Vướng mắc MinIO đã xử lý trong môi trường test

Image `quay.io/minio/minio:RELEASE.2024-06-13T22-53-53Z` và `quay.io/minio/mc:latest`
không pull được trong phiên kiểm tra (401), binary archive trả 410. Đã build server
từ tag chính thức cùng phiên bản, commit nguồn `20960b6a2ddb9594ee418035b3c7c7fe92ae6a12`.
Bucket được khởi tạo bằng AWS SDK khi chạy sample với opt-in `INIT_SAMPLE_BUCKETS=1`.
Đây là thay thế trong cluster test; manifest MinIO chung không bị sửa.

## 6. Hướng dẫn chạy và thông tin gửi nhóm

Xem [hướng dẫn Spark](../spark/README.md) cho các lệnh build/import/submit và SQL.

Tin nhắn gợi ý để Linh tự gửi nhóm:

> Linh đã hoàn thiện nhánh Spark + Iceberg: image 3.5.1/1.5.2, RBAC, batch MERGE và
> streaming Kafka với checkpoint trên MinIO. Đã test trên k3d ba node riêng;
> SELECT được daily/raw/minute, mỗi bảng 100 dòng sample. Tên daily table giữ nguyên
> `iceberg.stock.stock_prices_daily` để TD dùng Trino/dbt. Cần NAP xác nhận Kafka
> endpoint/topic, rồi tích hợp vào cluster chung. Hiện image MinIO cũ pull lỗi,
> nên cần thống nhất image/cache của nhóm trước khi dựng lại.
