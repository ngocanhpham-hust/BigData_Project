# LỘ TRÌNH LÀM LẠI DỰ ÁN
## Xây dựng hệ thống Data LakeHouse — Phân tích dữ liệu chứng khoán

> Tài liệu này là **kế hoạch/hướng đi**, chưa code. Mọi bước được suy ra trực tiếp từ báo cáo gốc
> `Báo cáo BigData.docx.pdf` (14 trang, 17 hình) — bản trích xuất đầy đủ nằm ở
> `00-reference/00_bao_cao_goc_trich_xuat.txt`, danh mục ảnh gốc ở `00-reference/00_doc_anh_goc.md`.

---

## PHẦN A — GIẢI MÃ BÁO CÁO GỐC

### A.1. Hồ sơ đề tài
| Mục | Nội dung trong báo cáo |
|---|---|
| Trường | ĐH Bách Khoa Hà Nội — Trường CNTT & Truyền thông |
| Môn | Lưu trữ và Xử lý dữ liệu lớn |
| Đề tài | Xây dựng hệ thống Data LakeHouse — Phân tích dữ liệu chứng khoán |
| GVHD | TS. Nguyễn Hữu Đức |
| SVTH | Đỗ Huy Đạt (20220024), Đào Bảo Ngọc (20224884), Lê Minh Triết (20220045) |
| Mốc thời gian | Hà Nội, 1/2026 |

### A.2. Cấu trúc báo cáo (5 chương — phải bám đúng)
1. **Giới thiệu bài toán** — BI là gì; kiến trúc Lakehouse: lưu trữ dữ liệu phi cấu trúc của Data Lake + ACID/quản lý giao dịch của Data Warehouse → nhất quán dữ liệu + hiệu năng truy vấn cao cho tầng Serving. *(Hình 1: so sánh 3 mô hình Data Lake / Data Warehouse / Lakehouse)*
2. **Tổng quan hệ thống** — Vì hạn chế máy vật lý nên **giả lập 9 máy = 9 container Ubuntu 22.04 bằng Docker**. Luồng: nguồn ngoài → **NiFi** thu thập → **HDFS** (batch) hoặc **Kafka** (stream) → **Spark** xử lý, ghi bảng **Iceberg** (metadata ở **PostgreSQL**) → **dbt** mô hình hóa → **Trino** truy vấn liên nguồn → **Superset** dashboard → toàn bộ điều phối bởi **Airflow**. *(Hình 2: sơ đồ kiến trúc LakeHouse)*
3. **Triển khai cụ thể** — 6 tiểu mục: 3.1 Hadoop + Spark · 3.2 NiFi · 3.3 Kafka · 3.4 Trino · 3.5 Superset · 3.6 Airflow + dbt
4. **Kết quả thu được** — thử 2 luồng dữ liệu: (a) dữ liệu sẵn có **hàng chục triệu dòng** để đo hiệu năng, (b) **tự động hóa lấy dữ liệu qua API**. Kết luận: hệ thống chạy ổn với đủ chức năng, có lập lịch/tự động hóa, hiệu năng tốt dù chỉ chạy Docker, có web UI thân thiện.
5. **Kết luận** — Lakehouse là nền tảng phân tích hiện đại; hướng phát triển: bảo mật tập trung, AI/ML trực tiếp trên Data Lake (Spark MLlib).

### A.3. Topology & cấu hình suy ra từ ảnh (đây là "bản thiết kế" thật)
**9 container (Hình 3.1):**

| Container | Vai trò | Port map (host→container) |
|---|---|---|
| `namenode01` | NameNode active + Zookeeper + JournalNode | 9870→9870 (HDFS UI) |
| `namenode02` | NameNode standby + Zookeeper + JournalNode | 9871→9870 |
| `datanode01` | DataNode + NodeManager + Kafka broker | – |
| `datanode02` | DataNode + NodeManager + Kafka broker | – |
| `datanode03` | DataNode + NodeManager + Kafka broker | – |
| `spark` | Spark (master) + ResourceManager + Zookeeper | 18080→18080 (Spark History) |
| `nifi` | Apache NiFi | 8443→8443 |
| `trino-master` | Trino Coordinator + Worker (+ PostgreSQL metadata) | – |
| `trino-worker` | Trino Worker | 8080→8080 (Trino UI) |

**Thông số quan sát được:**
- Network Docker bridge: `trino-master = 172.18.0.5`, `trino-worker = 172.18.0.7`; datanode01/02/03 = 172.18.0.x
- HDFS: `hdfs://hadoop-namenode` (nameservice HA), Cluster ID `CID-...`, **Version 3.4.2**, 3 DataNode, mỗi DN dùng 373.11 MB / capacity 1006.85 MB, replication mặc định
- HDFS `namenode01:9000` = active, `namenode02:9000` = standby → **HA qua Zookeeper (3 node: namenode01, namenode02, spark) + JournalNode + Leader Election**
- Thư mục HDFS gốc: `/checkpoints`, `/spark-jars`, `/test`, `/user`, `/warehouse` — owner `hduser` : `supergroup`
- YARN: Capacity Scheduler, 3 NodeManager (`datanode01/02/03:37873`, HTTP 8042), tổng `<memory:11.65 GB, vCores:18>`
- Trino **version 475**; `trino-master` có `coordinator=true` và cũng chạy worker; `trino-worker` là worker thuần
- Iceberg catalog/schema: **schema `test`**, bảng `analytics(name VARCHAR, user_id VARCHAR, total_visits BIGINT)`; Iceberg metadata partition: `record_count=303686, total_size=289953`
- Kafka: 3 broker đặt trên `datanode01/02/03`; NiFi đẩy vào topic qua processor `PublishKafka_2_0`
- **NiFi flow thật (Hình 3.6)** — chuỗi processor bắt buộc phải dựng lại:
  `ExecuteSQL` → `ConvertAvroToJSON` → `JoltTransformJSON` → `ReplaceText` → `ReplaceText` → `SplitJson` → `ExtractText` → `UpdateAttribute` → `InvokeHTTP` → `PublishKafka_2_0`
  (ExecuteSQL đọc DB nguồn → chuẩn hóa JSON → tách bản ghi → gọi API → bắn vào Kafka)
- Superset: SQL Lab với database `trino`, schema `test`; câu truy vấn mẫu:
  `SELECT name, SUM(total_visits) AS total FROM analytics GROUP BY name ORDER BY total DESC;`
  Chart mẫu: PIE CHART, dimension `name`, metric `SUM(total_visits)`, sort by metric, row limit 100

### A.4. ⚠️ Điểm "lệch" của báo cáo gốc — phải quyết trước khi làm
Đề tài ghi **"Phân tích dữ liệu chứng khoán"**, nhưng **dữ liệu thực tế đã chạy trong báo cáo là dữ liệu truy cập website của các đơn vị ngành điện (EVN)**:
`Office EVN (doffice.evn.com.vn)`, `Phòng chống thiên tai (phongchongthientai.evn.com.vn)`, `Hộ chứa EVN`, `EVNPortal (portal.evn.com.vn)`, `SmartEVN-EVN (smartevn.evn.com.vn)`, `Quản lý Khoa học Công nghệ (khcn.evn.com.vn)`, với metric `total_visits` (~303.686 bản ghi, 6.833 ms Office EVN = 2,740,106 lượt).

→ Nghĩa là phần "phân tích chứng khoán" trong báo cáo **chưa thực sự có dữ liệu chứng khoán**, chỉ có tên đề tài. Phải chọn 1 trong 3 hướng ở **Phần C**.

---

## PHẦN B — LỘ TRÌNH 7 GIAI ĐOẠN (để làm lại đúng như báo cáo)

### Giai đoạn 0 — Chuẩn bị & chốt phạm vi
- [ ] Chốt phương án dữ liệu (xem Phần C) — **quyết định này ảnh hưởng mọi bước sau**
- [ ] Máy host: Docker Desktop/Engine, cấp tối thiểu **12–16 GB RAM, ≥ 6 vCPU, ≥ 60 GB disk**; WSL2 nếu Windows
- [ ] Đặt tên dự án, tạo repo `lakehouse-stock-analytics/` + sơ đồ thư mục (xem Giai đoạn 6)
- [ ] Chuẩn bị danh sách công cụ cần tải: Hadoop 3.4.2, Spark 3.5.x, Kafka 3.x, NiFi 1.28.x/2.x, Trino 475, Superset, Airflow, dbt, PostgreSQL 15+, Iceberg Spark runtime
- [ ] Xác nhận license/phiên bản và **ghim version** giống báo cáo (Hadoop 3.4.2, Trino 475) để kết quả trùng khớp

### Giai đoạn 1 — Hạ tầng Docker + mạng (tương ứng "2. Tổng quan hệ thống")
- [ ] Viết `docker-compose.yaml` (hoặc 1 image nền `hadoop` build từ `ubuntu:22.04`) tạo **9 container** đúng tên: `namenode01`, `namenode02`, `datanode01`, `datanode02`, `datanode03`, `spark`, `nifi`, `trino-master`, `trino-worker`
- [ ] Tạo 1 bridge network riêng, **gán IP tĩnh** cho từng container (giống 172.18.0.x) để cấu hình file không phải sửa khi restart
- [ ] Cài trong image nền: OpenJDK 11/17, SSH server, `sshpass`, `pdsh`, cron, Python3
- [ ] Tạo user `hduser`/group `supergroup` (đúng như HDFS owner trong ảnh)
- [ ] Cấu hình SSH key-less từ `namenode01`/`spark` tới tất cả node (Hadoop yêu cầu)
- [ ] `/etc/hosts` trên mọi container map đủ 9 hostname
- [ ] **Kiểm chứng:** `docker ps` ra đủ 9 container, `ssh datanode01 hostname` chạy không cần mật khẩu
- [ ] **Ảnh cần chụp:** Docker Desktop hiển thị 9 container + cột Ports *(= Hình 3.1)*

### Giai đoạn 2 — Hadoop HA + YARN + Spark (mục "3.1")
**2.1. HDFS High Availability**
- [ ] Zookeeper quorum trên **3 node** (`namenode01`, `namenode02`, `spark`) — cổng 2181
- [ ] Định nghĩa **nameservice `hadoop-namenode`** trỏ tới 2 NameNode (nn1, nn2)
- [ ] JournalNode trên 3 node (thường `namenode01/02` + `datanode01`) — kiểm tra đủ theo yêu cầu quorum
- [ ] `hdfs-site.xml`: `dfs.nameservices`, `dfs.ha.namenodes`, `dfs.namenode.rpc-address` (:9000), `dfs.namenode.http-address` (:9870), `dfs.journalnode.edits.dir`, `dfs.replication`
- [ ] `core-site.xml`: `fs.defaultFS=hdfs://hadoop-namenode`, `ha.zookeeper.quorum`
- [ ] Format HDFS + `hdfs zkfc -formatZK` + khởi động JournalNode → NameNode → ZKFC
- [ ] **Kiểm thử HA thật:** xác nhận nn1 = active / nn2 = standby (UI :9870 và :9871), sau đó **kill active** và chứng minh standby tự lên active (Leader Election)
- [ ] Tạo cấu trúc thư mục: `/user/hduser`, `/warehouse` (Iceberg), `/spark-jars`, `/checkpoints`, `/test`; phân quyền `hduser:supergroup`
- [ ] **Ảnh cần chụp:** `Overview 'namenode01:9000' (active)` + `Overview 'namenode02:9000' (standby)` chung 1 khung *(= Hình 3.2)*; Browse Directory `/` *(= Hình 3.4)*

**2.2. YARN + Spark**
- [ ] `yarn-site.xml` trên `spark` (ResourceManager) + audit-log/NodeManager trên `datanode01/02/03`; Capacity Scheduler
- [ ] Kiểm chứng có **3 active node**, đọc được tổng memory/vCores *(= Hình 3.5)*
- [ ] Cài Spark **on YARN**, đặt jar vào `/spark-jars`
- [ ] Chạy thử `spark-submit` ví dụ `Pi`/`WordCount` on YARN → xem job trong YARN UI và Spark History Server :18080
- [ ] **Ảnh cần chụp:** Hadoop UI tổng quan *(= Hình 3.3)*, YARN jobs của Spark *(= Hình 3.5)*

### Giai đoạn 3 — NiFi thu thập dữ liệu (mục "3.2")
- [ ] Triển khai NiFi trên container riêng, HTTPS :8443, tạo user admin
- [ ] Dựng **luồng 2 nhánh** đúng như Hình 3.6:
  - **Nhánh Batch (ghi HDFS):** nguồn → `ExecuteSQL`/`GetFile`/`InvokeHTTP` → `ConvertAvroToJSON` → `JoltTransformJSON` → ghi HDFS (`PutHDFS`) vào `/test` (raw zone)
  - **Nhánh Streaming (ghi Kafka):** `ExecuteSQL` → `ConvertAvroToJSON` → `JoltTransformJSON` → `ReplaceText` → `ReplaceText` → `SplitJson` → `ExtractText` → `UpdateAttribute` → `InvokeHTTP` → `PublishKafka_2_0`
- [ ] Cấu hình **Controller Services** (DBCPConnectionPool cho ExecuteSQL, schema registry nếu cần)
- [ ] Ý nghĩa từng processor (phải giải thích được trong báo cáo): `JoltTransformJSON` = biến đổi cấu trúc JSON; `SplitJson` = tách mảng JSON thành từng bản ghi; `ExtractText` = bóc field thành attribute; `UpdateAttribute` = gắn metadata/tham số cho API; `InvokeHTTP` = gọi API làm giàu dữ liệu
- [ ] **Kiểm chứng:** Data Provenance xanh, không có bản ghi lỗi; file xuất hiện trong HDFS; message xuất hiện trong topic Kafka
- [ ] **Ảnh cần chụp:** canvas NiFi với đủ 10 processor *(= Hình 3.6)*

### Giai đoạn 4 — Kafka (mục "3.3")
- [ ] Triển khai **3 broker** trên `datanode01/02/03` (KRaft hoặc Zookeeper — ghim lựa chọn)
- [ ] `server.properties`: `broker.id`, `listeners`, `advertised.listeners`, `log.dirs`, `default.replication.factor=3`, `min.insync.replicas=2`, `offsets.topic.replication.factor=3`
- [ ] Tạo topic nguồn (ví dụ `stock-topic` / `evn-visits`) với 3 partition, replication 3
- [ ] Test producer/consumer console; xác nhận `ISR=3`
- [ ] **Kiểm chứng:** topic tồn tại, dữ liệu chảy từ NiFi → Kafka liên tục
- [ ] **Ảnh cần chụp:** describe 1 Kafka topic (partitions, ISR) *(= Hình 3.7)*

### Giai đoạn 5 — Spark + Iceberg + PostgreSQL (phần lõi Lakehouse)
- [ ] Triển khai PostgreSQL + tạo database làm **Iceberg JDBC Catalog** (và có thể dùng chung cho Superset/Airflow metadata — nên tách DB để tránh lẫn)
- [ ] Cấu hình Spark: `spark.sql.catalog.<name>` = `org.apache.iceberg.spark.SparkCatalog`, `catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog`, `uri=jdbc:postgresql://...`, `warehouse=hdfs://hadoop-namenode/warehouse`
- [ ] Tạo namespace/schema **`test`**, bảng `analytics(name, user_id, total_visits)` đúng kiểu dữ liệu như ảnh *(= Hình 4.2)*
- [ ] **Job Batch:** Spark SQL đọc raw từ HDFS → làm sạch/chuẩn hóa → `MERGE INTO`/`INSERT` vào bảng Iceberg
- [ ] **Job Streaming:** Spark Structured Streaming đọc từ Kafka → tính toán/join/aggregation → ghi Iceberg (`foreachBatch` + checkpoint ở HDFS `/checkpoints`)
- [ ] Bật tính năng Iceberg để khoe đúng điểm mạnh đã nêu ở mục 1: **schema evolution, time travel, partition metadata**
- [ ] **Kiểm chứng:** `SELECT * FROM test.analytics` ra dữ liệu; `.snapshots`/`.history`/`.files` chạy được; `record_count` khớp ảnh nếu dùng cùng dataset
- [ ] **Ảnh cần chụp:** Spark History/YARN job đã xong *(= phần Hình 3.5)*

### Giai đoạn 6 — Trino + Superset (mục "3.4", "3.5")
**6.1. Trino**
- [ ] `config.properties`: `trino-master` → `coordinator=true, node-scheduler.include-coordinator=true`; `trino-worker` → `coordinator=false`
- [ ] `node.properties` (`node.id`, `node.environment=production`), `jvm.config`, `catalog/iceberg.properties` (Iceberg connector trỏ vào JDBC catalog + HDFS + `hive.metastore`/file metastore nếu cần), thêm `catalog/postgresql.properties`
- [ ] Khởi động cluster theo thứ tự, vào UI :8080 xác nhận **cả 2 node state = active**, `version 475`
- [ ] Viết 2–3 truy vấn liên nguồn (Iceberg JOIN PostgreSQL) để chứng minh giá trị "truy vấn kết hợp nhiều datasource"
- [ ] **Ảnh cần chụp:** Trino Worker List *(= Hình 3.8)*

**6.2. Superset**
- [ ] Triển khai Superset, `superset db upgrade`, tạo admin
- [ ] Thêm database connection tới Trino (SQLAlchemy `trino://`)
- [ ] Tạo dataset `test.analytics`, khai báo metric `SUM(total_visits)`
- [ ] Dựng dashboard gồm: **PIE CHART** (dimension `name`, metric `SUM(total_visits)`) + 1–2 chart bổ sung (bảng xếp hạng, time-series)
- [ ] **Ảnh cần chụp:** màn hình tạo chart pie *(= Hình 4.1)*, SQL Lab chạy câu `SELECT name, SUM(total_visits) ... GROUP BY name ORDER BY total DESC` *(= Hình 4.2)*

### Giai đoạn 7 — Airflow + dbt (mục "3.6")
- [ ] Triển khai Airflow (LocalExecutor hoặc CeleryExecutor; metadata DB = PostgreSQL); dựng DAG điều phối toàn pipeline
- [ ] DAG gồm các task: **NiFi trigger (REST API) → chờ dữ liệu vào HDFS/Kafka → `spark-submit` job batch + streaming → `dbt run` + `dbt test` → refresh Superset dataset**
- [ ] Cài **dbt** (dbt-trino adapter), khởi tạo project; tạo các model theo tầng: `staging` (làm sạch) → `intermediate` (join/chuẩn hóa) → `marts` (bảng phân tích `analytics` phục vụ BI)
- [ ] Thêm `sources.yml`, `schema.yml` (test not_null/unique), `dbt docs generate`
- [ ] **Kiểm chứng:** DAG chạy xanh end-to-end; lineage trong dbt docs hiển thị đủ tầng
- [ ] **Ảnh cần chụp:** Airflow Graph/DAG list *(= Hình 3.10)*, dbt docs lineage *(= Hình 3.11)*

### Giai đoạn 8 — Kết quả & đo lường (mục "4")
- [ ] **Luồng A — đo hiệu năng:** nạp dataset **hàng chục triệu dòng**, ghi lại thời gian từng chặng (NiFi ingest, Spark job, Trino query, Superset render) + tài nguyên tiêu thụ → bảng số liệu
- [ ] **Luồng B — tự động hóa qua API:** dùng `InvokeHTTP` trong NiFi lấy dữ liệu từ API định kỳ theo lịch Airflow, chứng minh pipeline tự chạy không cần thao tác tay
- [ ] Ghi lại 3 kết luận như báo cáo: hệ thống đủ chức năng cơ bản; hiệu năng tốt dù chỉ chạy Docker; có web UI thân thiện cho người dùng cuối
- [ ] **Ảnh cần chụp:** biểu đồ trên Superset *(= Hình 4.1)*, truy vấn SQL trên Superset *(= Hình 4.2)*

### Giai đoạn 9 — Viết báo cáo & bàn giao
- [ ] Viết lại đúng 5 chương, đúng số hình (Hình 1 → Hình 4.2) — dùng ảnh của chính mình, không copy ảnh báo cáo gốc
- [ ] Bổ sung bảng version, sơ đồ kiến trúc tự vẽ (thay Hình 2), bảng kết quả đo hiệu năng
- [ ] Đóng gói repo: `docker-compose`, config từng service, NiFi flow template (`.json` export được), notebook/script Spark, dbt project, DAG Airflow, README hướng dẫn `docker compose up` từ đầu
- [ ] Checklist "người khác clone về chạy lại được"

---

## PHẦN C — CHỌN HƯỚNG DỮ LIỆU (quyết định ở Giai đoạn 0)

| | Phương án | Ưu | Nhược |
|---|---|---|---|
| **C1** | **Sao chép y nguyên báo cáo**: dùng dataset lượt truy cập website EVN (`name`, `user_id`, `total_visits`, ~303.686 dòng) | Bám sát 100% báo cáo gốc, mọi ảnh/kết quả đều tái lập được, rủi ro thấp nhất | Đề tài ghi "chứng khoán" nhưng dữ liệu không phải chứng khoán → vẫn lệch như báo cáo gốc |
| **C2** | **Làm đúng tên đề tài**: dữ liệu chứng khoán thật (ví dụ VN-Index/HOSE qua `ssi`/`vnstock`/`TCBS` API, hoặc nguồn free như Stooq/Yahoo) — bảng Fact `fact_stock_price(symbol, date, open, high, low, close, volume)` + Dim `dim_symbol(symbol, exchange, sector)` | Đúng tên đề tài, phân tích có ý nghĩa thật (MA, top biến động, thanh khoản), dễ ghi điểm | Phải thiết kế schema + dbt models mới; một số API chứng khoán VN cần key |
| **C3** | **Kết hợp cả hai**: pipeline chạy được cả nhánh batch (chứng khoán) lẫn nhánh streaming (dữ liệu đo/đếm), đúng tinh thần "2 luồng dữ liệu" ở mục 4 | Đầy đủ nhất, thể hiện rõ năng lực Lakehouse | Công sức lớn nhất, thời gian dài nhất |

**Gợi ý của tôi: C2** — vì mục tiêu của bạn là "làm lại một dự án giống vậy" nhưng đúng **tên đề tài**; kiến trúc/hạ tầng (Giai đoạn 1–7) giữ y nguyên báo cáo, chỉ thay dataset + dbt models + dashboard. Nếu bạn cần điểm "giống báo cáo" tối đa thì chọn C1.

---

## PHẦN D — RỦI RO & ĐIỂM DỄ SAI

1. **Tài nguyên máy** — 9 container + Hadoop + Spark + Trino + Superset + Airflow rất nặng. Dưới 12 GB RAM sẽ OOM. Nên bật giới hạn `mem_limit` từng container và giảm `dfs.replication=2` khi thử nếu cần.
2. **HDFS HA hay hỏng** — quên format ZK, sai `cluster-id` giữa 2 NameNode, JournalNode không đủ quorum. Phải `hdfs namenode -bootstrapStandby` trước khi start nn2.
3. **IP tĩnh vs Docker DNS** — báo cáo dùng IP cứng (172.18.0.x). Nếu dùng DNS nội bộ Docker thì khỏe hơn nhưng phải sửa lại nhiều config; **chọn một cách và giữ nhất quán**.
4. **Iceberg catalog** — nhầm giữa `HiveCatalog` và `JdbcCatalog`. Báo cáo ghi rõ metadata ở **PostgreSQL** → dùng `JdbcCatalog`.
5. **Trino đọc Iceberg** — cần chung warehouse HDFS + catalog; dễ lỗi Kerberos/quyền thư mục `/warehouse` (owner `hduser`).
6. **Version mismatch** — Spark–Iceberg runtime, Kafka client–broker, Trino–connector. Phải ghim version ngay từ Giai đoạn 0.
7. **NiFi `PublishKafka_2_0`** — processor này theo phiên bản; NiFi 2.x có thể dùng tên khác → ghi chú sai khác nếu có.
8. **Airflow gọi NiFi/Spark** — phải mở cổng và xử lý auth cho NiFi REST API (token), dễ tắc nhất trong toàn bộ pipeline.
9. **Bản quyền hình ảnh/tài liệu** — không sao chép ảnh chụp của báo cáo gốc vào bài của mình; chỉ dùng làm tham chiếu, tự chụp lại hệ thống của mình.
10. **Chứng khoán (nếu chọn C2)** — API có rate limit; cần cơ chế cache/backoff và mô phỏng `InvokeHTTP` đúng như báo cáo.

---

## PHẦN E — THỨ TỰ ƯU TIÊN & ƯỚC LƯỢNG

| Giai đoạn | Nội dung | Ưu tiên | Ước lượng |
|---|---|---|---|
| 0 | Chuẩn bị, chốt dữ liệu | Bắt buộc | 0.5 ngày |
| 1 | Docker 9 node + SSH + network | Bắt buộc | 1–2 ngày |
| 2 | Hadoop HA + YARN + Spark | Bắt buộc (khó nhất) | 3–5 ngày |
| 3 | NiFi flow batch + streaming | Bắt buộc | 1–2 ngày |
| 4 | Kafka 3 broker | Bắt buộc | 1 ngày |
| 5 | Spark + Iceberg + PostgreSQL | Bắt buộc (lõi) | 2–3 ngày |
| 6 | Trino + Superset | Bắt buộc | 1–2 ngày |
| 7 | Airflow + dbt | Bắt buộc | 2–3 ngày |
| 8 | Đo hiệu năng + tự động hóa API | Bắt buộc | 1–2 ngày |
| 9 | Viết báo cáo + đóng gói | Bắt buộc | 2–3 ngày |
| | **Tổng** | | **~15–24 ngày làm việc** |

**Thứ tự tối ưu để luôn có demo chạy được:** (1) Docker → (2) HDFS đơn giản trước, HA sau → (5) Spark + Iceberg + Postgres → (6a) Trino + Superset (đã có demo truy vấn/hiển thị) → (3) NiFi → (4) Kafka → (6b) Superset đầy đủ → (7) Airflow + dbt → (8) đo lường → (9) báo cáo. Mẹo: dựng "đường găng" HDFS→Spark→Iceberg→Trino→Superset trước để có sản phẩm trưng bày sớm.

---

## PHẦN F — CẤU TRÚC THƯ MỤC DỰ ÁN ĐỀ XUẤT

```
lakehouse-stock-analytics/
├── docker/
│   ├── docker-compose.yaml
│   ├── base/Dockerfile            # ubuntu:22.04 + java + ssh + python + hadoop
│   ├── hadoop/                    # core-site, hdfs-site, yarn-site, mapred-site
│   ├── spark/                     # spark-defaults.conf, iceberg runtime jar
│   ├── kafka/                     # server.properties x3 broker
│   ├── nifi/                      # flow.json (export từ UI), conf
│   ├── trino/                     # config.properties, node.properties, catalog/*.properties
│   ├── superset/                  # superset_config.py
│   ├── airflow/                   # dags/, requirements
│   └── postgres/                  # init.sql (iceberg_catalog, superset, airflow)
├── spark-jobs/                    # batch_ingest.py, streaming_ingest.py, iceberg_ddl.sql
├── dbt/                           # dbt_project.yml, models/{staging,intermediate,marts}, tests
├── etl/                           # script lấy dữ liệu API (chứng khoán), seed dữ liệu lớn
├── docs/                          # sơ đồ kiến trúc, ảnh chụp minh chứng, bảng đo hiệu năng
├── report/                        # báo cáo 5 chương (.docx/.pdf)
└── README.md                      # hướng dẫn chạy lại từ số 0
```

---

## VIỆC CẦN BẠN XÁC NHẬN TRƯỚC KHI ĐI TIẾP

1. **Dữ liệu:** chọn **C1** (giống hệt: lượt truy cập EVN), **C2** (đúng đề tài: chứng khoán), hay **C3** (cả hai)?
2. **Tài nguyên máy host:** RAM/CPU/disk hiện có bao nhiêu? (quyết định có phải giảm số node xuống 6–7 hay không)
3. **Số container:** giữ đúng **9** như báo cáo, hay cho phép gộp (ví dụ PostgreSQL chung `trino-master`) để nhẹ máy?
4. **Mức độ "giống":** cần trùng cả *version* (Hadoop 3.4.2 / Trino 475) và *IP/port* hay chỉ cần cùng kiến trúc?
5. **Bước tiếp theo bạn muốn tôi làm:** viết `docker-compose.yaml` + Dockerfile nền, hay viết đề cương báo cáo trước, hay dựng `docker/` config Hadoop HA trước?
