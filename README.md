# Data LakeHouse - Phan tich du lieu chung khoan

He thong Data LakeHouse phan tich du lieu chung khoan, chay thu nghiem tren Docker
(gia lap 9 may = 9 container Ubuntu 22.04).

> Trang thai: dang o giai doan dung khung (scaffold). Chua cai dat dich vu.

## 1. Kien truc tong the

```
Nguon ngoai (API chung khoan / DB / file)
        |
        v
   [ Apache NiFi ]  -- batch -->  [ HDFS ]  --+
        |                                     |
        +-- streaming --> [ Kafka ]           |
                              |               |
                              v               v
                     [ Spark / Spark Streaming ]
                              |
                              v
                     [ Apache Iceberg ]  <-- metadata -- [ PostgreSQL ]
                              |
                              v
                     [ dbt ]  mo hinh hoa du lieu
                              |
                              v
                     [ Trino ]  truy van phan tan
                              |
                              v
                     [ Apache Superset ]  dashboard
                              |
                     dieu phoi toan bo: [ Apache Airflow ]
```

## 2. Thanh phan & phien ban muc tieu

| Thanh phan | Vai tro | Phien ban |
|---|---|---|
| Docker / Compose | Ha tang gia lap 9 node | Docker Desktop |
| Ubuntu | OS trong container | 22.04 |
| Hadoop HDFS | Luu tru phan tan, High Availability | 3.4.2 |
| YARN | Quan ly tai nguyen | 3.4.2 |
| Apache Spark | Xu ly batch + streaming | 3.5.x |
| Apache Iceberg | Table format (ACID, time travel) | 1.5+ |
| PostgreSQL | Iceberg JDBC catalog + metadata | 15+ |
| Apache NiFi | Thu thap & chuan hoa du lieu | 1.28+ |
| Apache Kafka | Message broker (streaming) | 3.x |
| Trino | SQL query engine phan tan | 475 |
| Apache Superset | Truc quan hoa | 3.x |
| Apache Airflow | Lap lich & dieu phoi | 2.9+ |
| dbt | Mo hinh hoa du lieu (ELT) | 1.7+ |

## 3. Topology 9 container

| Container | Vai tro | Port (host) |
|---|---|---|
| namenode01 | NameNode active + Zookeeper | 9870 |
| namenode02 | NameNode standby + Zookeeper | 9871 |
| datanode01 | DataNode + NodeManager + Kafka broker | - |
| datanode02 | DataNode + NodeManager + Kafka broker | - |
| datanode03 | DataNode + NodeManager + Kafka broker | - |
| spark | Spark master + ResourceManager + Zookeeper | 18080 |
| nifi | Apache NiFi | 8443 |
| trino-master | Trino Coordinator + Worker | - |
| trino-worker | Trino Worker | 8080 |

## 4. Cau truc thu muc

```
.
├── docker/              # toan bo ha tang container
├── spark-jobs/          # job Spark batch & streaming ghi Iceberg
├── dbt/                 # dbt project: staging -> intermediate -> marts
├── etl/                 # script lay du lieu API chung khoan
├── data/                # du lieu local (raw/processed) - khong commit
├── docs/                # so do, anh minh chung, bang do hieu nang
├── report/              # bao cao 5 chuong
└── scripts/             # script tien ich
```

## 5. Yeu cau may

- RAM: >= 12 GB (khuyen nghi 16 GB)
- CPU: >= 6 vCPU
- Disk: >= 60 GB trong
- Docker Desktop da bat va dang chay

## 6. Bat dau

```bash
docker --version && docker compose version
cd docker && docker compose up -d
docker compose ps
```

## 7. Lo trinh thuc hien

- [ ] GD 0 - Chot du lieu (chung khoan), version, cau truc du an
- [ ] GD 1 - Docker 9 node + mang + SSH key-less
- [ ] GD 2 - Hadoop HDFS HA + Zookeeper + YARN + Spark
- [ ] GD 3 - NiFi: luong thu thap batch + streaming
- [ ] GD 4 - Kafka 3 broker
- [ ] GD 5 - Spark + Iceberg + PostgreSQL (loi Lakehouse)
- [ ] GD 6 - Trino + Superset
- [ ] GD 7 - Airflow + dbt
- [ ] GD 8 - Do hieu nang (hang chuc trieu dong) + tu dong hoa qua API
- [ ] GD 9 - Viet bao cao + dong goi

## 8. Ghi chu

- Khong commit du lieu tho, mat khau, key. Xem .gitignore.
- Toan bo version duoc ghim co dinh de ket qua tai lap duoc.
