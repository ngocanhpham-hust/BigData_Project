# Tài liệu chiến lược dự án

Thư mục này chứa tài liệu định hướng, **không phải mã nguồn**.

| File | Nội dung |
|---|---|
| `lo_trinh_lam_lai_du_an.md` | Lộ trình 9 giai đoạn làm lại dự án Data LakeHouse |
| `ghi_nho_k8s_lakehouse.md` | Ghi nhớ kỹ thuật từ 2 báo cáo Kubernetes: topology 3 node, 9 thành phần, 10 bài học |

## Ba tài liệu nguồn

1. **Báo cáo Docker/EVN** (14 trang): Lakehouse trên 9 container Docker + HDFS, dữ liệu lượt truy cập website ngành điện.
2. **Báo cáo Kubernetes chi tiết** (53 trang): cùng mô hình nhưng trên cụm K8s 3 node on-premises, dùng **MinIO thay HDFS**, thêm OpenMetadata/OpenSearch/CKAN, 2 case study.
3. **Cheat sheet Kubernetes** (37 trang): bản rút gọn thực hành của tài liệu số 2.

## Quyết định kiến trúc đã chốt

- **Hướng đi:** triển khai trên **Kubernetes**, không dùng Docker Compose thuần → đáp ứng yêu cầu "không chỉ dùng mỗi Docker".
- **Hạ tầng:** cụm **k3d 3 node giả lập** (1 server + 2 agent) trên **một máy 16GB RAM**.
- **Storage:** bỏ HDFS, dùng **MinIO** (S3-compatible) theo mô hình Medallion 3 vùng.
- **Cắt bớt để vừa 16GB:** bỏ CKAN, Superset; OpenSearch + OpenMetadata chạy theo đợt.
- **Spark:** bỏ cụm Master/Worker độc lập, chuyển sang **Spark-on-Kubernetes** (executor sinh theo nhu cầu).
