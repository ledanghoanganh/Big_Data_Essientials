# Hệ Sinh Thái Big Data Phân Tán: Apache Hadoop 3.5.0, Hive 4.1.0 & Spark 4.2.0

> **Kiến trúc phân tán Hybrid:** Kết hợp **Docker Container** và **Tailscale Mesh VPN Sidecar** để triển khai cụm dữ liệu lớn đa nút (Multi-node Cluster) hoạt động liên máy tính vật lý (Laptop cá nhân qua Internet) mà không cần địa chỉ IP tĩnh và không cần mở cổng NAT/Port Forwarding router.

---

## Mục Lục

- [1. Tổng Quan Kiến Trúc & Cấu Trúc Dự Án](#1-tổng-quan-kiến-trúc--cấu-trúc-dự-án)
  - [1.1. Sơ đồ kiến trúc Hybrid (Docker + Tailscale Sidecar)](#11-sơ-đồ-kiến-trúc-hybrid-docker--tailscale-sidecar)
  - [1.2. Phân bổ các Node trong Cụm](#12-phân-bổ-các-node-trong-cụm)
  - [1.3. Cấu trúc thư mục dự án](#13-cấu-trúc-thư-mục-dự-án)
- [2. Hướng Dẫn Cài Đặt & Vận Hành Toàn Cụm (Step-by-Step)](#2-hướng-dẫn-cài-đặt--vận-hành-toàn-cụm-step-by-step)
  - [Bước 1: Chuẩn bị Mạng Tailscale Mesh VPN & Cấu hình Direct P2P](#bước-1-chuẩn-bị-mạng-tailscale-mesh-vpn--cấu-hình-direct-p2p)
  - [Bước 2: Chuẩn bị Bộ cài Offline & Tệp cấu hình `.env`](#bước-2-chuẩn-bị-bộ-cài-offline--tệp-cấu-hình-env)
  - [Bước 3: Khởi tạo Container bằng Docker Compose](#bước-3-khởi-tạo-container-bằng-docker-compose)
  - [Bước 4: Thiết lập & Phân phối Khóa SSH không mật khẩu](#bước-4-thiết-lập--phân-phối-khóa-ssh-không-mật-khẩu)
  - [Bước 5: Khởi động Cụm Hadoop (HDFS, YARN, MapReduce)](#bước-5-khởi-động-cụm-hadoop-hdfs-yarn-mapreduce)
  - [Bước 6: Triển khai Kho Dữ Liệu Apache Hive & DBeaver](#bước-6-triển-khai-kho-dữ-liệu-apache-hive--dbeaver)
  - [Bước 7: Thực thi Tính toán Phân tán với Apache Spark & PySpark](#bước-7-thực-thi-tính-toán-phân-tán-với-apache-spark--pyspark)
  - [Bước 8: Danh Sách Giao Diện Quản Trị Web UI](#bước-8-danh-sách-giao-diện-quản-trị-web-ui)
  - [Bước 9: Quy trình Dừng Hệ Thống An Toàn](#bước-9-quy-trình-dừng-hệ-thống-an-toàn)
- [3. Lưu Ý Chuyên Sâu: Docker + Tailscale Sidecar & Các Vấn Đề Đặc Thù](#3-lưu-ý-chuyên-sâu-docker--tailscale-sidecar--các-vấn-đề-đặc-thù)
  - [3.1. Cơ chế hoạt động của Tailscale Sidecar (`network_mode: service:tailscale`)](#31-cơ-chế-hoạt-động-của-tailscale-sidecar-network_mode-servicetailscale)
  - [3.2. Vấn đề Đóng gói Image, Phân phối & Tính Khả Di (Portability)](#32-vấn-đề-đóng-gói-image-phân-phối--tính-khả-di-portability)
  - [3.3. Tối ưu kết nối P2P Direct WireGuard vs Trạm trung chuyển Relay (DERP)](#33-tối-ưu-kết-nối-p2p-direct-wireguard-vs-trạm-trung-chuyển-relay-derp)
  - [3.4. Tại sao phải thêm SSH Key localhost trên máy Master](#34-tại-sao-phải-thêm-ssh-key-localhost-trên-máy-master)
  - [3.5. Xung đột tài nguyên Java 17 và Virtual Memory trên YARN (`vmem-check-enabled`)](#35-xung-đột-tài-nguyên-java-17-và-virtual-memory-trên-yarn-vmem-check-enabled)
  - [3.6. Cơ chế Khóa Đơn tiến trình của Apache Derby Metastore](#36-cơ-chế-khóa-đơn-tiến-trình-của-apache-derby-metastore)
  - [3.7. Hiện tượng HDFS Safe Mode (Read-Only)](#37-hiện-tượng-hdfs-safe-mode-read-only)
  - [3.8. Tối ưu nạp thư viện Spark JARs trên YARN qua VPN](#38-tối-ưu-nạp-thư-viện-spark-jars-trên-yarn-qua-vpn)
- [4. Bảng Tra Cứu Xử Lý Sự Cố Nhanh (Troubleshooting Quick Reference)](#4-bảng-tra-cứu-xử-lý-sự-cố-nhanh-troubleshooting-quick-reference)

---

## 1. Tổng Quan Kiến Trúc & Cấu Trúc Dự Án

### 1.1. Sơ đồ kiến trúc Hybrid (Docker + Tailscale Sidecar)

Triển khai cụm Big Data phân tán thực tế trên các máy tính cá nhân (nhiều hệ điều hành khác nhau, nằm sau các mạng WiFi/NAT gia đình khác nhau) gặp rào cản lớn về việc định tuyến cổng và bảo mật. Dự án sử dụng mô hình **Tailscale Sidecar Pattern**:
- Mỗi node bao gồm **2 container** chạy song song:
  1. `tailscale`: Khởi chạy daemon `tailscaled`, sử dụng card mạng ảo `/dev/net/tun`, tham gia vào mạng ảo riêng Tailnet qua giao thức mã hóa WireGuard P2P.
  2. `hadoop-node`: Chứa toàn bộ môi trường Ubuntu 24.04 LTS, Java 17, Hadoop 3.5.0, Hive 4.1.0, Spark 4.2.0. Container này dùng thiết lập `network_mode: "service:tailscale"` để **chia sẻ chung toàn bộ Network Namespace** với container Tailscale, trực tiếp sở hữu IP mạng ảo (dải `100.x.y.z`) và toàn quyền mở mọi cổng dịch vụ mà không cần cấu hình NAT/Port Forwarding.

```text
               +--------------------------------------------------------+
               |             MẠNG ẢO TAILSCALE MESH VPN (100.x.y.z)      |
               +--------------------------------------------------------+
                     |                     |                     |
     +---------------+                     |                     +---------------+
     |                                     |                                     |
+----+---------------------+     +---------+-----------------+     +-------------+-----------+
|    Laptop 1 (Master)     |     |   Laptop 2 (Worker 1)     |     |   Laptop 3 (Worker 2)   |
| [ts-anh24162006-master]  |     |   [ts-hoc-slave1]         |     |   [ts-nguyen-slave2]    |
| (Tailscale Sidecar)      |     | (Tailscale Sidecar)       |     | (Tailscale Sidecar)     |
|          ^               |     |          ^                |     |          ^              |
|   network_mode: service  |     |   network_mode: service   |     |   network_mode: service |
|          v               |     |          v                |     |          v              |
| [anh24162006-master]     |     | [hoc-slave1]              |     | [nguyen-slave2]         |
| - HDFS NameNode (9000)   |     | - DataNode (9864/9866)    |     | - DataNode (9864/9866)  |
| - NameNode Web (9870)    |     | - NodeManager (8042)      |     | - NodeManager (8042)    |
| - YARN RM (8032/8088)    |     | - Spark Worker            |     | - Spark Worker          |
| - JobHistory (10020/19888|     +---------------------------+     +-------------------------+
| - HiveServer2 (10000)    |
| - Spark Master (7077/8080|
+--------------------------+
```

### 1.2. Phân bổ các Node trong Cụm

Toàn bộ các node dùng chung tài khoản Linux nội bộ: **`hadoopanh24162006`** (mật khẩu: **`hadoopanh24162006`**), chạy dịch vụ SSH trên cổng **`2222`**.

| STT | Hostname (`NODE_HOSTNAME`) | Vai trò (`NODE_ROLE`) | Dịch vụ chính đảm nhiệm | Cổng dịch vụ tiêu biểu |
| :-: | :--- | :---: | :--- | :--- |
| 1 | `anh24162006-master` | `master` | NameNode, SecondaryNameNode, ResourceManager, JobHistoryServer, HiveServer2, Spark Master | `9000`, `9870`, `8088`, `10000`, `19888`, `7077`, `8080` |
| 2 | `hoc-slave1` | `slave` | DataNode, YARN NodeManager, Spark Worker | `9864`, `9866`, `8042` |
| 3 | `nguyen-slave2` | `slave` | DataNode, YARN NodeManager, Spark Worker | `9864`, `9866`, `8042` |
| 4 | `anh24133004-slave3` | `slave` | DataNode, YARN NodeManager, Spark Worker | `9864`, `9866`, `8042` |

### 1.3. Cấu trúc thư mục dự án

```text
Big_Data_Essientials/
├── Dockerfile                  # Bản thiết kế Ubuntu 24.04, OpenJDK 17, Hadoop 3.5.0, Hive 4.1.0, Spark 4.2.0
├── docker-compose.yml          # Điều phối 2 service: tailscale (sidecar) và hadoop-node
├── entrypoint.sh               # Kịch bản gắn IP Tailscale vào /etc/hosts, bật sshd, tự động format NameNode
├── .env.example                # Tệp cấu hình mẫu chứa TS_AUTHKEY, NODE_HOSTNAME, NODE_ROLE
├── config/                     # Thư mục chứa các tệp cấu hình được mount vào container
│   ├── core-site.xml           # Cấu hình HDFS URI, buffer size, proxyuser cho HiveServer2
│   ├── hdfs-site.xml           # Cấu hình replication (2), blocksize (128MB), DataNode hostname, heartbeat
│   ├── yarn-site.xml           # Cấu hình ResourceManager, RAM NodeManager (4GB), vmem-check-enabled=false
│   ├── mapred-site.xml         # Cấu hình framework MapReduce trên YARN, ShuffleHandler, JobHistoryServer
│   ├── capacity-scheduler.xml  # Cấu hình bộ lập lịch tài nguyên hàng đợi YARN
│   ├── hive-site.xml           # Cấu hình Hive Metastore (Derby Embedded), HDFS warehouse, HiveServer2 port 10000
│   ├── spark-defaults.conf     # Tối ưu Spark on YARN: spark.yarn.jars=local://..., KryoSerializer, RAM driver/executor
│   └── workers                 # Danh sách Worker nodes (hoc-slave1, nguyen-slave2, anh24133004-slave3)
├── offline_pkgs/               # Thư mục chứa các gói nén cài đặt offline (không đẩy lên Git)
│   ├── hadoop-3.5.0.tar.gz
│   ├── apache-hive-4.1.0-bin.tar.gz
│   ├── spark-4.2.0-bin-hadoop3.gz
│   └── commons-collections-3.2.2.jar
├── test_bigdata/               # Thư mục mã nguồn kiểm thử và dữ liệu thực nghiệm (mount vào container)
│   ├── data/                   # Chứa 4 file CSV E-commerce thực tế và file văn bản test
│   ├── hive/                   # Các script HiveQL khởi tạo Data Warehouse và truy vấn phân tích
│   ├── spark/                  # Các script PySpark MapReduce (WordCount, Mean, Median, StdDev, SalesByProduct)
│   ├── generate_data.py        # Script sinh dữ liệu thực nghiệm mẫu
│   └── test.sh                 # Script chạy bài kiểm thử MapReduce WordCount mẫu
└── docs/                       # Tài liệu hướng dẫn chi tiết và báo cáo kỹ thuật
```

---

## 2. Hướng Dẫn Cài Đặt & Vận Hành Toàn Cụm (Step-by-Step)

### Bước 1: Chuẩn bị Mạng Tailscale Mesh VPN & Cấu hình Direct P2P

#### 1.1. Đăng ký Tailscale và lấy Khóa xác thực (Auth Key)
1. Đăng ký tài khoản miễn phí tại [https://tailscale.com](https://tailscale.com).
2. Vào **Tailscale Admin Console** → **Settings** → **Personal Settings** → **Keys** → **Generate auth key**.
3. Chọn các tùy chọn:
   - **Reusable**: Bật (để cả 4 máy cùng dùng chung 1 key tham gia vào 1 Tailnet).
   - **Expiration**: Đặt thời gian phù hợp (ví dụ: 90 ngày).
   - **Ephemeral**: Tắt (để thiết bị giữ cố định thông tin cấu hình).
4. Lưu chuỗi khóa nhận được (dạng `tskey-auth-k...`) để điền vào file `.env`.

#### 1.2. Mở cổng Firewall trên máy Host để kích hoạt kết nối Trực tiếp (Direct P2P)
Để các máy tính vật lý truyền tải dữ liệu HDFS khối lớn và Shuffle MapReduce với tốc độ mạng nội bộ cao nhất, cần cho phép kết nối ngang hàng trực tiếp (Direct Connection), tránh bị đi vòng qua các trạm trung chuyển trung gian (DERP Relay).
- **Trên Windows** (Mở PowerShell với quyền **Administrator** trên từng máy):
  ```powershell
  # Bật IPv6 và mở cổng UDP 41641/41642 cho kết nối trực tiếp Tailscale WireGuard
  Enable-NetAdapterBinding -Name "*" -ComponentID ms_tcpip6
  Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip6\Parameters" -Name "DisabledComponents" -Value 0 -Type DWord -ErrorAction SilentlyContinue
  New-NetFirewallRule -DisplayName "Tailscale Direct UDP 41641" -Direction Inbound -Protocol UDP -LocalPort 41641 -Action Allow -Profile Any -ErrorAction SilentlyContinue
  New-NetFirewallRule -DisplayName "Tailscale Direct UDP 41642" -Direction Inbound -Protocol UDP -LocalPort 41642 -Action Allow -Profile Any -ErrorAction SilentlyContinue
  ```
- **Trên Linux**:
  ```bash
  sudo ufw allow 41641/udp
  sudo ufw allow 41642/udp
  ```

#### 1.3. Ánh xạ Hostname vào file `hosts` của máy thật (Host OS)
Sau khi container khởi chạy và nhận IP Tailscale, để trình duyệt máy thật mở được Web UI và công cụ DBeaver kết nối được bằng tên máy (`anh24162006-master`), hãy tra cứu IP Tailscale của từng node trên Tailscale Admin Console (hoặc qua lệnh `tailscale status`) rồi thêm vào file `hosts` máy thật:
- **Trên Windows** (PowerShell Administrator):
  ```powershell
  # Ánh xạ theo đúng dải IP Tailscale (100.x.y.z) thực tế của 4 container:
  Add-Content -Path "$env:SystemRoot\System32\drivers\etc\hosts" -Value "`n100.93.138.6 anh24162006-master`n100.83.192.69 hoc-slave1`n100.99.120.86 nguyen-slave2`n100.111.208.15 anh24133004-slave3"
  ```
- **Trên Linux / macOS**:
  ```bash
  sudo tee -a /etc/hosts <<EOF
  100.93.138.6 anh24162006-master
  100.83.192.69 hoc-slave1
  100.99.120.86 nguyen-slave2
  100.111.208.15 anh24133004-slave3
  EOF
  ```

---

### Bước 2: Chuẩn bị Bộ cài Offline & Tệp cấu hình `.env`

#### 2.1. Tải các gói cài đặt vào thư mục `offline_pkgs/`
Sao chép hoặc tải các gói phân phối chính thức của Apache vào thư mục `offline_pkgs/` (Dockerfile sẽ dùng lệnh `ADD` để bung trực tiếp vào container lúc build image):
- `offline_pkgs/hadoop-3.5.0.tar.gz`
- `offline_pkgs/apache-hive-4.1.0-bin.tar.gz`
- `offline_pkgs/spark-4.2.0-bin-hadoop3.gz`
- `offline_pkgs/commons-collections-3.2.2.jar` *(Tệp JAR tương thích bổ sung cho Hive 4.1.0)*

#### 2.2. Tạo file `.env` trên từng máy tính
Tại thư mục gốc của dự án trên mỗi máy, tạo file `.env` với nội dung tương ứng theo vai trò của máy đó:

- **Trên máy Master (`anh24162006-master`):**
  ```env
  TS_AUTHKEY=tskey-auth-kXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
  NODE_HOSTNAME=anh24162006-master
  NODE_ROLE=master
  ```
- **Trên máy Worker 1 (`hoc-slave1`):**
  ```env
  TS_AUTHKEY=tskey-auth-kXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
  NODE_HOSTNAME=hoc-slave1
  NODE_ROLE=slave
  ```
- **Trên máy Worker 2 (`nguyen-slave2`):**
  ```env
  TS_AUTHKEY=tskey-auth-kXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
  NODE_HOSTNAME=nguyen-slave2
  NODE_ROLE=slave
  ```
- **Trên máy Worker 3 (`anh24133004-slave3`):**
  ```env
  TS_AUTHKEY=tskey-auth-kXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX
  NODE_HOSTNAME=anh24133004-slave3
  NODE_ROLE=slave
  ```

---

### Bước 3: Khởi tạo Container bằng Docker Compose

Thực hiện lệnh sau tại thư mục gốc dự án trên **cả 4 máy tính**:

```bash
docker compose up -d --build
```

Kiểm tra trạng thái các container đang chạy:
```bash
docker ps
```
Bạn sẽ thấy trên mỗi máy có **2 container** đang hoạt động:
1. `ts-<hostname>` (ví dụ: `ts-anh24162006-master`): Container Tailscale Sidecar.
2. `<hostname>` (ví dụ: `anh24162006-master`): Container Hadoop Node mang trạng thái `Up`.

Truy cập vào shell của container với user `hadoopanh24162006`:
- **Trên máy Master:**
  ```bash
  docker exec -it anh24162006-master su - hadoopanh24162006
  ```
- **Trên máy Worker:**
  ```bash
  docker exec -it hoc-slave1 su - hadoopanh24162006
  # Hoặc: docker exec -it nguyen-slave2 su - hadoopanh24162006
  # Hoặc: docker exec -it anh24133004-slave3 su - hadoopanh24162006
  ```

---

### Bước 4: Thiết lập & Phân phối Khóa SSH không mật khẩu

#### 4.1. Phân phối khóa SSH từ Master sang toàn bộ Cụm (Chạy trên `anh24162006-master`)
Trong container của máy Master, chạy lệnh `ssh-copy-id` để chuyển khóa công khai RSA sang chính nó và cả 3 Worker Node qua cổng `2222` (nhập mật khẩu là `hadoopanh24162006` khi được hỏi):

```bash
# Copy khóa vào chính máy Master (Bắt buộc cho kịch bản khởi động của Hadoop)
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@anh24162006-master

# Copy khóa sang lần lượt 3 máy Worker
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@hoc-slave1
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@nguyen-slave2
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@anh24133004-slave3
```

#### 4.2. Kiểm tra kết nối SSH không mật khẩu
Từ terminal máy Master, kiểm tra kết nối tới tất cả các node:
```bash
ssh anh24162006-master hostname
ssh hoc-slave1 hostname
ssh nguyen-slave2 hostname
ssh anh24133004-slave3 hostname
```
Nếu lệnh trả về đúng tên Hostname của từng node mà **không yêu cầu nhập mật khẩu**, hệ thống SSH đã sẵn sàng.

---

### Bước 5: Khởi động Cụm Hadoop (HDFS, YARN, MapReduce)

Thực hiện toàn bộ các thao tác dưới đây từ bên trong container **`anh24162006-master`** (user `hadoopanh24162006`):

#### 5.1. Định dạng HDFS NameNode (Chỉ chạy 1 lần duy nhất khi tạo cụm mới)
> **Lưu ý:** `entrypoint.sh` đã tự động kiểm tra và format nếu thư mục metadata chưa có. Nếu cần format thủ công lại từ đầu:
```bash
hdfs namenode -format -force
```

#### 5.2. Khởi động dịch vụ HDFS và YARN
```bash
# 1. Khởi động HDFS (NameNode, SecondaryNameNode, DataNodes) và YARN (ResourceManager, NodeManagers)
start-all.sh

# 2. Khởi động dịch vụ lưu lịch sử công việc MapReduce
mapred --daemon start historyserver
```

#### 5.3. Khởi động / Khởi động lại dịch vụ trên các máy Worker (nếu cần điều phối riêng biệt)
```bash
# Khởi động dịch vụ trên Worker 1
ssh hoc-slave1 '~/hadoop/bin/hdfs --daemon start datanode; ~/hadoop/bin/yarn --daemon start nodemanager; jps'

# Khởi động dịch vụ trên Worker 2
ssh nguyen-slave2 '~/hadoop/bin/hdfs --daemon start datanode; ~/hadoop/bin/yarn --daemon start nodemanager; jps'

# Khởi động dịch vụ trên Worker 3
ssh anh24133004-slave3 '~/hadoop/bin/hdfs --daemon start datanode; ~/hadoop/bin/yarn --daemon start nodemanager; jps'
```

#### 5.4. Kiểm tra danh sách tiến trình Java (`jps`)
Gõ `jps` trên từng node để kiểm tra:
- **Trên Master (`anh24162006-master`):** Cần thấy tối thiểu 4 tiến trình:
  - `NameNode`
  - `SecondaryNameNode`
  - `ResourceManager`
  - `JobHistoryServer`
- **Trên từng Worker (`hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3`):** Cần thấy tối thiểu 2 tiến trình:
  - `DataNode`
  - `NodeManager`

#### 5.5. Kiểm tra trạng thái hoạt động của HDFS & YARN
```bash
# Tắt Safe Mode nếu NameNode đang ở trạng thái Read-Only
hdfs dfsadmin -safemode leave

# Kiểm tra báo cáo DataNode (Cần hiển thị "Live datanodes (3)")
hdfs dfsadmin -report

# Kiểm tra danh sách Worker đăng ký trên YARN (Cần hiển thị 3 NodeManager ở trạng thái RUNNING)
yarn node -list
```

#### 5.6. Chạy kịch bản kiểm thử MapReduce WordCount mẫu
```bash
bash ~/test_bigdata/test.sh
```
Kịch bản sẽ tự động tạo dữ liệu mẫu, đẩy lên HDFS tại `/user/hadoopanh24162006/input1`, kích hoạt MapReduce job trên YARN và in kết quả đếm từ hoàn tất.

---

### Bước 6: Triển khai Kho Dữ Liệu Apache Hive & DBeaver

#### 6.1. Khởi tạo cấu trúc thư mục và nạp dữ liệu E-commerce lên HDFS
Chạy các lệnh sau trên terminal của **`anh24162006-master`**:

```bash
# 1. Tạo các thư mục Staging chuyên biệt cho 4 bảng trên HDFS
hdfs dfs -mkdir -p /ecommerce_dw/staging/customers
hdfs dfs -mkdir -p /ecommerce_dw/staging/products
hdfs dfs -mkdir -p /ecommerce_dw/staging/order_items
hdfs dfs -mkdir -p /ecommerce_dw/staging/raw_orders

# 2. Đẩy 4 tệp dữ liệu CSV từ thư mục test_bigdata/data/ lên HDFS
hdfs dfs -put -f ~/test_bigdata/data/customer_master.csv /ecommerce_dw/staging/customers/
hdfs dfs -put -f ~/test_bigdata/data/product_catalog.csv /ecommerce_dw/staging/products/
hdfs dfs -put -f ~/test_bigdata/data/order_items.csv /ecommerce_dw/staging/order_items/
hdfs dfs -put -f ~/test_bigdata/data/ecommerce_sales_customer_analytics_150k.csv /ecommerce_dw/staging/raw_orders/

# 3. Kiểm tra danh sách và kích thước dữ liệu trên HDFS
hdfs dfs -ls -h -R /ecommerce_dw/staging
```

#### 6.2. Khởi tạo Schematool Metastore (Chỉ thực hiện lần đầu tiên)
Nếu CSDL Derby Metastore chưa được khởi tạo:
```bash
schematool -initSchema -dbType derby
```

#### 6.3. Khởi động dịch vụ nền `HiveServer2` (Port `10000`)
```bash
# 1. Đảm bảo HDFS không bị kẹt ở chế độ Safe Mode
hdfs dfsadmin -safemode leave

# 2. Dọn dẹp tiến trình Hive cũ (nếu có) và xóa file khóa .lck của Derby
kill -9 $(jps -ml | grep -E "HiveMetaStore|BeeLine|HiveServer2" | awk '{print $1}') 2>/dev/null || true
rm -f /home/hadoopanh24162006/hive/metastore_db/*.lck

# 3. Khởi chạy HiveServer2 chạy ngầm
nohup hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &

# 4. Theo dõi log khởi động (khoảng 15-20s, xuất hiện dòng "Hive Session ID = ..." là sẵn sàng, nhấn Ctrl+C để thoát log)
tail -f /tmp/hiveserver2.log
```

#### 6.4. Kết nối Hive qua công cụ dòng lệnh Beeline
Luôn kết nối qua giao thức Thrift mạng của HiveServer2 (tránh mở kết nối embedded nội bộ gây khóa CSDL Derby):

```bash
beeline -u jdbc:hive2://anh24162006-master:10000/default -n hadoopanh24162006
```

#### 6.5. Kết nối trực quan bằng DBeaver (Từ máy Host)
1. Tải file driver từ container ra máy Host (nếu chưa có):
   ```bash
   docker cp anh24162006-master:/home/hadoopanh24162006/hive/jdbc/hive-jdbc-4.1.0-standalone.jar .
   ```
2. Mở **DBeaver** → **Database** → **New Database Connection** → Chọn **Apache Hive**.
3. Điền thông tin kết nối:
   - **Host:** `anh24162006-master` *(đã ánh xạ trong file `hosts`)*
   - **Port:** `10000`
   - **Database:** `ecommerce_dw` *(hoặc `default`)*
   - **Username:** `hadoopanh24162006`
   - **Password:** *(Để trống)*
4. Nhấn **Edit Driver Settings** → Tab **Libraries** → Xóa các driver mặc định bị xung đột và nhấn **Add File** để chọn file `hive-jdbc-4.1.0-standalone.jar`.
5. Nhấn **Test Connection** để xác nhận thành công.

#### 6.6. Thực thi các kịch bản HiveQL phân tích kinh doanh
Trong dấu nhắc lệnh **Beeline** (lưu ý: lệnh `!run` trong Beeline **không có dấu `;`** ở cuối dòng) hoặc chạy trực tiếp trên cửa sổ SQL của **DBeaver**:

```sql
-- 1. Khởi tạo Cơ sở dữ liệu và 4 bảng Staging ánh xạ vào dữ liệu HDFS
!run /home/hadoopanh24162006/test_bigdata/hive/01_master_init_table.sql

-- 2. Kiểm tra danh sách bảng và truy vấn thử dữ liệu
USE ecommerce_dw;
SHOW TABLES;
SELECT * FROM staging_customers LIMIT 5;
SELECT * FROM staging_products LIMIT 5;

-- 3. Chạy các truy vấn phân tích tổng hợp (mỗi truy vấn sẽ sinh ra MapReduce job trên cụm YARN):
-- Kịch bản 1: Thống kê số lượng đơn hàng và tổng doanh thu theo từng thành phố (customer_city)
!run /home/hadoopanh24162006/test_bigdata/hive/02_slave1_hoc_sales_by_city.sql

-- Kịch bản 2: Thống kê số lượng giao dịch và tổng doanh thu theo phương thức thanh toán (payment_method)
!run /home/hadoopanh24162006/test_bigdata/hive/03_slave2_nguyen_sales_by_payment.sql

-- Kịch bản 3: Thống kê doanh thu theo từng danh mục sản phẩm qua phép kết bảng (JOIN staging_products)
!run /home/hadoopanh24162006/test_bigdata/hive/04_slave3_anh_sales_by_product.sql
```

---

### Bước 7: Thực thi Tính toán Phân tán với Apache Spark & PySpark

#### 7.1. Khởi động cụm Spark Standalone (Tùy chọn)
Nếu muốn chạy trực tiếp qua Spark Standalone thay vì YARN:
```bash
$SPARK_HOME/sbin/start-all.sh
```
*Truy cập giao diện Spark Master Web UI tại [http://anh24162006-master:8080](http://anh24162006-master:8080) để thấy cả 3 Worker đăng ký thành công.*

#### 7.2. Thực thi 5 bài toán PySpark MapReduce
Toàn bộ các tác vụ có thể submit lên hệ thống quản lý tài nguyên YARN Cluster (`--master yarn`) hoặc Spark Master (`--master spark://anh24162006-master:7077`).

```bash
# =========================================================================
# Bài 1: Đếm tần suất từ phân tán (Word Count - Top 20 từ xuất hiện nhiều nhất)
# Thuật toán: flatMap tách từ -> map (word, 1) -> reduceByKey cộng dồn -> takeOrdered
# =========================================================================
spark-submit --master yarn ~/test_bigdata/spark/01_master_word_count.py

# =========================================================================
# Bài 2: Tính độ dài từ trung bình trên dữ liệu đánh giá khách hàng (Word Mean)
# Thuật toán: map (length, 1) -> fold/reduce cộng tổng ký tự và tổng số từ -> chia trung bình
# =========================================================================
spark-submit --master yarn ~/test_bigdata/spark/02_slave1_hoc_word_mean.py

# =========================================================================
# Bài 3: Xác định trung vị độ dài từ phân tán (Word Median)
# Thuật toán: MapReduce thống kê bảng tần suất độ dài -> sắp xếp key -> quét tần suất tích lũy
# =========================================================================
spark-submit --master yarn ~/test_bigdata/spark/03_slave2_nguyen_word_median.py

# =========================================================================
# Bài 4: Tính phương sai và độ lệch chuẩn độ dài từ (Word Standard Deviation)
# Thuật toán: Biến đổi song song tính E[X] và E[X^2] -> Var(X) = E[X^2] - (E[X])^2 -> StdDev = sqrt(Var)
# =========================================================================
spark-submit --master yarn ~/test_bigdata/spark/04_slave3_anh_word_stddev.py

# =========================================================================
# Bài 5 (Mở rộng): Phân tích doanh số sản phẩm qua Spark SQL DataFrame
# Thuật toán: Đọc CSV HDFS -> tạo TempView -> Spark SQL INNER JOIN + GROUP BY + SUM + ORDER BY
# =========================================================================
spark-submit --master yarn ~/test_bigdata/spark/05_slave3_anh_sales_by_product.py
```

---

### Bước 8: Danh Sách Giao Diện Quản Trị Web UI

Truy cập trực tiếp bằng địa chỉ IP Tailscale của các container trên trình duyệt (hoặc qua `localhost:<cổng>` trên máy tính chứa container tương ứng nếu có map port):

| Dịch vụ | Địa chỉ URL truy cập Web UI (Theo IP Tailscale) | Chức năng giám sát |
| :--- | :--- | :--- |
| **HDFS NameNode** | [http://100.93.138.6:9870](http://100.93.138.6:9870) | Quản lý cây thư mục HDFS, dung lượng lưu trữ, trạng thái các DataNode |
| **YARN ResourceManager** | [http://100.93.138.6:8088](http://100.93.138.6:8088) | Theo dõi hàng đợi YARN, phân bổ CPU/RAM, trạng thái các Container và Job |
| **MapReduce JobHistory** | [http://100.93.138.6:19888](http://100.93.138.6:19888) | Xem lại lịch sử các tác vụ MapReduce và log thực thi chi tiết |
| **HiveServer2 Web UI** | [http://100.93.138.6:10002](http://100.93.138.6:10002) | Giám sát các phiên kết nối JDBC, câu lệnh HiveQL đang thực thi |
| **Spark Master UI** | [http://100.93.138.6:8080](http://100.93.138.6:8080) | Quản lý cụm Spark Standalone, danh sách Worker, tài nguyên lõi/bộ nhớ |
| **Spark Application UI** | [http://100.93.138.6:4040](http://100.93.138.6:4040) | Hiển thị đồ thị DAG thực thi khi có job PySpark đang chạy |
| **HDFS DataNode Worker 1**| [http://100.83.192.69:9864](http://100.83.192.69:9864) | Thông tin block và dung lượng đĩa của DataNode 1 (`hoc-slave1`) |
| **YARN NodeManager Worker 1**| [http://100.83.192.69:8042](http://100.83.192.69:8042) | Giám sát tài nguyên và log cục bộ của NodeManager 1 (`hoc-slave1`) |
| **HDFS DataNode Worker 2**| [http://100.99.120.86:9864](http://100.99.120.86:9864) | Thông tin block và dung lượng đĩa của DataNode 2 (`nguyen-slave2`) |
| **YARN NodeManager Worker 2**| [http://100.99.120.86:8042](http://100.99.120.86:8042) | Giám sát tài nguyên và log cục bộ của NodeManager 2 (`nguyen-slave2`) |
| **HDFS DataNode Worker 3**| [http://100.111.208.15:9864](http://100.111.208.15:9864) | Thông tin block và dung lượng đĩa của DataNode 3 (`anh24133004-slave3`) |
| **YARN NodeManager Worker 3**| [http://100.111.208.15:8042](http://100.111.208.15:8042) | Giám sát tài nguyên và log cục bộ của NodeManager 3 (`anh24133004-slave3`) |

---

### Bước 9: Quy trình Dừng Hệ Thống An Toàn

Dừng các tiến trình đúng trình tự giúp tránh làm hỏng tệp tin trạng thái HDFS (FsImage/EditLog) và khóa cơ sở dữ liệu Derby Metastore. Chạy trên **`anh24162006-master`**:

```bash
# 1. Dừng HiveServer2
kill -9 $(jps -ml | grep "HiveServer2" | awk '{print $1}') 2>/dev/null || true

# 2. Dừng dịch vụ Spark Standalone (nếu có khởi động)
$SPARK_HOME/sbin/stop-all.sh

# 3. Dừng JobHistoryServer, YARN và HDFS
mapred --daemon stop historyserver
stop-all.sh
```

Nếu muốn tắt toàn bộ các container trên máy thật:
```bash
docker compose down
```

---

## 3. Lưu Ý Chuyên Sâu: Docker + Tailscale Sidecar & Các Vấn Đề Đặc Thù

### 3.1. Cơ chế hoạt động của Tailscale Sidecar (`network_mode: service:tailscale`)
Trong mô hình kiến trúc này:
1. **Container Tailscale** là thực thể trực tiếp tạo kết nối VPN WireGuard ra Internet, sở hữu card mạng ảo `tailscale0` và được Tailscale gán một địa chỉ IP riêng trong dải `100.x.y.z`.
2. **Container `hadoop-node`** sử dụng cấu hình `network_mode: "service:tailscale"`. Nghĩa là container Hadoop không có card mạng riêng biệt, mà **dùng chung hoàn toàn ngăn xếp mạng (Network Stack) và Network Namespace** với container Tailscale.
3. Khi container Hadoop lắng nghe trên cổng `9000` (HDFS RPC) hay `8088` (YARN Web UI), cổng đó thực chất đang mở trực tiếp trên địa chỉ IP Tailscale của máy đó. Nhờ vậy, máy Master và các Worker có thể liên lạc với nhau qua mạng VPN với hiệu năng cao mà không phải thực hiện các lệnh ánh xạ cổng (port forwarding) phức tạp.
4. Trong kịch bản `entrypoint.sh`, một vòng lặp kiểm tra sẽ chờ card `tailscale0` xuất hiện, sau đó tự động lấy địa chỉ IP Tailscale và ghi vào `/etc/hosts`:
   ```bash
   TS_IP=$(ifconfig tailscale0 2>/dev/null | grep 'inet ' | awk '{print $2}' | sed 's/addr://')
   echo "$TS_IP $(hostname)" >> /etc/hosts
   ```
   Thao tác này đảm bảo các tiến trình Hadoop/Java khi phân giải chính Hostname của nó sẽ luôn trỏ về đúng IP Tailscale thay vì loopback `127.0.0.1` hay IP mạng cầu nội bộ Docker.

---

### 3.2. Vấn đề Đóng gói Image, Phân phối & Tính Khả Di (Portability)

> [!IMPORTANT]
> **Đặc biệt lưu ý khi nộp bài cho Giảng viên hoặc chuyển giao dự án cho người khác!**

#### a. Docker Image KHÔNG chứa trạng thái Tailscale và KHÔNG chứa dữ liệu HDFS
Nhiều người lầm tưởng rằng chạy `docker commit` hoặc `docker save -o image.tar` sẽ đóng gói toàn bộ trạng thái cụm đang chạy để gửi cho người khác là xong. Thực tế:
- **Trạng thái Tailscale nằm trong Docker Volume `ts_state`:** Khóa định danh thiết bị, IP Tailscale `100.x.y.z`, và phiên đăng nhập VPN được lưu tại thư mục `/var/lib/tailscale` được mount vào named volume `ts_state`. Docker image chỉ chứa mã nhị phân cài đặt, hoàn toàn không chứa dữ liệu của Volume này.
- **Dữ liệu HDFS nằm trong Docker Volume `hadoop_data`:** Toàn bộ dữ liệu các file nạp lên HDFS (FsImage, EditLogs, Block dữ liệu thực tế) nằm trong named volume `hadoop_data` mount vào `/home/hadoopanh24162006/hadoop/hdfs`. Lệnh `docker commit` hay `docker save` **bỏ qua hoàn toàn dữ liệu của Docker Volume**.

#### b. IP Tailscale là IP cấp phát động từ Cloud, không cố định theo Image
- Địa chỉ IP `100.x.y.z` do máy chủ điều phối của Tailscale (Coordination Server) quản lý và cấp phát dựa trên tài khoản Tailnet.
- Khi một người khác (ví dụ: Giảng viên) import image của nhóm vào máy của họ, container Tailscale sẽ kết nối ra Tailscale Cloud bằng tài khoản của họ hoặc yêu cầu Auth Key mới, từ đó được cấp một dải IP hoàn toàn mới, **không thể giữ nguyên các IP cũ như trên máy của nhóm**.

#### c. Hiện tượng Xung đột Định danh Node (Node Identity Conflict)
- Nếu cố tình copy thư mục volume `ts_state` cũ sang máy khác và cùng khởi động đồng thời, Tailscale Cloud sẽ phát hiện **2 máy tính khác nhau sử dụng cùng 1 Node ID**. Hệ thống sẽ liên tục ngắt kết nối (kick) một trong hai máy ra khỏi mạng, dẫn đến tình trạng mạng bị chập chờn, mất gói (packet flap).

#### d. Hướng dẫn Người nhận / Giảng viên triển khai lại dự án đúng chuẩn:
Để một người ngoài có thể chạy thành công dự án trên máy của họ:
1. **Bước 1:** `git clone` mã nguồn dự án về máy.
2. **Bước 2:** Đăng ký 1 tài khoản Tailscale riêng và tạo một **Reusable Auth Key**.
3. **Bước 3:** Điền khóa đó vào biến `TS_AUTHKEY` trong file `.env`.
4. **Bước 4:** Chạy `docker compose up -d --build` để Docker tự động dịch image sạch từ Dockerfile và kết nối vào Tailnet của họ.
5. **Bước 5:** Tra cứu IP Tailscale mới cấp, cập nhật vào file `hosts` và phân phối SSH key theo [Bước 4](#bước-4-thiết-lập--phân-phối-khóa-ssh-không-mật-khẩu).
6. **Bước 6:** Khởi động cụm và nạp lại dữ liệu sạch lên HDFS bằng các lệnh `hdfs dfs -put` có sẵn trong [Bước 6.1](#61-khởi-tạo-cấu-trúc-thư-mục-và-nạp-dữ-liệu-e-commerce-lên-hdfs).

---

### 3.3. Tối ưu kết nối P2P Direct WireGuard vs Trạm trung chuyển Relay (DERP)
- Mạng Tailscale ưu tiên thiết lập kết nối ngang hàng trực tiếp (Direct Connection) giữa các máy qua giao thức UDP WireGuard.
- Tuy nhiên, nếu tường lửa của hệ điều hành chặn các gói tin UDP đến, Tailscale sẽ buộc phải chuyển sang chế độ dự phòng là đi vòng qua các máy chủ chuyển tiếp công cộng (DERP Relay, ví dụ trạm Singapore `relay "sin"`).
- Khi rơi vào trạng thái Relay, băng thông truyền dữ liệu giữa các node sẽ bị bóp nghẽn nghiêm trọng (chỉ đạt 100KB/s - 1MB/s), dẫn đến việc đọc ghi HDFS block hoặc pha Shuffle của MapReduce chạy cực kỳ chậm hoặc bị timeout.
- **Cách kiểm tra:** Chạy lệnh `tailscale status` trên máy Host. Nếu cạnh tên máy khác hiển thị `direct`, kết nối đã tối ưu. Nếu hiển thị `relay "sin"`, hãy chạy lệnh mở cổng UDP `41641` và `41642` ở [Bước 1.2](#12-mở-cổng-firewall-trên-máy-host-để-kích-hoạt-kết-nối-trực-tiếp-direct-p2p).

---

### 3.4. Tại sao phải thêm SSH Key localhost trên máy Master
Khi vận hành Hadoop, nhiều người thắc mắc: *"Tại sao đứng trên máy Master lại phải copy SSH key vào chính máy Master?"*.
- **Nguyên lý của Hadoop Scripts:** Các kịch bản quản trị cụm của Apache Hadoop như `start-all.sh`, `start-dfs.sh`, hay `start-yarn.sh` được thiết kế theo kiến trúc quản trị tự động từ xa.
- Khi bạn chạy `start-dfs.sh` trên Master, kịch bản này **không gọi trực tiếp lệnh khởi động tiến trình nội bộ**, mà nó đọc tệp cấu hình và thực thi lệnh SSH:
  ```bash
  ssh localhost (hoặc ssh anh24162006-master) "hdfs --daemon start namenode"
  ```
- Nếu máy Master chưa được cấu hình khóa công khai trong chính tệp `~/.ssh/authorized_keys` của nó, phiên SSH nội bộ này sẽ bị từ chối kết nối hoặc bị treo vĩnh viễn để chờ người dùng nhập mật khẩu trên terminal.

---

### 3.5. Xung đột tài nguyên Java 17 và Virtual Memory trên YARN (`vmem-check-enabled`)
- Môi trường chạy dự án sử dụng Java 17 OpenJDK trên Ubuntu 24.04 LTS.
- Trình quản lý bộ nhớ của Java 17 thường yêu cầu cấp phát trước một không gian bộ nhớ ảo (Virtual Memory - vmem) rất lớn cho các luồng xử lý (threads) và bộ thu gom rác (Garbage Collector), dù dung lượng bộ nhớ vật lý thực tế (Physical RAM - rss) sử dụng là rất nhỏ.
- Theo mặc định, YARN NodeManager kích hoạt tính năng giám sát `yarn.nodemanager.vmem-check-enabled=true` với tỷ lệ giới hạn ảo/thực là `2.1`. Khi một tác vụ PySpark hoặc MapReduce vừa khởi động, dung lượng vmem của JVM Java 17 lập tức vượt ngưỡng này, khiến NodeManager cưỡng chế tiêu diệt (kill) container với thông báo lỗi:
  ```text
  Container [...] is running beyond virtual memory limits. Current usage: ... GB of ... GB physical memory used; ... GB of ... GB virtual memory used. Killing container.
  ```
- **Giải pháp tối ưu:** Dự án đã tắt hoàn toàn cơ chế kiểm tra bộ nhớ ảo này trong `config/yarn-site.xml`:
  ```xml
  <property>
      <name>yarn.nodemanager.vmem-check-enabled</name>
      <value>false</value>
  </property>
  ```

---

### 3.6. Cơ chế Khóa Đơn tiến trình của Apache Derby Metastore
- Apache Hive 4.1.0 sử dụng cơ sở dữ liệu nhúng **Apache Derby Embedded** để lưu trữ thông tin lược đồ bảng (Metastore) tại `/home/hadoopanh24162006/hive/metastore_db`.
- Đặc tính kỹ thuật của Derby Embedded là **chỉ cho phép DUY NHẤT một tiến trình Java giữ quyền ghi vào thư mục cơ sở dữ liệu tại một thời điểm**.
- **Nguyên nhân lỗi thường gặp:** Nếu bạn vừa khởi động dịch vụ nền `HiveServer2`, sau đó trên terminal lại gõ trực tiếp lệnh `hive` CLI hoặc dùng Beeline kết nối không qua cổng mạng (`!connect jdbc:hive2://`), một tiến trình Java thứ hai sẽ cố gắng khởi động lại Derby, dẫn đến xung đột và văng lỗi nghiêm trọng:
  ```text
  ERROR: Another instance of Derby may have already booted the database /home/hadoopanh24162006/hive/metastore_db
  ```
- **Quy tắc vận hành chuẩn:**
  1. Chỉ khởi động duy nhất tiến trình nền **`HiveServer2`** lắng nghe trên cổng `10000`.
  2. Mọi kết nối từ dòng lệnh Beeline hay giao diện đồ họa DBeaver đều phải trỏ qua giao thức mạng: `jdbc:hive2://anh24162006-master:10000/<database>`.
  3. Nếu tiến trình bị tắt đột ngột và để lại file khóa, hãy dọn dẹp bằng lệnh:
     ```bash
     kill -9 $(jps -ml | grep -E "HiveMetaStore|BeeLine|HiveServer2" | awk '{print $1}') 2>/dev/null || true
     rm -f /home/hadoopanh24162006/hive/metastore_db/*.lck
     ```

---

### 3.7. Hiện tượng HDFS Safe Mode (Read-Only)
- Khi NameNode vừa khởi động, nó tự động kích hoạt chế độ an toàn (Safe Mode). Trong giai đoạn này, NameNode chỉ nạp metadata từ file FsImage vào RAM và chờ đợi báo cáo danh sách block từ các DataNode gửi về. Lúc này hệ thống tệp HDFS ở trạng thái **chỉ đọc (Read-Only)**, không cho phép tạo mới thư mục hoặc ghi dữ liệu.
- Nếu bạn khởi động `HiveServer2` ngay khi NameNode còn ở Safe Mode, HiveServer2 sẽ không thể tạo thư mục tạm `/tmp/hive` trên HDFS và tự động tắt (dẫn đến lỗi `Connection refused` khi Beeline kết nối vào cổng 10000).
- **Cách xử lý:** Luôn kiểm tra và ép NameNode thoát khỏi Safe Mode trước khi bật các dịch vụ tầng trên:
  ```bash
  hdfs dfsadmin -safemode leave
  ```

---

### 3.8. Tối ưu nạp thư viện Spark JARs trên YARN qua VPN
- Theo cơ chế mặc định của Spark on YARN, mỗi khi submit một ứng dụng, Spark Driver sẽ đóng gói toàn bộ thư viện Spark JARs (~500 MB) và đẩy lên thư mục tạm của HDFS, sau đó các Worker Node sẽ tải lại 500 MB này qua mạng VPN Tailscale về máy của chúng.
- Trong điều kiện kết nối mạng Internet gia đình, việc tải đi tải lại hàng trăm MB thư viện qua VPN khiến mỗi lần chạy `spark-submit` bị treo chờ từ 3 - 5 phút.
- **Giải pháp tối ưu của dự án:** Vì tất cả các node (Master và Worker) đều được dựng từ chung một Docker Image (`bigdata-ecosystem:latest`), thư viện Spark đã có sẵn trên ổ cứng cục bộ của từng node tại `/home/hadoopanh24162006/spark/jars/`. Dự án đã cấu hình trong `config/spark-defaults.conf`:
  ```properties
  spark.yarn.jars   local:///home/hadoopanh24162006/spark/jars/*
  ```
- Tiền tố `local:///` (với 3 dấu gạch chéo) chỉ thị cho YARN Container trên mỗi Worker sử dụng trực tiếp các file JAR có sẵn trên ổ cứng nội bộ của máy nó, loại bỏ hoàn toàn việc truyền tải 500 MB qua VPN, giúp các job PySpark khởi động chỉ trong 3 - 5 giây.

---

## 4. Bảng Tra Cứu Xử Lý Sự Cố Nhanh (Troubleshooting Quick Reference)

| Triệu chứng lỗi | Nguyên nhân gốc rễ | Câu lệnh / Thao tác khắc phục |
| :--- | :--- | :--- |
| `Connection refused: anh24162006-master:10000` khi kết nối Beeline / DBeaver | NameNode đang ở chế độ `Safe Mode` khiến `HiveServer2` không ghi được vào `/tmp/hive` và tự tắt; hoặc HiveServer2 chưa khởi động xong. | 1. `hdfs dfsadmin -safemode leave`<br>2. `nohup hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &`<br>3. `tail -f /tmp/hiveserver2.log` (đợi xuất hiện Hive Session ID). |
| `Another instance of Derby may have already booted the database` | Có tiến trình Hive cũ bị treo hoặc người dùng mở trực tiếp lệnh `hive` CLI làm tranh chấp file khóa của CSDL Derby Metastore. | 1. `kill -9 $(jps -ml \| grep -E "HiveMetaStore\|BeeLine\|HiveServer2" \| awk '{print $1}') 2>/dev/null`<br>2. `rm -f /home/hadoopanh24162006/hive/metastore_db/*.lck`<br>3. Khởi động lại HiveServer2. |
| `!run /path/to/script.sql; (No such file or directory)` trong Beeline | Lệnh đặc biệt `!run` trong Beeline đọc trực tiếp đường dẫn file trong container và **không được chứa dấu chấm phẩy `;`** ở cuối. | Gõ lại lệnh bỏ dấu `;`: `!run /home/hadoopanh24162006/test_bigdata/hive/01_master_init_table.sql` |
| `\r: command not found` khi khởi động container Docker trên Windows | Tệp script Bash (`entrypoint.sh` hoặc cấu hình) bị chuyển đổi ký tự xuống dòng từ định dạng UNIX (`LF`) sang Windows (`CRLF`). | Trong `docker-compose.yml`, dự án đã sử dụng cú pháp lọc bỏ tự động: `entrypoint: ["/bin/bash", "-c", "tr -d '\\r' < /usr/local/bin/entrypoint.sh \| /bin/bash"]` |
| Job MapReduce / Spark chạy rất chậm hoặc bị timeout ở bước phân bổ Container | Máy Worker đang kết nối với Master qua trạm chuyển tiếp (Relay DERP) thay vì kết nối trực tiếp P2P WireGuard. | Chạy lệnh mở cổng UDP `41641` và `41642` trên Firewall của máy Host Windows/Linux theo [Bước 1.2](#12-mở-cổng-firewall-trên-máy-host-để-kích-hoạt-kết-nối-trực-tiếp-direct-p2p). |
| `Container [...] is running beyond virtual memory limits` trên YARN | Java 17 cấp phát Virtual Memory (vmem) lớn cho các luồng xử lý, vi phạm ngưỡng kiểm tra mặc định của YARN NodeManager. | Kiểm tra tệp `config/yarn-site.xml` và đảm bảo thuộc tính `yarn.nodemanager.vmem-check-enabled` đã được đặt thành `false`. |
| DataNode trên máy Worker tự tắt sau vài phút hoạt động | Thư mục lưu trữ HDFS dữ liệu bị sai lệch quyền người dùng hoặc ID Cluster không đồng nhất sau khi NameNode format lại. | Xóa sạch thư mục dữ liệu cũ trên máy Worker và khởi động lại: `rm -rf ~/hadoop/hdfs/datanode/* && ~/hadoop/bin/hdfs --daemon start datanode` |
| Trình duyệt không mở được các đường dẫn Web UI qua Hostname | Máy Host chưa được phân giải các tên miền máy ảo vào địa chỉ IP Tailscale tương ứng. | Thêm 4 dòng ánh xạ IP và Hostname vào tệp `hosts` của hệ điều hành máy thật theo hướng dẫn tại [Bước 1.3](#13-ánh-xạ-hostname-vào-file-hosts-của-máy-thật-host-os). |
