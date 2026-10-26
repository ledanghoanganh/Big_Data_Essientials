# =====================================================================
# BÀI 3 (SLAVE 2): WORD MEDIAN BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Quách Lộc Nguyên - 21139037 (nguyen-slave2)
# Mục tiêu: Tính trung vị độ dài của các từ trong customer_review (Word Median)
# Hệ thống: Apache Spark 4.2.0 trên cụm Hadoop Cluster (Tailscale VPN)
# =====================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("03_WordMedian_CustomerReview_Slave2_Nguyen")
    .getOrCreate()
)

try:
    # Đọc CSV gốc trực tiếp từ HDFS; hàng đầu là header
    orders = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .csv("hdfs://anh24162006-master:9000/ecommerce_dw/staging/raw_orders")
    )

    reviews = orders.select("customer_review").where(
        col("customer_review").isNotNull()
        & (col("customer_review").cast("string") != "")
    )

    # 1. Bước MAP: Tách từ và ánh xạ mỗi từ thành cặp (độ_dài_từ, 1)
    words = reviews.rdd.flatMap(lambda row: (row["customer_review"] or "").split())
    length_pairs = words.map(lambda word: (len(word), 1))

    # 2. Bước REDUCE: Đếm tần suất xuất hiện của từng độ dài từ và sắp xếp theo độ dài tăng dần
    length_counts = length_pairs.reduceByKey(lambda a, b: a + b).sortByKey().collect()

    # 3. Tính toán vị trí trung vị (Median) từ bảng phân phối tần suất
    total_words = sum(count for _, count in length_counts)

    print("===== BÀI TOÁN MAPREDUCE: TÍNH TRUNG VỊ ĐỘ DÀI TỪ (WORD MEDIAN) =====")
    print(f"Sinh viên thực hiện: Quách Lộc Nguyên - 21139037 (nguyen-slave2)")
    print(f"Tổng số từ được xử lý: {total_words:,}")

    if total_words == 0:
        print("Không tìm thấy từ nào trong customer_review.")
    else:
        mid_pos_1 = (total_words - 1) // 2
        mid_pos_2 = total_words // 2

        running_count = 0
        val1, val2 = None, None

        print("\n===== BẢNG PHÂN PHỐI TẦN SUẤT ĐỘ DÀI TỪ (LENGTH -> COUNT) =====")
        print(f"{'Độ dài (ký tự)':<18} | {'Số lượng từ':<15} | {'Tỷ lệ (%)':<12} | {'Tích lũy'}")
        print("-" * 65)

        for length, count in length_counts:
            running_count += count
            pct = (count / total_words) * 100.0

            if val1 is None and running_count > mid_pos_1:
                val1 = length
            if val2 is None and running_count > mid_pos_2:
                val2 = length

            marker = " <--- [MEDIAN]" if (length in (val1, val2)) else ""
            print(f"Độ dài {length:2d} ký tự    | {count:12,d}    | {pct:6.2f}%      | {running_count:10,d}{marker}")

        print("-" * 65)
        word_median = (val1 + val2) / 2.0

        print(f"Vị trí trung vị cần tra cứu : index #{mid_pos_1:,} và index #{mid_pos_2:,}")
        print(f"Trung vị độ dài từ vựng     : {word_median:.1f} ký tự")
        print("=======================================================================")

finally:
    spark.stop()
