# =====================================================================
# BÀI 1 (MASTER): WORD COUNT BẰNG PYSPARK MAPREDUCE
# Sinh viên thực hiện: Lê Đặng Hoàng Anh - 24162006 (anh24162006-master)
# =====================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("01_WordCount_Master_HoangAnh24162006")
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

    # Bước MAP: Tách từng dòng thành các từ và gán mỗi từ thành cặp (word, 1)
    words = reviews.rdd.flatMap(lambda row: (row["customer_review"] or "").split())
    word_pairs = words.map(lambda word: (word, 1))

    # Bước REDUCE: Cộng dồn số lần xuất hiện của từng từ có cùng key
    word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

    # Sắp xếp và lấy 20 từ xuất hiện nhiều nhất (Top 20)
    top_20_words = word_counts.takeOrdered(20, key=lambda x: -x[1])

    print("===== KẾT QUẢ WORD COUNT (TOP 20 TỪ XUẤT HIỆN NHIỀU NHẤT) =====")
    for word, count in top_20_words:
        print(f"{word:20s}: {count}")

    print("===== TỔNG SỐ TỪ TRONG VĂN BẢN =====")
    print("Total words:", words.count())

finally: 
    spark.stop()

