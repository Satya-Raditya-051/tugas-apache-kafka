from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("Gudang Monitoring") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema untuk suhu
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

# Schema untuk kelembaban
schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Baca stream suhu
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", expr("current_timestamp()"))

# Baca stream kelembaban
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_parsed = kelembaban_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembaban).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", expr("current_timestamp()"))

# Filtering Suhu Tinggi
peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)
peringatan_kelembaban = kelembaban_parsed.filter(col("kelembaban") > 70)

# Gabungkan stream berdasarkan gudang_id dan window 10 detik
joined = suhu_parsed.join(
    kelembaban_parsed,
    expr("""
        gudang_id = gudang_id AND
        suhu_parsed.timestamp BETWEEN kelembaban_parsed.timestamp - interval 10 seconds AND kelembaban_parsed.timestamp + interval 10 seconds
    """),
    "inner"
).select(
    suhu_parsed.gudang_id.alias("gudang_id"),
    "suhu", "kelembaban",
    suhu_parsed.timestamp.alias("timestamp")
)

# Tambahkan status
from pyspark.sql.functions import when

hasil = joined.withColumn("status",
    when((col("suhu") > 80) & (col("kelembaban") > 70), "Bahaya tinggi! Barang berisiko rusak")
    .when((col("suhu") > 80), "Suhu tinggi, kelembaban normal")
    .when((col("kelembaban") > 70), "Kelembaban tinggi, suhu aman")
    .otherwise("Aman")
)

# Tampilkan hasil
query = hasil.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

