from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import col
# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("Gudang Monitoring") \
    .config("spark.jars", "C:/KAFKA/kafka_2.13-3.7.0/libs/libsspark-sql-kafka-0-10_2.12-3.5.0.jar,C:/KAFKA/kafka_2.13-3.7.0/libs/kafka-clients-3.5.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema untuk suhu
schema_suhu = StructType() \
    .add("id_gudang", StringType()) \
    .add("suhu", IntegerType())

# Schema untuk kelembaban
schema_kelembaban = StructType() \
    .add("id_gudang", StringType()) \
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

# Gabungkan stream berdasarkan id_gudang dan window 10 detik

s = suhu_parsed.alias("s")
k = kelembaban_parsed.alias("k")

joined = s.join(
    k,
    expr("""
        s.id_gudang = k.id_gudang AND
        s.timestamp BETWEEN k.timestamp - interval 10 seconds AND k.timestamp + interval 10 seconds
    """),
    "inner"
).select(
    col("s.id_gudang").alias("id_gudang"),
    col("s.suhu"),
    col("k.kelembaban"),
    col("s.timestamp").alias("timestamp")
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

