### Write-up Pengerjaan Apache Kafka

Nama: I Dewa Made Satya Raditya
NRP: 5027231051
Kelas: B
 
#### Latar Belakang Masalah
Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor:

  - Sensor Suhu

  - Sensor Kelembaban
Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.


1. Buat Topik Kafka
Buat dua topik di Apache Kafka:
  - sensor-suhu-gudang:
    `./bin/kafka-topics.sh --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092`
  -sensor-kelembaban-gudang:
    `./bin/kafka-topics.sh --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092`
    ![image](https://github.com/user-attachments/assets/5d39ce42-b41c-4231-9af6-5c81cadc7f1a)

2. Simulasikan Data Sensor (Producer Kafka)
Buat dua Kafka producer terpisah:

  a. Producer Suhu
    ![image](https://github.com/user-attachments/assets/f4798062-bb0b-4812-b4d9-5803950cfe8e)
    mengirimkan data setiap detik dengan Format {"gudang_id": "G1", "suhu": x}
    hasil:
    ![image](https://github.com/user-attachments/assets/a89ad5bd-e280-48ca-ba31-aec376de1e2c)

  
  b. Producer Kelembaban
    ![image](https://github.com/user-attachments/assets/e27582fe-80e7-44d7-ae14-53201c4ec676)
    mengirimkan data setiap detik dengan Format {"gudang_id": "G1", "kelembaban": 75}
    hasil:
    ![image](https://github.com/user-attachments/assets/4bfa344b-fa9b-4657-9268-4f83156b6473)


3. Konsumsi dan Olah Data dengan PySpark
    a. Buat PySpark Consumer:
       Konsumsi data dari kedua topik Kafka.
       Kode:

          from pyspark.sql import SparkSession
          from pyspark.sql.functions import from_json, col, expr, window
          from pyspark.sql.types import StructType, StringType, IntegerType
          from pyspark.sql.functions import col
          # Inisialisasi Spark
          spark = SparkSession.builder \
          .appName("Gudang Monitoring") \
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
   
 b. Lakukan Filtering:
       Suhu > 80°C → tampilkan sebagai peringatan suhu tinggi
       Kelembaban > 70% → tampilkan sebagai peringatan kelembaban tinggi
       
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
      
  3. Buat Peringatan gabungan
     output:
     ![image](https://github.com/user-attachments/assets/96f16ed0-1b3d-4688-a32b-96686b42617f)
     ![image](https://github.com/user-attachments/assets/787cda1f-2efa-41c6-859e-d434d9b2123a)

     Penjelasan:
      - NoClassDefFoundError berarti Java runtime tidak dapat menemukan definisi kelas yang diperlukan saat menjalankan program. Di sini kelas yang hilang adalah scala.<:< (yang di-encode jadi $less$colon$less), sebuah kelas utilitas dari Scala yang sering digunakan untuk tipe parameterisasi (type constraints).
      - Kelas ini merupakan bagian dari library Scala standard yang digunakan Spark secara internal.error ini terjadi karena ketidakcocokan versi Scala yang digunakan oleh Spark dan Kafka connector, atau karena dependency Scala tidak ditemukan di classpath saat runtime.
      - Saya sudah berusaha melakukan perbaikan dengan mengganti ke versi scala yang cocok, menentukan classpath di element variable nya, namun hasilnya tetap sama, kemungkinan ada konfigurasi yang belum di ubah.
