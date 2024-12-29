from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, sum, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def transform_listings_to_postgres():

    scala_version = '2.12'
    spark_version = '3.4.3'

    packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.1'
    ]

    spark = SparkSession.builder \
        .appName("KafkaRealEstateStreaming") \
        .config("spark.jars.packages", ",".join(packages)) \
        .getOrCreate()

    # Чтение данных из Kafka
    raw_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "portugal_listings") \
        .load()

    # Преобразуем данные из Kafka в строковый формат
    kafka_values_df = raw_stream_df.selectExpr("CAST(value AS STRING)")

    # Определение схемы данных для JSON
    json_schema = StructType([
        StructField("listing_id", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("area", FloatType(), True),
        StructField("property_type", StringType(), True),
        StructField("district", StringType(), True)
    ])

    processed_df = kafka_values_df.select(from_json(col("value"), json_schema).alias("data"))
    processed_df = processed_df.select("data.*")
    filtered_df = processed_df.filter((col("price") >= 50000) & (col("area") >= 20))
    windowed_df = filtered_df.withColumn("window", window(current_timestamp(), "30 seconds"))

    aggregated_df = windowed_df.groupBy("property_type", "district") \
        .agg(
            avg("price").alias("avg_price"),
            avg("area").alias("avg_area"),
            count("listing_id").alias("total_listings"),
            (avg("price") / avg("area")).alias("avg_price_per_m2")
        )

    from pyspark.sql import functions as F

    def write_to_postgres(batch_df, batch_id):
        # Конфигурация для подключения к PostgreSQL
        jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
        properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
        # Запись в базу данных
        batch_df.write.jdbc(url=jdbc_url, table="portugal_listings_aggregated", mode="append", properties=properties)

    # Использование foreachBatch для записи
    aggregated_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("complete") \
        .trigger(processingTime="30 seconds") \
        .start() \
        .awaitTermination()
        