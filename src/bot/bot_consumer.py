import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

def ufc_streaming():
    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("ufc_bot") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-dev:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
            "org.postgresql:postgresql:42.2.18,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ) \
        .getOrCreate()

    # Kafka settings
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic     = os.getenv("KAFKA_TOPIC",           "ufc")

    # Read raw JSON strings from Kafka
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(F.col("value").cast(T.StringType()))

    # Define JSON schema for incoming messages
    message_schema = T.StructType([
        T.StructField("user_id",   T.StringType(), nullable=False),
        T.StructField("username",      T.StringType(), nullable=True),
        T.StructField("action",    T.StringType(), nullable=True),
        T.StructField("timestamp", T.StringType(), nullable=True),
        T.StructField("subscribe", T.IntegerType(), nullable=True),
    ])

    # Parse JSON into individual columns
    parsed_df = raw_df \
        .select(F.from_json(F.col("value"), message_schema).alias("data")) \
        .select("data.*")

    # Per-batch write: first print, then write to Postgres
    def write_to_postgres(batch_df, batch_id):
        # Print every incoming event in this micro-batch
        batch_df.show(truncate=False)

        # Then append to PostgreSQL
        batch_df.write \
            .mode("append") \
            .format("jdbc") \
            .option(
                "url",
                os.getenv(
                    "POSTGRES_URL",
                    "jdbc:postgresql://postgres_db:5432/ufc_db"
                )
            ) \
            .option("driver",   "org.postgresql.Driver") \
            .option("dbtable",  "public.user_actions") \
            .option("user",     os.getenv("POSTGRES_USER",     "postgres")) \
            .option("password", os.getenv("POSTGRES_PASSWORD", "postgres")) \
            .save()

    # Start the streaming query
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .option(
            "checkpointLocation",
            "s3a://ufc/spark/checkpoints/ufc_bot"
        ) \
        .start()

    query.awaitTermination()
    spark.stop()

if __name__ == "__main__":
    ufc_streaming()
