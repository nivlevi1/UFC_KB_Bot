import time
from datetime import datetime

import s3fs
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
    BooleanType, DateType, TimestampType
)

# 1) Define your schemas

schema_events = StructType([
    StructField("event_name", StringType(), nullable=False),
    StructField("date", DateType(), nullable=True),              # will parse "March 11, 1994"
    StructField("location", StringType(), nullable=True),
    StructField("link", StringType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=True)   # ISO8601 UTC
])

schema_fights = StructType([
    StructField("event_name", StringType(), nullable=False),
    StructField("fight_id", IntegerType(), nullable=False),
    StructField("fight_result", StringType(), nullable=True),
    StructField("fighter_1", StringType(), nullable=True),
    StructField("fighter_2", StringType(), nullable=True),
    StructField("weight_class", StringType(), nullable=True),
    StructField("method", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("round", IntegerType(), nullable=True),
    StructField("time", StringType(), nullable=True),                # "3:45"
    StructField("championship_fight", IntegerType(), nullable=True),
    StructField("fight_of_the_night", IntegerType(), nullable=True),
    StructField("performance_of_the_night", IntegerType(), nullable=True),
    StructField("sub_of_the_night", IntegerType(), nullable=True),
    StructField("ko_of_the_night", IntegerType(), nullable=True),
    StructField("fight_stats_link", StringType(), nullable=True),
    StructField("last_run", TimestampType(), nullable=True)
])

schema_fight_stats = StructType([
    StructField("event", StringType(), nullable=False),
    StructField("fight_id", IntegerType(), nullable=False),
    StructField("fighter", StringType(), nullable=True),
    StructField("knockdowns", IntegerType(), nullable=True),
    StructField("significant_strikes", StringType(), nullable=True),
    StructField("significant_strike_accuracy_pct", StringType(), nullable=True),
    StructField("total_strikes", StringType(), nullable=True),
    StructField("takedowns", StringType(), nullable=True),
    StructField("takedown_accuracy_pct", StringType(), nullable=True),
    StructField("submission_attempts", IntegerType(), nullable=True),
    StructField("reversals", IntegerType(), nullable=True),
    StructField("control_time", StringType(), nullable=True),
    StructField("significant_head_strikes", StringType(), nullable=True),
    StructField("significant_body_strikes", StringType(), nullable=True),
    StructField("significant_leg_strikes", StringType(), nullable=True),
    StructField("distance_strikes", StringType(), nullable=True),
    StructField("clinch_strikes", StringType(), nullable=True),
    StructField("ground_strikes", StringType(), nullable=True),
    StructField("last_run", TimestampType(), nullable=True)
])

schema_round_stats = StructType([
    StructField("event", StringType(), nullable=False),
    StructField("fight_id", IntegerType(), nullable=False),
    StructField("round", StringType(), nullable=True),
    StructField("fighter", StringType(), nullable=True),
    StructField("knockdowns", IntegerType(), nullable=True),
    StructField("significant_strikes", StringType(), nullable=True),
    StructField("significant_strike_accuracy_pct", StringType(), nullable=True),
    StructField("total_strikes", StringType(), nullable=True),
    StructField("takedowns", StringType(), nullable=True),
    StructField("takedown_accuracy_pct", StringType(), nullable=True),
    StructField("submission_attempts", IntegerType(), nullable=True),
    StructField("reversals", IntegerType(), nullable=True),
    StructField("control_time", StringType(), nullable=True),
    StructField("significant_head_strikes", StringType(), nullable=True),
    StructField("significant_body_strikes", StringType(), nullable=True),
    StructField("significant_leg_strikes", StringType(), nullable=True),
    StructField("distance_strikes", StringType(), nullable=True),
    StructField("clinch_strikes", StringType(), nullable=True),
    StructField("ground_strikes", StringType(), nullable=True),
    StructField("last_run", TimestampType(), nullable=True)
])

schema_fighters = StructType([
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("nickname", StringType(), nullable=True),
    StructField("height", StringType(), nullable=True),
    StructField("weight", StringType(), nullable=True),
    StructField("reach", DoubleType(), nullable=True),
    StructField("stance", StringType(), nullable=True),
    StructField("wins", IntegerType(), nullable=True),
    StructField("losses", IntegerType(), nullable=True),
    StructField("draws", IntegerType(), nullable=True),
    StructField("belt", StringType(), nullable=True),
    StructField("link", StringType(), nullable=True),
    StructField("last_run", TimestampType(), nullable=True)
])

# 2) S3 & logging config
S3_BUCKET = 'ufc'
LOG_PATH = f'{S3_BUCKET}/logs/spark_s3_to_postgres.log'
S3_OPTS = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def write_df_to_postgres(df, table_name):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://postgres_db:5432/ufc_db") \
      .option("dbtable", table_name) \
      .option("user", "postgres") \
      .option("password", "postgres") \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite") \
      .save()

def log_run(duration_s, status):
    timestamp = datetime.utcnow().isoformat() + 'Z'
    log_line = f"{timestamp} duration={duration_s:.2f}s status={status}\n"
    fs = s3fs.S3FileSystem(**S3_OPTS)
    with fs.open(LOG_PATH, 'a') as log_file:
        log_file.write(log_line)
    print('----------------------')
    print(log_line)

def main():
    start_time = time.time()
    try:
        spark = SparkSession.builder \
            .appName("CSV from MinIO to PostgreSQL") \
            .master("local[*]") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
                    "org.postgresql:postgresql:42.2.18") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio-dev:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        # 3) Read each CSV with its schema and proper parsing options
        events = spark.read \
            .option("header", True) \
            .option("dateFormat", "MMMM d, yyyy") \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'") \
            .schema(schema_events) \
            .csv("s3a://ufc/UFC_Events.csv")

        fighters = spark.read \
            .option("header", True) \
            .schema(schema_fighters) \
            .csv("s3a://ufc/UFC_Fighters.csv")

        fights = spark.read \
            .option("header", True) \
            .schema(schema_fights) \
            .csv("s3a://ufc/UFC_Fights.csv")

        stats = spark.read \
            .option("header", True) \
            .schema(schema_fight_stats) \
            .csv("s3a://ufc/UFC_Fights_stats.csv")

        rounds = spark.read \
            .option("header", True) \
            .schema(schema_round_stats) \
            .csv("s3a://ufc/UFC_Round_stats.csv")

        # 4) Push to Postgres
        write_df_to_postgres(events, "events")
        write_df_to_postgres(fighters, "fighters")
        write_df_to_postgres(fights, "fights")
        write_df_to_postgres(stats, "fight_stats")
        write_df_to_postgres(rounds, "round_stats")

        spark.stop()
        log_run(time.time() - start_time, status="success")

    except Exception as e:
        log_run(time.time() - start_time, status=f"failed: {e}")
        raise

if __name__ == "__main__":
    main()
