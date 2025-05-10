import time
from datetime import datetime
import s3fs

from pyspark.sql import SparkSession

S3_BUCKET = 'ufc'
LOG_PATH  = f'{S3_BUCKET}/logs/spark_s3_to_postgres.log'
S3_OPTS   = {
    'key': 'minioadmin',
    'secret': 'minioadmin',
    'client_kwargs': {'endpoint_url': 'http://minio-dev:9000'}
}

def write_df_to_postgres(df, table_name):
    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres_db:5432/ufc_db")
        .option("dbtable", table_name)
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

def log_run(duration_s, status):
    timestamp = datetime.utcnow().isoformat() + 'Z'
    log_line  = f"{timestamp} duration={duration_s:.2f}s status={status}\n"
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
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.postgresql:postgresql:42.2.18") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio-dev:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        # Read CSV files from MinIO
        events = spark.read.option("header", "true").csv("s3a://ufc/UFC_Events.csv")
        fighters = spark.read.option("header", "true").csv("s3a://ufc/UFC_Fighters.csv")
        fights = spark.read.option("header", "true").csv("s3a://ufc/UFC_Fights.csv")
        stats = spark.read.option("header", "true").csv("s3a://ufc/UFC_Fights_stats.csv")
        rounds = spark.read.option("header", "true").csv("s3a://ufc/UFC_Round_stats.csv")

        # Write each DataFrame to PostgreSQL
        write_df_to_postgres(events, "events")
        write_df_to_postgres(fighters, "fighters")
        write_df_to_postgres(fights, "fights")
        write_df_to_postgres(stats, "fight_stats")
        write_df_to_postgres(rounds, "round_stats")

        spark.stop()
        log_run(time.time() - start_time, status="success")
        print
    except Exception as e:
        log_run(time.time() - start_time, status=f"failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
