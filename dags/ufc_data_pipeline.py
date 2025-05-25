from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# shared MinIO environment for all tasks
minio_env = {
    'MINIO_ENDPOINT': 'minio-dev:9000',
    'MINIO_ACCESS_KEY': 'minioadmin',
    'MINIO_SECRET_KEY': 'minioadmin',
    'MINIO_BUCKET': 'ufc'
}

# Postgres env for only the final loader task
pg_env = {
    'PG_HOST': 'postgres_db',
    'PG_PORT': '5432',
    'PG_USER': 'postgres',
    'PG_PASSWORD': 'postgres',
    'PG_DB': 'ufc_db'
}

with DAG(
    'ufc_data_pipeline',
    default_args=default_args,
    description='Run UFC data processing in Docker',
    # every Tuesday (2) at 09:00
    schedule_interval='0 9 * * 2',
    start_date=datetime(2025, 5, 17),
    catchup=False,
) as dag:

    # common kwargs
    common_kwargs = dict(
        image='ufc_app:latest',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='etl_net',
        mount_tmp_dir=False,
        tty=True,
        working_dir='/app',
    )

    run_events = DockerOperator(
        task_id='run_events',
        command='python src/scraper/events.py',
        environment=minio_env,
        **common_kwargs
    )

    run_fighters = DockerOperator(
        task_id='run_fighters',
        command='python src/scraper/fighters.py',
        environment=minio_env,
        **common_kwargs
    )

    run_fights = DockerOperator(
        task_id='run_fights',
        command='python src/scraper/fights.py',
        environment=minio_env,
        **common_kwargs
    )

    run_stats = DockerOperator(
        task_id='run_stats',
        command='python src/scraper/stats.py',
        environment=minio_env,
        **common_kwargs
    )

    
    run_s3_to_postgres = DockerOperator(
        task_id='run_s3_to_postgres',
        command='python src/s3_to_postgres.py',
        environment={**minio_env, **pg_env},
        **common_kwargs
    )

    run_bot_send_update = DockerOperator(
        task_id='run_bot_send_update',
        command='python src/bot/bot_send_update.py',
        environment={**minio_env, **pg_env},
        **common_kwargs
    )

    run_events >> run_fighters >> run_fights >> run_stats >> run_s3_to_postgres >> run_bot_send_update
