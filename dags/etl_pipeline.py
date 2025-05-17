from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



from src.scraper.events import main as run_events
from src.scraper.fighters import main as run_fighters
from src.scraper.fights import main as run_fights
from src.scraper.stats import main as run_stats
from src.s3_to_postgres import main as run_s3_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='full_etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline: events, fighters, fights, stats, then load to Postgres/S3',
    start_date=datetime(2025, 5, 12),
    schedule_interval='0 9 * * 1',
    catchup=False,
) as dag:

    extract_events = PythonOperator(
        task_id='extract_events',
        python_callable=run_events,
    )

    extract_fighters = PythonOperator(
        task_id='extract_fighters',
        python_callable=run_fighters,
    )

    extract_fights = PythonOperator(
        task_id='extract_fights',
        python_callable=run_fights,
    )

    compute_stats = PythonOperator(
        task_id='compute_stats',
        python_callable=run_stats,
    )

    load_to_s3_postgres = PythonOperator(
        task_id='load_to_s3_postgres',
        python_callable=run_s3_to_postgres,
    )

    extract_events >> extract_fighters >> extract_fights >> compute_stats >> load_to_s3_postgres
