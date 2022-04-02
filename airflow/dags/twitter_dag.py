from datetime import datetime, date, timedelta
from os.path import join
from pathlib import Path
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
import sys
sys.path.append('/home/renanl/datapipeline/airflow')
from plugins.operators.twitter_operator import TwitterOperator


ARGS = {
    "owner": "airflow",
    "email": "renan.dsouza.luiz@gmail.com",
    "depends_on_past": False,
    "start_date": days_ago(6),
}

BASE_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "alura/datalake/{stage}/twitter_aluraonline/{partition}",
)

PARTITION_FOLDER = "extract_date={{ ds }}"

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
    dag_id="twitter_dag",
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=join(
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "AluraOnline_{{ ds_nodash }}.json"
        ),
        start_time=(date.today()-timedelta(days = 6)).strftime(f'{ TIMESTAMP_FORMAT }'),
        end_time=(date.today()-timedelta(days = 1)).strftime(f'{ TIMESTAMP_FORMAT }')
        #start_time=(
         #   "{{"
          #  f" execution_date.strftime('{ TIMESTAMP_FORMAT }') "
           # "}}"
        #),
        #end_time=(
         #   "{{"
          #  f" next_execution_date.strftime('{ TIMESTAMP_FORMAT }') "
           # "}}"
        #)
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application=join(
            str(Path(__file__).parents[2]),
            "spark/transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "--path",
            BASE_FOLDER.format(stage="silver", partition=""),
            "--process_date",
            "{{ ds }}",
        ]
    )

    twitter_insight = SparkSubmitOperator(
        task_id="twitter_aluraonline_insight",
        application=join(
            str(Path(__file__).parents[2]),
            "spark/insight_tweet.py"
        ),
        name="twitter_insight",
        application_args=[
            "--src",
            BASE_FOLDER.format(stage="silver", partition="tweets"),
            "--path",
            BASE_FOLDER.format(stage="gold", partition=""),
            "--process_date",
            "{{ ds }}",
        ]
    )

    twitter_operator >> twitter_transform >> twitter_insight