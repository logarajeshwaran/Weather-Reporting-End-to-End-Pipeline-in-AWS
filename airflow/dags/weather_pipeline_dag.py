from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/ingestion')))
from weather_api_to_kinesis import WeatherKinesisPipeline
from load_mysql_to_rds import WeatherDataPipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_weather_api_to_kinesis():
    pipeline = WeatherKinesisPipeline()
    pipeline.run()

def run_load_mysql_to_rds():
    pipeline = WeatherDataPipeline()
    pipeline.run_pipeline()

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='DAG to orchestrate weather data ingestion and processing',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'pipeline'],
) as dag:

    task_weather_api_to_kinesis = PythonOperator(
        task_id='weather_api_to_kinesis',
        python_callable=run_weather_api_to_kinesis,
    )

    task_load_mysql_to_rds = PythonOperator(
        task_id='load_mysql_to_rds',
        python_callable=run_load_mysql_to_rds,
    )

    task_lambda_kinesis_to_s3 = AwsLambdaInvokeFunctionOperator(
        task_id='lambda_kinesis_to_s3',
        function_name='lambda_kinesis_to_s3',
        aws_conn_id='aws_default',
        invocation_type='Event',
    )

    task_lambda_athena_to_s3 = AwsLambdaInvokeFunctionOperator(
        task_id='lambda_athena_to_s3',
        function_name='lambda_athena_to_s3',
        aws_conn_id='aws_default',
        invocation_type='Event',
    )

    # Define task dependencies
    [task_weather_api_to_kinesis, task_load_mysql_to_rds] >> task_lambda_kinesis_to_s3 >> task_lambda_athena_to_s3
