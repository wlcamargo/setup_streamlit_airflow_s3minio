import boto3
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Wallace Graça',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

def move_file_minio(**kwargs):
    source_bucket = 'landing'
    target_bucket = 'bronze'
    key = 'data.csv'  
    new_key = 'moved_data.csv'

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='http://host.docker.internal:9000',
        aws_access_key_id='chapolin',
        aws_secret_access_key='mudar@123',
        region_name='us-east-1'
    )

    copy_source = {'Bucket': source_bucket, 'Key': key}
    s3.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=new_key)
    s3.delete_object(Bucket=source_bucket, Key=key)

with DAG(
    dag_id='sensor_s3_to_move_file',
    start_date=datetime(2025, 6, 12),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='landing',
        bucket_key='data.csv',
        aws_conn_id='conn_s3',
        mode='poke',
        poke_interval=5,
        timeout=9000
    )

    task2 = PythonOperator(
        task_id='move_file_task',
        python_callable=move_file_minio,
        provide_context=True
    )

    task1 >> task2
