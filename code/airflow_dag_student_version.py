import airflow
import json
import os
import csv
import boto3
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

# ==============================================================

# The default arguments for your Airflow, these have no reason to change for the purposes of this predict.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    's3_to_rds_pipeline',
    default_args=default_args,
    description='Load data from S3 to RDS',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

HOME_DIR = "/opt/airflow"

# Insert your mount folder
MOUNT_FOLDER = os.path.join(HOME_DIR, "mnt", "data")

def upload_to_postgres(**kwargs):
    # Fetch the S3 key from the task instance's XCom
    s3_key = kwargs['task_instance'].xcom_pull(task_ids='fetch_s3_file')
    
    # Initialize S3Hook and download the file
    s3 = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'your-s3-bucket'
    file_obj = s3.get_key(s3_key, bucket_name)
    file_content = file_obj.get()['Body'].read().decode('utf-8')
    
    # Read CSV content
    df = pd.read_csv(io.StringIO(file_content))
    
    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='rds_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Load data into Postgres
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO your_table_name (column1, column2, column3)
            VALUES (%s, %s, %s)
        """, (row['column1'], row['column2'], row['column3']))
    
    conn.commit()
    cursor.close()
    conn.close()

    return "CSV Uploaded to postgres database"

def failure_sns(context):
    # Initialize SNS client
    sns_client = boto3.client('sns', region_name='your-region')
    topic_arn = 'arn:aws:sns:your-region:your-account-id:your-sns-topic'
    
    message = f"DAG: {context['dag'].dag_id} - Task: {context['task'].task_id} has failed."
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject='Airflow Task Failure Alert'
    )

    return "Failure SNS Sent"

def success_sns(context):
    # Initialize SNS client
    sns_client = boto3.client('sns', region_name='your-region')
    topic_arn = 'arn:aws:sns:your-region:your-account-id:your-sns-topic'
    
    message = f"DAG: {context['dag'].dag_id} - Task: {context['task'].task_id} has succeeded."
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject='Airflow Task Success Alert'
    )

    return "Success SNS sent"

# The dag configuration ===========================================================================
# Ensure your DAG calls on the success and failure functions above as it succeeds or fails.

with dag:
    start = DummyOperator(task_id='start')
    
    fetch_s3_file = PythonOperator(
        task_id='fetch_s3_file',
        python_callable=lambda: "path/to/your/csvfile.csv",
        provide_context=True
    )

    upload_task = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=success_sns,
        provide_context=True,
        trigger_rule='all_success'
    )

    failure_notification = PythonOperator(
        task_id='failure_notification',
        python_callable=failure_sns,
        provide_context=True,
        trigger_rule='all_failed'
    )

    # Define your Task flow below ===========================================================

    start >> fetch_s3_file >> upload_task >> end
    end >> success_notification
    fetch_s3_file >> failure_notification

# Define the tasks in the DAG
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# Set the task dependencies
start_task >> fetch_s3_file >> upload_task >> end_task
end_task >> success_notification
fetch_s3_file >> failure_notification
