from datetime import datetime, timedelta

import airflow.hooks.S3_hook

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.utils.dates import days_ago

from model import train_model

# data source: data_s3_path = ' https://iasd-data-in-the-cloud.s3.eu-west-3.amazonaws.com/petrol_consumption.csv'

data_remote_bucket = 'iasd-data-in-the-cloud'
data_local_path = 'petrol_consumption.csv'

trained_model_local_path = 'airflow_model.pickle'
trained_model_remote_bucket = 'iasd-klouvi-data' # create your own s3 bucket and put it here

args = {
    'owner': 'KodjoKlouvi', # the owner id
    'start_date': days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='_ml_petroleum_pipeline',
    start_date=datetime(2020, 6, 5),
    default_args=args,
    schedule_interval=None,
    tags=['ml', 'pipeline', 'petrol']
)


# [START ml_pipeline]
def download_from_s3_task(key, bucket_name, output_path, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook('aws_default')
    source_object = hook.get_key(key, bucket_name)
    source_object.download_file(output_path)


def upload_to_s3_task(filename, key, bucket_name, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook('aws_default')
    hook.load_file(filename, key, bucket_name)


# let's start with a dummy task
start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

# task to download the dataset from s3
download_data_from_s3_task = PythonOperator(
    task_id='download_data_from_s3_task',
    provide_context=True,
    python_callable=download_from_s3_task,
    op_kwargs={'key': 'petrol_consumption.csv', 'bucket_name': data_remote_bucket, 'output_path': data_local_path},
    dag=dag,
)

# task to train the model (see model.py)
train_model_task = PythonOperator(
    task_id='train_model_task',
    provide_context=True,
    python_callable=train_model,
    op_kwargs={'dataset_filepath': data_local_path, 'trained_model_path': trained_model_local_path},
    dag=dag,
)

# task to upload the trained model to s3 (yours)
upload_model_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    provide_context=True,
    python_callable=upload_to_s3_task,
    op_kwargs={'filename': trained_model_local_path, 'key': 'airflow_model_{{ds}}.pickle', 'bucket_name': trained_model_remote_bucket},
    dag=dag,
)

# just a dummy task to end the DAGs
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# chain the pipeline
start_task >> download_data_from_s3_task >> train_model_task >> upload_model_to_s3_task >> end_task
