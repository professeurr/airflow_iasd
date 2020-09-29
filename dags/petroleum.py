from datetime import datetime, timedelta

import airflow.hooks.S3_hook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from model import train_model

# data source: data_s3_path = ' https://iasd-data-in-the-cloud.s3.eu-west-3.amazonaws.com/petrol_consumption.csv'
# the data was copied into my own S3 bucket
# set up some variables
remote_bucket = 'iasd-klouvi-data'  # S3 bucket name where to store data and trained models
data_path = 'petrol_consumption.csv'  # dataset file name
trained_model_path = 'petrol_consumption_model.pickle'  # file where to save the trained model
aws_credentials_key = 'aws_credentials'

args = {
    'owner': 'KodjoKlouvi',  # the owner id (Kodjo KLOUVI)
    'start_date': days_ago(0),
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    dag_id='_ml_petroleum_pipeline',
    start_date=datetime(2020, 9, 29),
    default_args=args,
    schedule_interval="0 */1 * * *",  # every hours
    tags=['ml', 'pipeline', 'petrol']
)


def download_from_s3_task(aws_credentials, bucket_name, key, output_path, **kwargs):
    """
        Download data from S3 bucket
    """
    hook = airflow.hooks.S3_hook.S3Hook(aws_credentials)  # AWS credentials are stored into Airflow connections manager
    source_object = hook.get_key(key, bucket_name)
    source_object.download_file(output_path)


def upload_to_s3_task(aws_credentials, bucket_name, filename, **kwargs):
    """
        Upload data into S3 bucket
    """
    hook = airflow.hooks.S3_hook.S3Hook(aws_credentials)
    key = datetime.now().strftime(
        "%Y/%m/%d/%H/") + filename  # the trained model is pushed into S3 under the folder YYYY/MM/dd/HH
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
    op_kwargs={'aws_credentials': aws_credentials_key, 'bucket_name': remote_bucket, 'key': data_path,
               'output_path': data_path},
    dag=dag,
)

# task to train the model (see model.py)
train_model_task = PythonOperator(
    task_id='train_model_task',
    provide_context=True,
    python_callable=train_model,
    op_kwargs={'dataset_filepath': data_path, 'trained_model_path': trained_model_path},
    dag=dag,
)

# task to upload the trained model to s3 (yours)
upload_model_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    provide_context=True,
    python_callable=upload_to_s3_task,
    op_kwargs={'aws_credentials': aws_credentials_key, 'bucket_name': remote_bucket, 'filename': trained_model_path},
    dag=dag,
)

# just a dummy task to end the DAGs
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# chain the pipeline start -> download -> train -> upload -> end
start_task >> download_data_from_s3_task >> train_model_task >> upload_model_to_s3_task >> end_task
