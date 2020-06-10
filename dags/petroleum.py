from datetime import datetime, timedelta

import airflow.hooks.S3_hook

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from model import train_model


data_s3_path = ' https://iasd-data-in-the-cloud.s3.eu-west-3.amazonaws.com/petrol_consumption.csv'
data_local_path = './petrol_consumption.csv'
trained_model_local_path = './model.pickle'


args = {
    'owner': 'KodjoKlouvi'
}

dag = DAG(
    dag_id='_ml_petroleum_pipeline',
    start_date = datetime(2020, 6, 5),
    # retry_delay = timedelta(minutes=5),
    default_args=args,
    schedule_interval=None,
    tags=['ml', 'pipeline', 'example']
)


# [START ml_pipeline]
def download_from_s3_task(key, bucket_name, output_path):
    hook = airflow.hooks.S3_hook.S3Hook('aws_default')
    source_object = hook.get_key(key, bucket_name)
    source_object.get_contents_to_file(output_path)


def upload_to_s3_task(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('aws_default')
    hook.load_file(filename, key, bucket_name)


def echo(str):
    print(str)


start_task = PythonOperator(
    task_id='start_task',
    provide_context=False,
    python_callable=echo,
    op_kwargs={'str':'Start the pipeline'},
    dag=dag,
)

download_data_from_s3_task = PythonOperator(
    task_id='download_data_from_s3_task',
    provide_context=True,
    python_callable=download_from_s3_task,
    op_kwargs={'key': 'petrol_consumption.csv', 'bucket_name':'iasd-data-in-the-cloud'},
    dag=dag,
)

train_model_task = PythonOperator(
    task_id='train_model_task',
    provide_context=True,
    python_callable=train_model,
    op_kwargs={'dataset_filepath': data_local_path, 'trained_model_path': trained_model_local_path},
    dag=dag,
)

upload_model_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    provide_context=True,
    python_callable=upload_to_s3_task,
    op_kwargs={'filename':trained_model_local_path, 'key': 'petrol_consumption.csv', 'bucket_name':'iasd-data-in-the-cloud'},
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_task',
    provide_context=False,
    python_callable=echo,
    op_kwargs={'str':'Pipeline ended'},
    dag=dag,
)

start_task >> download_data_from_s3_task >> train_model_task >> upload_model_to_s3_task >> end_task

