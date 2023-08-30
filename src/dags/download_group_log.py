from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import os
import logging

import boto3
import pendulum

def fetch_s3_file(bucket: str, key: str) -> str:

    try:
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")

        )
        
        s3_client.download_file(
            Bucket=bucket,
            Key='group_log.csv',
            Filename=f'/data/group_log.csv'
        )

    except:
        logging.exception('Download error')

bash_command_tmpl = """
head {{ params.files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-08-28'))
def sprint6_get_group_log():
    fetch_tasks = PythonOperator(
            task_id='get_group_log',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
        )
    
    print_10_lines = BashOperator(
        task_id='print_10_lines',
        bash_command=bash_command_tmpl,
        params={'files': '/data/group_log.csv'}
    )    
    

    fetch_tasks >> print_10_lines


_ = sprint6_get_group_log()