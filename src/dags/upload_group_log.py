import contextlib
import hashlib
import json
from typing import Dict, List, Optional

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag

import pandas as pd
import pendulum
import vertica_python
import logging


def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host=Variable.get('v_host'),
        port=Variable.get('v_port'),
        user=Variable.get('v_user'),
        password=Variable.get('v_password')
    )
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            logging.info(f"loading rows {start}-{end}")
            logging.info("loaded")
            start += chunk_size + 1

    vertica_conn.close()


@dag(schedule_interval=None, start_date=pendulum.parse('2023-08-28'))
def sprint6_dag_load_group_log_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_dataset_file_to_vertica,
        op_kwargs={
            'dataset_path': '/data/group_log.csv',
            'schema': 'STV230547__STAGING',
            'table': 'group_log',
            'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'datetime'],
        },
    )

    start >> load_group_log >> end


_ = sprint6_dag_load_group_log_to_staging()
