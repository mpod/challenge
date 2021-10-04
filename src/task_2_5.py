import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG('my_dag', start_date=datetime(2021, 1, 1)) as dag:
    task_1 = DummyOperator(task_id='task_1', dag=dag)
    task_2 = DummyOperator(task_id='task_2', dag=dag)
    task_3 = DummyOperator(task_id='task_3', dag=dag)
    task_4 = DummyOperator(task_id='task_4', dag=dag)
    task_5 = DummyOperator(task_id='task_5', dag=dag)
    task_6 = DummyOperator(task_id='task_6', dag=dag)
    task_dummy = DummyOperator(task_id='dummy', dag=dag)

    task_1.set_downstream(task_2)
    task_1.set_downstream(task_3)
    task_2.set_downstream(task_dummy)
    task_3.set_downstream(task_dummy)
    task_dummy.set_downstream(task_4)
    task_dummy.set_downstream(task_5)
    task_dummy.set_downstream(task_6)

