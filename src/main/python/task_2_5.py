from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='example_dag',
    start_date=datetime(2023, 2, 19),
    schedule_interval=None
)
# Dummy Operator from tast 1 to task 6
task_1 = DummyOperator(task_id='task_1', dag=dag)
task_2 = DummyOperator(task_id='task_2', dag=dag)
task_3 = DummyOperator(task_id='task_3', dag=dag)
task_4 = DummyOperator(task_id='task_4', dag=dag)
task_5 = DummyOperator(task_id='task_5', dag=dag)
task_6 = DummyOperator(task_id='task_6', dag=dag)

task_1 >> [task_2, task_3]
[task_2, task_3] >> [task_4, task_5, task_6]