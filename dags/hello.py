from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owener' : 'datamasterylab.com',
    'start_date' : datetime(2024, 1, 25),
    'catchup' : False,
}
dag = DAG(
    'hello',
    default_args=default_args,
    schedule= timedelta(days=1)
)
t1= BashOperator(
    task_id = 'Hello World',
    bash_command = 'echo "Hello World"',
    dag = dag
)
t2= BashOperator(
    task_id = 'Hello_dml',
    bash_command = 'echo "Hello Data Mastery Lab"',
    dag = dag
)

t1 >> t2