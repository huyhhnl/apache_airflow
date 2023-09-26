from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
 
from datetime import datetime

def _branch():
    tabDays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    if tabDays[datetime.now().weekday()] =='monday':
        return 'task_for_monday'
    elif tabDays[datetime.now().weekday()]== 'tuesday':
        return 'task_for_tuesday'
    elif tabDays[datetime.now().weekday()]=='wednesday':
        return 'task_for_wednesday'
    elif tabDays[datetime.now().weekday()]=='thursday':
        return 'task_for_thursday'
    elif tabDays[datetime.now().weekday()]=='friday':
        return 'task_for_friday'
    elif tabDays[datetime.now().weekday()]=='saturday':
        return 'task_for_saturday'
    elif tabDays[datetime.now().weekday()]=='sunday':
        return 'task_for_sunday'



with DAG('branch_dag', start_date=datetime(2023,9,14), schedule_interval='@daily', catchup=False) as dag:
    branching = BranchPythonOperator(
        task_id = 'branching',
        python_callable=_branch
    )
    task_for_monday = DummyOperator(task_id = 'task_for_monday')
    task_for_tuesday = DummyOperator(task_id = 'task_for_tuesday')
    task_for_wednesday = DummyOperator(task_id = 'task_for_wednesday')
    task_for_thursday = DummyOperator(task_id = 'task_for_thursday')
    task_for_friday = DummyOperator(task_id = 'task_for_friday')
    task_for_saturday = DummyOperator(task_id = 'task_for_saturday')
    task_for_sunday = DummyOperator(task_id = 'task_for_sunday')
    branching>>[task_for_monday, task_for_tuesday, task_for_wednesday, task_for_thursday, task_for_friday, task_for_saturday, task_for_sunday]
