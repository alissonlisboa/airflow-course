from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag
from airflow.utils.task_group import TaskGroup

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG("parallel_dag", 
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command="sleep 3"
    )

    with TaskGroup('parallel_tasks2') as parallel_tasks2:
        
        task2 = BashOperator(
            task_id='task2',
            bash_command="sleep 3"
        )
        
        task3 = BashOperator(
            task_id='task3',
            bash_command="sleep 3"
        )
    
    parallel_tasks = SubDagOperator(
        task_id='parallel_tasks',
        subdag=subdag_parallel_dag('parallel_dag', 'parallel_tasks', default_args)
    )
    
    task4 = BashOperator(
        task_id='task4',
        bash_command="sleep 3"
    )

    task1 >> parallel_tasks2 >> parallel_tasks >> task4
