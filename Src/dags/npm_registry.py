import sys
sys.path.append('..')

from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group

from Utils.Connectors.Databases import database_loader

DEFAULT_START_DATE = datetime(2022,9,17)
DEFAULT_END_DATE = None
DEFAULT_SCHEDULE_INTERVAL = '@daily'

with DAG(
    'npm_registry',
    start_date=DEFAULT_START_DATE ,
    end_date=DEFAULT_END_DATE ,
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL ,
    catchup=False
) as dag:

    @task 
    def load_api_to_lake():
        return 1

    @task 
    def prepare_data_before_loading(raw_df):
        return 1

    
    @task
    def load_to_dim_package(pre_df):
        return database_loader.load_to_dim_package(pre_df)
    
    @task
    def load_to_dim_version(pre_df):
        return database_loader.load_to_dim_version(pre_df)
    

    @task_group
    def dim_task_group(pre_df):
        tasks = [ load_to_dim_package(pre_df) , load_to_dim_version(pre_df) ]
        tasks
    
    dim_task_group(prepare_data_before_loading(load_api_to_lake()))