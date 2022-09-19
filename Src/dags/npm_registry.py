from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group

from Connectors.Databases import database_loader
from Connectors.APIs.NpmRegistry import NpmRegistry 

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
        npm_registry = NpmRegistry()
        npm_registry.make_api_request_and_save_to_lake(sub_url='_all_docs',field='rows')

    
    @task
    def extract_from_lake(raw_df):
        return database_loader.extract_from_lake(raw_df)
    

    
    @task
    def load_to_dim_package(pre_df):
        return database_loader.load_to_dim_package(pre_df)
    
    @task
    def load_to_dim_version(pre_df):
        return database_loader.load_to_dim_version(pre_df)
    

    @task_group
    def extract_task_group(raw_df):
        tasks = [ extract_from_lake(raw_df) ]
        return tasks

    @task_group
    def dim_task_group(pre_df):
        tasks = [ load_to_dim_package(pre_df) , load_to_dim_version(pre_df) ]
        return tasks
    
    dim_task_group(extract_task_group(load_api_to_lake()))