from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group

from Connectors.Databases import database_loader

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
    def get_all_docs():
        return database_loader.get_all_docs()
    
    @task
    def get_doc_detail():
        return database_loader.get_doc_detail()
    
    @task
    def extract_from_lake():
        return database_loader.extract_from_lake()
    

    
    @task
    def load_to_dim_package(pre_df):
        return database_loader.load_to_dim_package(pre_df)
    
    @task
    def load_to_dim_version(pre_df):
        return database_loader.load_to_dim_version(pre_df)
    

    @task_group
    def extract_task_group():
        return get_all_docs()  >> get_doc_detail()  >> extract_from_lake() 

    @task_group
    def dim_task_group(pre_df):
        return [ load_to_dim_package(pre_df) , load_to_dim_version(pre_df) ]
        
    
    dim_task_group(extract_task_group())