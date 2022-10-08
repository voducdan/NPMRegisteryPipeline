import os
import json

from Connectors.APIs.NpmRegistry import NpmRegistry 
from date import DateUtils

def get_all_docs_from_lake():
    all_packages = []
    package_url = DateUtils.today_to_url('npm_registry/_all_docs')
    for file in os.listdir(package_url):
        file_url = f'{package_url}/{file}'
        with open(file_url, 'r') as f:
            packages = f.read()
            j_packages = json.loads(packages)
            all_packages.extend([ p['id'] for p in j_packages])
    return all_packages

def get_all_docs():
    npm_registry = NpmRegistry()
    npm_registry.get_all_docs_and_save_to_lake(sub_url='_all_docs',field='rows')

def get_doc_detail():
    npm_registry = NpmRegistry()
    all_docs = get_all_docs_from_lake()
    for doc in all_docs[:2]:
        npm_registry.get_doc_detail_and_save_to_lake(doc=doc)

def extract_from_lake():
    pass

def load_to_dim_package(pre_df):
    return 1

def load_to_dim_version(pre_df):
    return 1