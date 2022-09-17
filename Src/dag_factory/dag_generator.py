from jinja2 import Environment, FileSystemLoader
import yaml
import os
from airflow.settings import DAGS_FOLDER

file_dir = os.path.dirname(os.path.abspath(f'{__file__}/.'))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('templates/npm_registry.jinja2')

for filename in os.listdir(f'{file_dir}/configs'):
    with open(f'{file_dir}/configs/{filename}', 'r') as f:
        input = yaml.safe_load(f)
        with open(f'{ DAGS_FOLDER }/{filename.replace("yaml","py")}', 'w+') as dag_file:
            dag_file.write(template.render(input))         