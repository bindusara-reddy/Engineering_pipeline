from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import random
import os
import zipfile
import datetime
import yaml

with open('params.yaml', 'r') as file:
    data = yaml.safe_load(file)

year = data['year']
num_files = data['num_files']
path = data['path']

def parse_html():
    with open('/tmp/data/index.html', 'r') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
        csv_links = [link['href'] for link in soup.find_all('a') if link['href'].endswith('.csv')]
        selected_files = random.sample(csv_links, int(num_files))
    
    with open('/tmp/data/output.txt', 'w') as outfile:
        outfile.write('\n'.join(str(i) for i in selected_files))

        return selected_files
    
def zip_files():
    with zipfile.ZipFile(f'/tmp/data/{year}_data.zip', 'w') as zipf:
        for foldername, subfolders, filenames in os.walk('/tmp/data/dataset/'):
            for file in filenames:
                zipf.write(os.path.join(foldername, file), arcname=file)


def move_archive():
    os.system(f"mv /tmp/data/{year}_data.zip {path}")


default_args = {
    'owner': 'bindusara',
    'start_date' : datetime.datetime(2024,3,10),
    'retries': 0,
}

dag = DAG(
    dag_id = 'DataFetch_Pipeline',
    default_args=default_args,
)

fetch_html = BashOperator(
    task_id='fetch_html_page',
    bash_command=f"mkdir -p /tmp/data/dataset && wget -O /tmp/data/index.html 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}'",
    dag=dag,
)


select_files = PythonOperator(
    task_id='parse_html',
    python_callable=parse_html,
    dag=dag,
)

download_data = BashOperator(
    task_id='download_data',
    bash_command=f"""
    while read line; do
        wget -P /tmp/data/dataset/ "https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/$line"
    done < /tmp/data/output.txt
    """,
    dag=dag,
)

zip_data = PythonOperator(
    task_id='zip_data',
    python_callable=zip_files,
    dag=dag,
)

move_data = PythonOperator(
    task_id='move_data',
    python_callable=move_archive,
    dag=dag,
)

fetch_html >> select_files >> download_data >> zip_data >> move_data
