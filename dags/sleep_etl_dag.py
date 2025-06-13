from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# Cấu hình DAG
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
with DAG(
    dag_id='sleep_data_etl',
    default_args=default_args,
    description='ETL sleep dataset',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # hoặc None nếu muốn chạy thủ công
    catchup=False
) as dag:

    def run_etl_script():
        script_path = os.path.join(os.getcwd(), 'scripts', 'etl_sleep_data.py')
        subprocess.run(['python', script_path], check=True)

    etl_task = PythonOperator(
        task_id='run_etl_script',
        python_callable=run_etl_script
    )

    etl_task  # chỉ 1 task
