from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import sys

# Add your repository to the Python path
sys.path.append('/path/to/your/github/repository')

# Import the function from the script
from dag_report_utils import generate_and_format_report

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ticketing_report',
    default_args=default_args,
    description='A DAG to generate a report of failed and optionally running DAG runs for today and send it via email',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,  # Limit to 1 active DAG run at a time
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_and_format_report,
    op_kwargs={'include_running': True},  # Set to False to only include failed DAGs
    provide_context=True,
    dag=dag,
    execution_timeout=timedelta(minutes=10),  # Limit task execution time
)

send_email_task = EmailOperator(
    task_id='send_email',
    to='boxgideon@gmail.com',
    subject='DAG Runs Report (Running & Failed)',
    html_content="{{ task_instance.xcom_pull(task_ids='generate_report') or 'No DAG runs for today.' }}",
    dag=dag,
    retries=3,  # Increase retries for the email task
)

generate_report_task >> send_email_task
