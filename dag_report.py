import pandas as pd
import fnmatch
from datetime import datetime
from airflow.models import DagRun
from airflow.utils.email import send_email

def generate_and_send_report(include_running=True):
    today = datetime.now().date()
    dag_runs = DagRun.find()  # Fetch all DAG runs

    # Initialize dictionaries for the latest DAG runs
    latest_dag_runs = {}

    # Collect the latest run for each DAG
    for run in dag_runs:
        execution_date = run.execution_date.date() if isinstance(run.execution_date, datetime) else run.execution_date

        if execution_date == today:
            if run.dag_id not in latest_dag_runs or run.execution_date > latest_dag_runs[run.dag_id].execution_date:
                latest_dag_runs[run.dag_id] = run

    # List of DAGs or patterns to ignore (wildcards supported)
    ignore_dag_list = ['ignore_this_dag', 'test_*', 'example_*', 'siera_dep*']

    # Filter out DAGs based on the ignore list and their state
    running_dag_runs = {
        dag_id: run for dag_id, run in latest_dag_runs.items()
        if not any(fnmatch.fnmatch(dag_id, pattern) for pattern in ignore_dag_list) and run.state == 'running'
    }
    
    failed_dag_runs = {
        dag_id: run for dag_id, run in latest_dag_runs.items()
        if not any(fnmatch.fnmatch(dag_id, pattern) for pattern in ignore_dag_list) and run.state == 'failed'
    }

    # Format datetime objects
    def format_datetime(dt):
        return dt.strftime('%m-%d-%Y %H:%M:%S') if dt else 'N/A'

    # Generate HTML report
    def generate_html_section(title, dag_runs):
        if dag_runs:
            report = [
                {
                    'DAG ID': run.dag_id,
                    'Execution Date': format_datetime(run.execution_date),
                    'Start Date': format_datetime(run.start_date),
                    'End Date': format_datetime(run.end_date),
                    'State': run.state
                }
                for run in dag_runs.values()
            ]
            df = pd.DataFrame(report)
            html = df.to_html(index=False, border=0, classes='dataframe', justify='left')
            return f"<h2>{title}</h2>{html}"
        else:
            return f"<h2>{title}</h2><p>No {title.lower()} for today.</p>"

    if include_running:
        running_section = generate_html_section("Running DAGs", running_dag_runs)
    else:
        running_section = ""

    failed_section = generate_html_section("Failed DAGs", failed_dag_runs)

    styled_html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: 'Helvetica Neue', Arial, sans-serif;
                color: #4A4A4A;
                background-color: #F5F5F5;
                margin: 40px;
                line-height: 1.6;
            }}
            h2 {{
                color: #0b2f6d;
                border-bottom: 2px solid #0b2f6d;
                padding-bottom: 10px;
            }}
            p {{
                font-size: 14px;
            }}
            table.dataframe {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                font-size: 14px;
                background-color: #fff;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }}
            th {{
                background-color: #0b2f6d;
                color: white;
                text-align: left;
                padding: 10px;
                border-bottom: 1px solid #ddd;
            }}
            td {{
                padding: 10px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            tr {{
                background-color: #fff;
            }}
            tr:hover {{
                background-color: #f1f1f1;
            }}
        </style>
    </head>
    <body>
        <h1>Daily DAG Runs Report</h1>
        <p>Dear Team,</p>
        <p>Please find below the report of today's DAG runs.</p>
        {running_section}
        {failed_section}
        <p>Best Regards,<br/>Airflow Monitoring Team</p>
    </body>
    </html>
    """

    # Send email with the report
    send_email(
        to='boxgideon@gmail.com',
        subject='DAG Runs Report (Running & Failed)',
        html_content=styled_html
    )
