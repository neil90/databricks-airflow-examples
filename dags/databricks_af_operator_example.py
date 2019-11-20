from datetime import timedelta

import airflow
from airflow.models import DAG

from airflow.operators.bash_operator import BashOperator

from airflow.contrib.operators.databricks_operator import (
    DatabricksRunNowOperator,
)

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "depends_on_past": True,
}

dag = DAG(
    dag_id="databricks_af_operator",
    default_args=args,
    schedule_interval="@daily",
)

t1 = BashOperator(
    task_id="t1_bash_operator",
    bash_command="echo Airflow execution date is {{ ds }} ",
    dag=dag,
)

json = {"notebook_params": {"airflow_param": "NeilAF", "dt": "{{ ds }}"}}
# by default uses the connection databricks_conn_id
t2 = DatabricksRunNowOperator(
    task_id="notebook_run", json=json, job_id=21598, dag=dag
)

t1 >> t2
