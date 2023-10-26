from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow import models
from airflow.models import TaskInstance
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path

def map_policy(policy):
    return {
        "table_fqn": policy[0],
        "column": policy[1],
        "value": policy[2],
    }

def get_policies(ds=None):
    """Retrieve all partitions effected by a policy"""
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    #sql = Path("dags/sql/data_retention_delete.sql")
    return pg_hook.get_records(
        sql="""SELECT QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(r.partition_column),
       TRY_CAST(p.values[r.partition_column] AS BIGINT)
FROM information_schema.table_partitions p
JOIN doc.retention_policies r ON p.table_schema = r.table_schema
  AND p.table_name = r.table_name
  AND p.values[r.partition_column] < %(day)s::TIMESTAMP - (r.retention_period || ' days')::INTERVAL
WHERE r.strategy = 'delete';""",
        parameters={"day": ds},
    )


default_args = {
    'owner': 'npd',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': datetime.timedelta(minutes=5)
}

def data_retention_delete():
    SQLExecuteQueryOperator.partial(
        task_id="delete_partition",
        conn_id="cratedb_connection",
        sql="DELETE FROM {{params.table_fqn}} WHERE {{params.column}} = {{params.value}};",
    ).expand(params=get_policies().map(map_policy))


with models.DAG(
    'ttl_dag',
    start_date= days_ago(1),
    schedule_interval=None, 
    catchup=False,
    default_args=default_args
) as dag:


    start = DummyOperator(task_id='start', dag=dag)


    data_retention_delete_tsk = PythonOperator(
        task_id='data_retention_delete_tsk',
        python_callable=data_retention_delete,
        dag=dag,
    )

    end = DummyOperator(task_id='end', dag=dag)

    start >> data_retention_delete_tsk  >> end