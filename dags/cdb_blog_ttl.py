"""
Implements a retention policy by dropping expired partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-implementation-of-data-retention-policy/913

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/data_retention_schema.sql in this repository.
"""
from pathlib import Path
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task


def map_policy(policy):
    return {
        "table_fqn": policy[0],
        "column": policy[1],
        "value": policy[2],
    }


@task
def get_policies(ds=None):
    """Retrieve all partitions effected by a policy"""
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    #sql = Path("include/data_retention_retrieve_delete_policies.sql")
    return pg_hook.get_records(
        sql=""" SELECT QUOTE_IDENT(p.table_schema) || '.' || QUOTE_IDENT(p.table_name),
       QUOTE_IDENT(r.partition_column),
       TRY_CAST(p.values[r.partition_column] AS BIGINT)
FROM information_schema.table_partitions p
JOIN doc.retention_policies r ON p.table_schema = r.table_schema
  AND p.table_name = r.table_name
  AND p.values[r.partition_column] < %(day)s::TIMESTAMP - (r.retention_period || ' days')::INTERVAL
WHERE r.strategy = 'delete'; """,
        parameters={"day": ds},
    )


@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def data_retention_delete():
    SQLExecuteQueryOperator.partial(
        task_id="delete_partition",
        conn_id="cratedb_connection",
        sql="DELETE FROM {{params.table_fqn}} WHERE {{params.column}} = {{params.value}};",
    ).expand(params=get_policies().map(map_policy))


data_retention_delete()