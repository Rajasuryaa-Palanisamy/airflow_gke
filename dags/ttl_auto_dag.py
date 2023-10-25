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
    sql = Path("include/data_retention_retrieve_delete_policies.sql")
    return pg_hook.get_records(
        sql=sql.read_text(encoding="utf-8"),
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