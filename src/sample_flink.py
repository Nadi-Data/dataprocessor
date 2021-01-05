from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import lit

# environment configuration
exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

# register Orders table and Result table sink in table environment
ip_data_path = "/tmp/input"
op_data_path = "/tmp/output"
source_ddl = f"""
        create table Orders(
            a VARCHAR
        ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{ip_data_path}'
        )
        """
t_env.execute_sql(source_ddl)

sink_ddl = f"""
    create table `Result`(
        a VARCHAR,
        cnt BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{op_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

# specify table program
orders = t_env.from_path("Orders")

orders.group_by("a").select(orders.a, orders.a.count.alias('cnt')).execute_insert("Result").wait()