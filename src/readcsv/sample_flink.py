from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, Csv, FileSystem
from pyflink.table.expressions import lit
import timeit

t1 = timeit.default_timer()
exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(2)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

t_env.connect(FileSystem().path('/Users/sriyan/Downloads/1500000_Sales_Records.csv')) \
    .with_format(Csv(field_delimiter=',', line_delimiter='\n', derive)) \
    .create_temporary_table('mySource')

t_env.connect(FileSystem().path('/tmp/output')) \
    .with_format(Csv(field_delimiter=',', line_delimiter='\n')) \
    .create_temporary_table('mySink')

tab = t_env.from_path('mySource')
tab.execute_insert('mySink')

print(timeit.default_timer() - t1)