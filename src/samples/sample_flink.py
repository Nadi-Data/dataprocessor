from pyflink.table import EnvironmentSettings, StreamTableEnvironment, BatchTableEnvironment
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf
from timeit import default_timer as timer
import logging
import sys

def test():
    # 1. create a TableEnvironment
    #env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    #table_env = StreamTableEnvironment.create(environment_settings=env_settings)
    env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
    table_env = BatchTableEnvironment.create(environment_settings=env_settings)

    # 2. create source Table
    table_env.execute_sql("""
        CREATE TABLE source_table (
            Region VARCHAR,
            Country	VARCHAR,
            Item_Type VARCHAR,
            Sales_Channel VARCHAR,
            Order_Priority VARCHAR,
            Order_Date VARCHAR,
            Order_ID VARCHAR,
            Ship_Date VARCHAR,
            Units_Sold VARCHAR,
            Unit_Price VARCHAR,
            Unit_Cost VARCHAR,
            Total_Revenue VARCHAR,
            Total_Cost VARCHAR,
            Total_Profit VARCHAR
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/tmp/data/5m_Sales_Records.csv',
            'format' = 'csv'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE sink_table (
            Region VARCHAR,
            Country	VARCHAR,
            Item_Type VARCHAR,
            Sales_Channel VARCHAR,
            Order_Priority VARCHAR,
            Order_Date VARCHAR,
            Order_ID VARCHAR,
            Ship_Date VARCHAR,
            Units_Sold VARCHAR,
            Unit_Price VARCHAR,
            Unit_Cost VARCHAR,
            Total_Revenue VARCHAR,
            Total_Cost VARCHAR,
            Total_Profit VARCHAR
        )
          WITH (
            'connector' = 'filesystem',
            'path' = '/tmp/data/xxx_Sales_Records.csv',
            'format' = 'csv'
        )
    """)
   
    @udf(input_types=DataTypes.STRING(), result_type=DataTypes.ARRAY(DataTypes.STRING()))
    def split(input_str: str):
       return input_str.split(",")


    @udf(input_types=[DataTypes.ARRAY(DataTypes.STRING()), DataTypes.INT()], result_type=DataTypes.STRING())
    def get(arr, index):
       return arr[index]

    table_env.register_function("split", split)
    table_env.register_function("get", get)

    table_env.sql_query("SELECT * FROM source_table order by Region") \
            .execute_insert("sink_table").wait()

if __name__ == '__main__':
    #logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    start = timer()
    test()
    print(timer() - start)
