import findspark
import os
import pandas as pd
import pendulum
import farmhash
findspark.init(spark_home='/opt/spark')
import pyspark.sql.functions as fs
from pyspark.sql.types import StringType
from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult, DailyPartitionsDefinition
from datasaku import datasaku_spark
from dagster import DagsterInstance

def farmhash_python(string_input:str):
    return farmhash.hash64(string_input)

def pendulum_now_python(string_input:str):
    return pendulum.now().to_date_string()

today = pendulum.now().format('YYYYMMDD')

from test.ingestion import ingestion

# initiate the test environment

@asset(name='test__ingestion', partitions_def=DailyPartitionsDefinition(start_date="2024-07-01"))
def dagster_ingestion(context:AssetExecutionContext):
    # context.log.info(context.partition_time_window)
    my_run_id = context.run.run_id
    print(my_run_id)
    partition_date_str = context.partition_key
    partition_date_str = pendulum.parse(partition_date_str).format('YYYYMMDD')
    ingestion(partition_date_str)
    context.log.info(f"ingestion done!")

@asset(name='test__staging', deps=[dagster_ingestion], partitions_def=DailyPartitionsDefinition(start_date="2024-07-01"))
def dagster_staging(context:AssetExecutionContext):
    my_run_id = context.run.run_id
    print(my_run_id)
    partition_date_str = context.partition_key
    partition_date_str = pendulum.parse(partition_date_str).format('YYYYMMDD')
    df = pd.read_csv(f'/data/data/dev/ingestion/test/test_{partition_date_str}.csv')
    df['test_staging_column'] = 'test_staging_column'
    os.makedirs("/data/data/dev/staging/test", exist_ok=True)
    df.to_csv(f'/data/data/dev/staging/test/test_{partition_date_str}.csv')
    context.log.info(f"staging done!")

@asset(name='test__insertion', deps=[dagster_staging], partitions_def=DailyPartitionsDefinition(start_date="2024-07-01"))
def dagster_insertion(context:AssetExecutionContext):
    my_run_id = context.run.run_id
    print(my_run_id)
    partition_date_str = context.partition_key
    partition_date_str = pendulum.parse(partition_date_str).format('YYYYMMDD')

    df = pd.read_csv(f'/data/data/dev/staging/test/test_{partition_date_str}.csv')

    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-arm64/'
    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.11'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/datasaku/.venv/bin/python3'
    Spark = datasaku_spark.DatasakuSparkNessieMinioIceberg(
        spark_home = '/opt/spark'
        , minio_username = 'minio'
        , minio_password = 'minio123'
    )
    spark = Spark.spark_session()
    Spark.namespace_create(namespace='datasaku')

    sdf = spark.createDataFrame(df)

    Spark.dataframe_append(sdf, table_path = "nessie_catalog.datasaku.dagster_test")
    spark.read.format('iceberg').load('nessie_catalog.datasaku.dagster_test').show()
    context.log.info(f"insertion done!")
