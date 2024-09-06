import findspark
import os
import pandas as pd
import pendulum
import farmhash
import re
findspark.init(spark_home='/opt/spark')
import pyspark.sql.functions as fs
from pyspark.sql.types import StringType
from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult, DailyPartitionsDefinition, DagsterInstance
from datasaku import datasaku_spark
from util.file_folder.file_folder import get_latest_file_from_folder
from test.extract.extract import extract
from test.transform.transform import transform
from test.load.load import load

def farmhash_python(string_input:str):
    return farmhash.hash64(string_input)

def pendulum_now_python(string_input:str):
    return pendulum.now().to_date_string()

today = pendulum.now().format('YYYYMMDD')

# initiate the test environment

@asset(name='test__extract', partitions_def=DailyPartitionsDefinition(start_date="2024-07-01"))
def dagster_extract(context:AssetExecutionContext):
    # context.log.info(context.partition_time_window)
    my_run_id = context.run.run_id
    print(my_run_id)
    partition_date_str = context.partition_key
    partition_date_str = pendulum.parse(partition_date_str).format('YYYYMMDD')
    extract(partition_date_str)
    context.log.info(f"extract done!")

@asset(name='test__transform', deps=[dagster_extract], partitions_def=DailyPartitionsDefinition(start_date="2024-07-01"))
def dagster_transform(context:AssetExecutionContext):
    my_run_id = context.run.run_id
    print(my_run_id)
    partition_date_str = context.partition_key
    partition_date_str = pendulum.parse(partition_date_str).format('YYYYMMDD')
    transform(execution_date = partition_date_str)
    context.log.info(f"transform done!")

@asset(name='test__load', deps=[dagster_transform], partitions_def=DailyPartitionsDefinition(start_date="2024-07-01"))
def dagster_load(context:AssetExecutionContext):
    my_run_id = context.run.run_id
    print(my_run_id)
    partition_date_str = context.partition_key
    partition_date_str = pendulum.parse(partition_date_str).format('YYYYMMDD')
    load(execution_date=partition_date_str)
    context.log.info(f"load done!")
