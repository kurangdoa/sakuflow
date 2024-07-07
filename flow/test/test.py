import findspark
import os
import pandas as pd
import pendulum
import farmhash
findspark.init(spark_home='/opt/spark')
import pyspark.sql.functions as fs
from pyspark.sql.types import StringType
from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult
from datasaku import datasaku_spark

def farmhash_python(string_input:str):
    return farmhash.hash64(string_input)

def pendulum_now_python(string_input:str):
    return pendulum.now().to_date_string()

today = pendulum.now().format('YYYYMMDD')

# initiate the test environment

@asset(name='test__ingestion')
def ingestion():
    today = pendulum.now().format('YYYYMMDD')
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-arm64/'
    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.11'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/datasaku/.venv/bin/python3'
    
    Spark = datasaku_spark.DatasakuSparkNessieMinioIceberg(
        spark_home = '/opt/spark'
        , minio_username = 'minio'
        , minio_password = 'minio123'
    )

    spark = Spark.spark_session()

    sdf = spark.createDataFrame(
        [
            ("1", "chicago", "100", "kurang"),  # create your data here, be consistent in the types.
            ("2", "chicago", "200", "kurang"),
        ],
        ["Season", "Team", "Salary", "Player"]  # add your column names here
    )
    
    key_column = ['Team','Season','Player']
    all_column = sdf.columns

    sdf = sdf.withColumn('__unique_row_id', fs.md5(fs.concat_ws('', *all_column)))
    sdf = sdf.withColumn('__created_timestamp_utc', fs.lit(pendulum.now('utc').to_iso8601_string()))
    sdf = sdf.withColumn('__is_current', fs.lit(True))
    sdf = sdf.withColumn('__logical_date', fs.lit(today))
    sdf.show()

    os.makedirs("/data/data/dev/ingestion/test", exist_ok=True)
    df = sdf.toPandas()
    df.to_csv(f'/data/data/dev/ingestion/test/test_{today}.csv')

@asset(name='test__staging')
def staging():
    today = pendulum.now().format('YYYYMMDD')
    df = pd.read_csv(f'/data/data/dev/ingestion/test/test_{today}.csv')
    df['test_staging_column'] = 'test_staging_column'
    os.makedirs("/data/data/dev/staging/test", exist_ok=True)
    df.to_csv(f'/data/data/dev/staging/test/test_{today}.csv')

@asset(name='test__insertion')
def insertion():
    today = pendulum.now().format('YYYYMMDD')
    df = pd.read_csv(f'/data/data/dev/staging/test/test_{today}.csv')

    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-arm64/'
    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.11'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/datasaku/.venv/bin/python3'
    Spark = datasaku_spark.DatasakuSparkNessieMinioIceberg(
        spark_home = '/opt/spark'
        , minio_username = 'minio'
        , minio_password = 'minio123'
    )
    spark = Spark.spark_session()

    spark.sql("CREATE NAMESPACE nessie_catalog.datasaku")

    sdf = spark.createDataFrame(df)
    sdf.writeTo("nessie_catalog.datasaku.dagster_test").create()
    spark.read.format('iceberg').load('nessie_catalog.datasaku.dagster_test').show()