import findspark
import os
import pandas as pd
import pendulum
import farmhash
findspark.init(spark_home='/opt/spark')
import pyspark.sql.functions as fs
from pyspark.sql.types import StringType
from datasaku import datasaku_spark

today = pendulum.now().format('YYYYMMDD')
def ingestion(execution_date:str=today):

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
    sdf = sdf.withColumn('__logical_date', fs.lit(execution_date))
    sdf.show()

    os.makedirs("/data/data/dev/ingestion/test", exist_ok=True)
    df = sdf.toPandas()
    df.to_csv(f'/data/data/dev/ingestion/test/test_{execution_date}.csv')

if __name__ == "__main__":
    ingestion()