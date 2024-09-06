import findspark
import os
import pandas as pd
import pendulum
import farmhash
from ulid import ULID
findspark.init(spark_home='/opt/spark')
import pyspark.sql.functions as fs
from pyspark.sql.types import StringType
from datasaku import datasaku_spark
from util.init import init

today = pendulum.now().format('YYYYMMDD')
logger, config, args = init()

def extract(execution_date:str=today):

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
    batch_id = str(ULID())

    sdf = sdf.withColumn('__unique_row_id', fs.md5(fs.concat_ws('', *all_column)))
    sdf = sdf.withColumn('__created_timestamp_utc', fs.lit(pendulum.now('utc').to_iso8601_string()))
    sdf = sdf.withColumn('__is_current', fs.lit(True))
    sdf = sdf.withColumn('__batch_id', fs.lit(batch_id))
    sdf = sdf.withColumn('__logical_date', fs.lit(execution_date))
    sdf.show()

    os.makedirs("/data/data/dev/extract/test", exist_ok=True)
    df = sdf.toPandas()
    df.to_csv(f'/data/data/dev/extract/test/test_{execution_date}_{batch_id}.csv', index=False)

    logger.info("extract success")

if __name__ == "__main__":
    extract(execution_date=args.execution_date)