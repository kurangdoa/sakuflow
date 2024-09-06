import findspark
import os
import pandas as pd
import pendulum
import farmhash
import re
from ulid import ULID
findspark.init(spark_home='/opt/spark')
import pyspark.sql.functions as fs
from pyspark.sql.types import StringType
from datasaku import datasaku_spark
from util.init import init
from util.file_folder.file_folder import get_latest_file_from_folder

today = pendulum.now().format('YYYYMMDD')
logger, config, args = init()

def load(execution_date:str=today):
    pattern = re.compile(rf'test_{execution_date}_.*\.*')
    transform_dir = '/data/data/dev/transform/test/'
    file = get_latest_file_from_folder(folder_path=transform_dir, file_pattern_regex=rf'test_{execution_date}_.*\.*')

    logger.info(file)
    df = pd.read_csv(os.path.join(transform_dir,file))

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
    sdf.show()

    Spark.dataframe_write(sdf, table_path = "nessie_catalog.datasaku.dagster_test", mode = 'overwrite')
    spark.read.format('iceberg').load('nessie_catalog.datasaku.dagster_test').show()

    logger.info("load success")

if __name__ == "__main__":
    load(execution_date=args.execution_date)