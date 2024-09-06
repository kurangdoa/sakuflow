import findspark
import os
import pandas as pd
import pendulum
import sys
from dagster import get_dagster_logger
from util.init import init
from util.file_folder.file_folder import get_latest_file_from_folder
import logging

today = pendulum.now().format('YYYYMMDD')
logger, config, args = init()

def transform(execution_date:str=today
            , logger:logging.Logger=logger
            , config:dict=config
            ):
    
    env = os.getenv('env')
    extract_dir = f'/data/data/{env}/extract/test/'
    file = get_latest_file_from_folder(folder_path=extract_dir, file_pattern_regex=rf'test_{execution_date}_.*\.*')

    df = pd.read_csv(os.path.join(extract_dir,file))

    transform_dir = f"/data/data/{env}/transform/test"
    os.makedirs(transform_dir, exist_ok=True)
    df.to_csv(f'{transform_dir}/{file}.csv', index=False)
    
    logger.info(f"transform success")

if __name__ == "__main__":
    transform(execution_date=args.execution_date)