from dotenv import load_dotenv
import inspect
import os
import yaml
import sys
import argparse
from util.logging.logging import logger

def init():

    # get caller path
    caller_path = inspect.stack()[1].filename
    caller_dir = os.path.dirname(caller_path)
    caller_parent_dir = os.path.dirname(caller_dir)

    # get environment
    dotenv_path = os.path.join(caller_parent_dir, '.env')
    load_dotenv(dotenv_path)

    assert os.getenv('env'), f"please specify env variable inside {dotenv_path}"
    log = logger(caller_path=caller_path).logger

    # open config file
    with open(os.path.join(caller_parent_dir, 'config.yaml')) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            log.warning(f"could not open config file -- {exc}")

    # take argument
    args = sys.argv
    parser = argparse.ArgumentParser()
    parser.add_argument('--execution_date')
    args = parser.parse_args()

    return log, config, args