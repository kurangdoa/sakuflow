import inspect
import os
import logging

class logger:

    def __init__(self, caller_path):
        env = os.getenv('env')

        # prepare log path
        caller_path = caller_path
        _, caller_file = os.path.split(caller_path)
        caller_dir = os.path.dirname(caller_path)
        logging_dir = caller_dir.replace('/datasaku/sakuflow', f'/logs/{env}')
        logging_path = os.path.join(logging_dir, caller_file.replace('.py', '.log'))

        # logging
        os.makedirs(logging_dir, exist_ok=True)
        logging.basicConfig(filename=logging_path, encoding='utf-8', level=logging.DEBUG)
        logger = logging.getLogger("datasaku_flow_logger")
        logger.setLevel(logging.DEBUG)

        # stream to console
        if os.getenv('is_show_log_in_terminal'):
            console = logging.StreamHandler()
            logger.addHandler(console)

        # self
        self.logger = logger