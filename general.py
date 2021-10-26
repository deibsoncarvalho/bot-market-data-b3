import os
import pandas as pd

import logging
from datetime import datetime


PANDAS_SEP = ";"
PANDAS_ENCODING = 'latin1'


LOGS_PATH = os.path.join(os.path.dirname(__file__), 'logs')
DATA_PATH = os.path.join(os.path.dirname(__file__), 'data')
TEMP_PATH = os.path.join(os.path.dirname(__file__), 'temp')

folders = [LOGS_PATH, DATA_PATH, TEMP_PATH]

for folder in folders:
    if not os.path.isdir(folder):    
        os.mkdir(folder)
    

HOLLIDAYS = {2020: [datetime(2020, 1, 1), datetime(2020, 1, 25), datetime(2020, 2, 24), datetime(2020, 2, 25),
                    datetime(2020, 4, 10), datetime(2020, 4, 21), datetime(2020, 5, 1), datetime(2020, 6, 11),
                    datetime(2020, 7, 9), datetime(2020, 9, 7), datetime(2020, 10, 12), datetime(2020, 11, 2),
                    datetime(2020, 11, 20), datetime(2020, 12, 24), datetime(2020, 12, 25), datetime(2020, 12, 31)],

             2021: [datetime(2021, 1, 1), datetime(2021, 1, 25), datetime(2021, 2, 15), datetime(2021, 2, 16),
                    datetime(2021, 4, 2), datetime(2021, 4, 21), datetime(2021, 5, 1), datetime(2021, 6, 3),
                    datetime(2021, 7, 9), datetime(2021, 9, 7), datetime(2021, 10, 12), datetime(2021, 11, 2),
                    datetime(2021, 11, 15), datetime(2021, 11, 20), datetime(2021, 12, 24), datetime(2021, 12, 25),
                    datetime(2021, 12, 31)],

             2022: [datetime(2021, 1, 1), datetime(2021, 1, 25), datetime(2021, 2, 28), datetime(2021, 3, 1),
                    datetime(2021, 4, 15), datetime(2021, 4, 21), datetime(2021, 5, 1), datetime(2021, 6, 16),
                    datetime(2021, 7, 9), datetime(2021, 9, 7), datetime(2021, 10, 12), datetime(2021, 11, 2),
                    datetime(2021, 11, 15), datetime(2021, 11, 20), datetime(2021, 12, 24), datetime(2021, 12, 25),
                    datetime(2021, 12, 30), datetime(2021, 12, 31)],
             }

TIME_START_TRADING = datetime(year=1970, month=1, day=1, hour=9, minute=0)
TIME_FINISH_TRADING = datetime(year=1970, month=1, day=1, hour=18, minute=0)


class ChainedAssignent:
    """ Context manager to temporarily set pandas chained assignment warning. Usage:

        with ChainedAssignment():
             blah

        with ChainedAssignment('error'):
             run my code and figure out which line causes the error!

    """

    def __init__(self, chained=None):
        acceptable = [None, 'warn', 'raise']
        assert chained in acceptable, "chained must be in " + str(acceptable)
        self.swcw = chained

    def __enter__(self):
        self.saved_swcw = pd.options.mode.chained_assignment
        pd.options.mode.chained_assignment = self.swcw
        return self

    def __exit__(self, *args):
        pd.options.mode.chained_assignment = self.saved_swcw


class Logging:

    def __init__(self,
                 logger_name: str,
                 verbose: bool = False):

        self._name = logger_name

        self._logger = logging.getLogger(logger_name)

        log_format_file = f"[%(asctime)s - %(threadName)s - %(levelname)s]  %(message)s"
        log_formatter = logging.Formatter(log_format_file)
        file_log = os.path.join(LOGS_PATH, f'{logger_name}.log')
        file_handler = logging.FileHandler(file_log)
        file_handler.setFormatter(log_formatter)
        self._logger.addHandler(file_handler)

        log_format = f"[%(asctime)s - %(threadName)s - %(levelname)s]  ({self._name}) -> %(message)s"
        console_formatter = logging.Formatter(log_format)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        self._logger.addHandler(console_handler)
        if verbose:
            self._logger.setLevel(logging.INFO)
        else:
            self._logger.setLevel(logging.ERROR)

    def info(self, msg: str):
        """ Method for register log in file and Stream console """
        self._logger.info(msg)

    def error(self, msg: str):
        """ Method for register log in file and Stream console """
        self._logger.error(msg)
