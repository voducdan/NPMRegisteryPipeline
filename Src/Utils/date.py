from os import path
import time
from datetime import date
from pathlib import Path
import logging
import logging.config
import traceback

log_file_path = path.join(path.dirname(
    path.abspath(__file__)), 'LogHandler/logging.ini')
logging.config.fileConfig(log_file_path,
                          disable_existing_loggers=False)
logger = logging.getLogger(__name__)


class DateUtils():
    def __init__():
        pass

    class DateFormat():
        YMD = '%Y-%m-%d'
        YMD_NODASH = '%Y%m%d'

    @staticmethod
    def today_to_url(folder_name):
        try:
            todays_date = date.today()
            curr_path = Path.cwd()
            root_path = curr_path.parent.parent
            date_path = str(todays_date).replace('-', '/')
            lake_path = path.join(root_path, 'Lake', folder_name, date_path)
            return lake_path
        except TypeError as e:
            pass
            logger.error('folder name must be a string')
            logger.error(e, exc_info=True)
        except:
            logger.error('Uncatch exception: %s', traceback.format_exc())
            return None

    @staticmethod
    def get_current_time_ms():
        return round(time.time()*1000)
