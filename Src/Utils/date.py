import os
import time
from datetime import date

class DateUtils():
    def __init__():
        pass

    class DateFormat():
        YMD = '%Y-%m-%d'
        YMD_NODASH = '%Y%m%d'
            
    @staticmethod
    def today_to_url():
        todays_date = date.today()
        curr_path = os.path.abspath(os.curdir)
        date_path = str(todays_date).replace('-','/')
        lake_path =  os.path.join('../../', curr_path, date_path)
        return lake_path
    
    @staticmethod
    def get_current_time_ms():
        return round(time.time()*1000)