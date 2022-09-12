import logging
import os
import time
from datetime import date

from APIs.Base import APIRequest

from Utils.paging import get_skip_items_by_offset, has_next , get_page_by_offset

class NpmRegistry(APIRequest):
    def __init__(self, rows_per_page = 100):
        super.__init__()
        self.rows_per_page = rows_per_page

    def get_request(self):
        offset = 0
        has_next_page = True
        rows_per_page = self.rows_per_page
        while has_next_page:
            page = get_page_by_offset(offset, rows_per_page)
            params = {
                'limit':rows_per_page,
                'skip': get_skip_items_by_offset(page, rows_per_page)
            }
            self.response = self.get(self.url, params)
            has_next_page = has_next(page, self.total_rows, rows_per_page)
            try:
                self.handle_response()
            except ValueError as e:
                logging.error(e)

    def handle_response(self):
        status = self.response.status_code
        try:
            if status == 200:
                content_text = self.response.text_content
                todays_date = date.today()
                curr_path = os.path.abspath(os.curdir)
                date_path = str(todays_date).replace('-','/')
                lake_path =  os.path.join('../../../../',curr_path, date_path)
                lake_file = f'{lake_path}/{curr_time}.json'
                print(lake_path)
                curr_time = round(time.time()*1000)
                with open(lake_file,'w+') as f:
                    f.write(content_text)
        except Exception as e:
            raise ValueError(e)