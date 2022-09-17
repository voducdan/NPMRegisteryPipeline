import logging

from Utils.Connectors.APIs.Base import APIRequest

from Utils.paging import get_skip_items_by_offset, has_next , get_page_by_offset
from Utils.data_extractor import DataExtractor
from Utils.date import DateUtils
class NpmRegistry(APIRequest):
    def __init__(self, rows_per_page = 100):
        super.__init__()
        self.rows_per_page = rows_per_page

    def get_request(self):
        offset = 0
        has_next_page = True
        rows_per_page = self.rows_per_page
        while has_next_page:
            try:
                page = get_page_by_offset(offset, rows_per_page)
                params = {
                    'limit':rows_per_page,
                    'skip': get_skip_items_by_offset(page, rows_per_page)
                }
                self.response = self.get(self.url, params)
                status = self.response.status_code
                if not status == 200:
                    # Logging
                    return 
                data_dict = DataExtractor.response_json_to_dict(self.response)
                output_uri = DateUtils.today_to_url()
                DataExtractor.dict_to_json_file(content_json=data_dict, output_uri=output_uri)
                has_next_page = has_next(page, data_dict['total_rows'], rows_per_page)
            except ValueError as e:
                logging.error(e)