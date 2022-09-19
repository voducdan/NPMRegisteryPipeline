from os import path
import logging.config

from Utils.Connectors.APIs.Base import APIRequest

from Utils.paging import get_skip_items_by_offset, has_next , get_page_by_offset
from Utils.data_extractor import DataExtractor
from Utils.date import DateUtils
from Utils.constants import Constant

log_file_path = path.join(path.dirname(
    path.abspath(f'../../{__file__}')), 'LogHandler/logging.ini')
logging.config.fileConfig(log_file_path,
                          disable_existing_loggers=False)
logger = logging.getLogger(__name__)
class NpmRegistry(APIRequest):
    def __init__(self, rows_per_page = 1000):
        super.__init__(base_url=Constant.NPM_REGISTRY_URL)
        self.rows_per_page = rows_per_page

    def make_api_request_and_save_to_lake(self, sub_url, field):
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
                self.response = self.get(f'{self.url}/{sub_url}', params)
                status = self.response.status_code
                if not status == 200:
                    logger.info(f"Error when call api, error message: {self.response.text_content}")
                    return 
                data_dict = DataExtractor.response_json_to_dict(self.response)
                data_to_load = data_dict[field] if field else data_dict
                output_uri = DateUtils.today_to_url('npm_registry/{sub_url}')
                DataExtractor.dict_to_json_file(content_json=data_to_load, output_uri=output_uri)
                has_next_page = has_next(page, data_dict['total_rows'], rows_per_page)
                offset = data_dict['offset']
            except ValueError as e:
                pass
                logger.error(e)