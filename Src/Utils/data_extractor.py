import json
import pandas as pd

from date import DateUtils

class DataExtractor():
    def __init__(self, date_output_format=DateUtils.DateFormat.YMD):
        self.date_output_format = date_output_format

    def response_json_to_dict(self, response):
        try:
            content_text = response.text_content
            content_json = json.loads(content_text)
            return content_json
        except Exception as e:
            raise ValueError(e)

    def dict_to_dataframe(obj, columns=None):
        df = pd.DataFrame([obj])
        if columns:
            df.columns = columns
        return df

    def dict_to_json_file(self, response, output_uri, compression=None):
        try:
            content_json = self.response_json_to_dict(response)
            if not output_uri:
                raise AttributeError('output uri can not be null')
            curr_time = DateUtils.get_current_time_ms()
            uri = f'{output_uri}/{curr_time}.json'
            if compression:
                uri += f'.{compression}'
            with open(uri, 'w+') as f:
                f.write(content_json)
        except Exception as e:
            raise ValueError(e)