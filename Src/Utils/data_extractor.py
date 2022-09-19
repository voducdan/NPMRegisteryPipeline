import os
from pathlib import Path
import json
import pandas as pd

from date import DateUtils

class DataExtractor():
    def __init__(self, date_output_format=DateUtils.DateFormat.YMD):
        self.date_output_format = date_output_format

    def response_json_to_dict(self, response):
        try:
            content_text = response.text
            content_json = json.loads(content_text)
            return content_json
        except Exception as e:
            raise ValueError(e)

    def dict_to_dataframe(obj,columns=None):
        df = pd.DataFrame([obj])
        if columns:
            df.columns = columns
        return df

    def dict_to_json_file(self, content_json, output_uri, compression=None):
        try:
            if not output_uri:
                raise AttributeError('output uri can not be null')
            curr_time = DateUtils.get_current_time_ms()
            uri = f'{output_uri}/{curr_time}.json'
            if compression:
                uri += f'.{compression}'
            if not os.path.exists(output_uri):
                path = Path(output_uri)
                path.mkdir(parents=True)
            with open(uri, 'w+') as f:
                content_dump = json.dumps(content_json)
                f.write(content_dump)
        except Exception as e:
            raise ValueError(e)