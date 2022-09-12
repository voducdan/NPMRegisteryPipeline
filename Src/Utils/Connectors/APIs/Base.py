import json
import requests

class APIRequest():

    def __init__(self, url):

        self.url = url
        self.session = requests.Session()

    def get(self, params=None, headers=None):
        if headers:
            self.session.headers.update(headers)
        response = self.session.get(self.url, params=params)
        return response

    def handle_response(self):
        text_content = self.response.text_content
        return json.loads(text_content)

