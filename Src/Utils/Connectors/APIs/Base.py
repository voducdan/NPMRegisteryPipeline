import requests

class APIRequest():

    def __init__(self, base_url):

        self.base_url = base_url
        self.session = requests.Session()

    def get(self, url, params=None, headers=None):
        if not url:
            return None
        if headers:
            self.session.headers.update(headers)
        response = self.session.get(url, params=params)
        return response