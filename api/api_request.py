# api/api_request.py

import requests

class APIRequest:
    def __init__(self, base_url="https://api.upbit.com/v1", headers=None):
        self.base_url = base_url.rstrip("/")
        self.headers = headers if headers else {"accept": "application/json"}

    def get(self, path: str, params: dict = None):
        url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"[API GET Error] {url} :: {e}")
            return None

    def post(self, path: str, data: dict = None):
        url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"
        try:
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"[API POST Error] {url} :: {e}")
            return None
