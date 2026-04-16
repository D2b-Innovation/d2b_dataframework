import json
import requests
import urllib.parse
import pandas as pd
from d2b_data.verbose_logger import Verbose
import time
import random

class TikTokMarketing():
    def __init__(self, token: str | None, verbose: bool = True):
        self.token = token
        self.endpoint_base = "https://business-api.tiktok.com/open_api/v1.3/"

        self.verbose = Verbose(
            active=verbose,
            alerts_enabled=False,
            workflow_name="TikTokMarketing"
        )

        self.headers = {
            "Access-Token": self.token,
            "Content-Type": "application/json"
        }

        self.verbose.log("Tiktok Class instanciated with token")

    def get_access_token(self, app_id: str, secret: str, auth_code: str):
        """Intercambia el auth_code por un access_token"""

        url = f"{self.endpoint_base}oauth2/access_token/"
        payload = {
            "app_id": app_id,
            "secret": secret,
            "auth_code": auth_code
        }
        response = requests.post(url, json=payload)
        data = response.json()

        if data.get("code") == 0:
            self.app_id = app_id
            self.secret = secret
            self.token = data['data']['access_token']
            self.headers["Access-Token"] = self.token
            self.verbose.log(" Token obtenido y actualizado en la clase.")
            return data['data']
        else:
            self.verbose.log(f" Error obteniendo token: {data.get('message')}")
            return None

    def get_authorized_advertisers(self, app_id: str | None = None, secret: str | None = None):
        """Returns a list with the advertisers that the token has access to"""
        url=f"{self.endpoint_base}oauth2/advertiser/get/"

        if app_id and secret:
            self.app_id = app_id
            self.secret = secret

        if not self.app_id or not self.secret:
            self.verbose.log(" You must provide add_id or secret to retrieve accounts")
            return []    
        
        params = {
            "app_id": self.app_id,
            "secret": self.secret
        }

        response = requests.get(url, headers=self.headers, params=params)
        data = response.json()
        if data.get("code") == 0:
            return data.get('data', {}).get('list', [])
        return []

    def _get_report_raw(self, params: dict, max_retries: int = 5):
        """Low level calling API method"""

        url = f"{self.endpoint_base}report/integrated/get/"

        self.verbose.log(f" Calling the TikTok API v1.3 for dates:{params.get('start_date')} -> {params.get('end_date')}")


        for attempt in range(max_retries):
            try:
              self.verbose.log(f" Calling TikTok API: {params.get('start_date')} -> {params.get('end_date')} (Intento {attempt+1})")
              response = requests.get(url, headers=self.headers, params=params)

              if response.status_code == 429:
                  wait_time = (2 ** attempt) + random.random()
                  self.verbose.log(f"Rate limit exceeded. Waiting for {wait_time} seconds before retrying.")
                  time.sleep(wait_time)
                  continue

              response.raise_for_status()
              data = response.json()

              if data.get("code") != 0:
                self.verbose.log(f"Error en API de TikTok: {data.get('message')} (Code: {data.get('code')})")
                return None

              return data

            except requests.exceptions.HTTPError as e:
                  self.verbose.log(f"HTTP Error: {e}")
                  return None
            except Exception as e:
              self.verbose.log(f"Error calling the API: {e}")
              return None


    def get_report_dataframe(self, advertiser_id: str, start_date: str, end_date: str, dimensions: list, metrics: list, data_level: str = "AUCTION_AD"):
      """Constructs the params for _get_report_raw and transforms to Pandas DataFrame"""

      start_dt = pd.to_datetime(start_date)
      end_dt = pd.to_datetime(end_date)

      current_start = start_dt
      all_dataframes = []

      while current_start <= end_dt:
            current_end = min(current_start + pd.Timedelta(days=29), end_dt)
            self.verbose.log(f" Extracting data from date: {current_start} to date: {current_end}")

            page = 1
            while True:
                  params = {
                      "advertiser_id": advertiser_id,
                      "service_type": "AUCTION",
                      "report_type": "BASIC",
                      "data_level": data_level,
                      "start_date": current_start.strftime('%Y-%m-%d'),
                      "end_date": current_end.strftime('%Y-%m-%d'),
                      "metrics": json.dumps(metrics),
                      "dimensions": json.dumps(dimensions),
                      "page_size": 1000,
                      "page": page
                      }

                  data = self._get_report_raw(params)

                  if data is None:
                     return pd.DataFrame()

                  if data and "list" in data.get("data", {}):
                     all_dataframes.extend(data["data"]["list"])

                  total_page = data.get("data", {}).get("page_info", {}).get("total_page", 1)
                  if page < total_page:
                      page += 1
                  else:
                      break

            current_start = current_end + pd.Timedelta(days=1)

      if all_dataframes:
          result_df = pd.json_normalize(all_dataframes)
          result_df.columns = [col.split('.')[-1] for col in result_df.columns]
          for col in metrics:
              result_df[col] = pd.to_numeric(result_df[col], errors="coerce")
          self.verbose.log(f"Total rows extracted {len(result_df)}")
          return result_df
      else:
          self.verbose.log("No data was extracted")
          return pd.DataFrame()

    def get_report_json(self, params: dict, max_retries: int = 5):
        """Low level calling API method"""

        url = f"{self.endpoint_base}report/integrated/get/"

        self.verbose.log(f" Calling the TikTok API v1.3 for dates:{params.get('start_date')} -> {params.get('end_date')}")


        for attempt in range(max_retries):
            try:
              self.verbose.log(f" Calling TikTok API: {params.get('start_date')} -> {params.get('end_date')} (Intento {attempt+1})")
              response = requests.get(url, headers=self.headers, params=params)

              if response.status_code == 429:
                  wait_time = (2 ** attempt) + random.random()
                  self.verbose.log(f"Rate limit exceeded. Waiting for {wait_time} seconds before retrying.")
                  time.sleep(wait_time)
                  continue

              response.raise_for_status()
              data = response.json()

              if data.get("code") != 0:
                self.verbose.log(f"Error en API de TikTok: {data.get('message')} (Code: {data.get('code')})")
                return None

              return data

            except requests.exceptions.HTTPError as e:
                  self.verbose.log(f"HTTP Error: {e}")
                  return None
            except Exception as e:
              self.verbose.log(f"Error calling the API: {e}")
              return None

# Token de autorización similar a el ga4 (casilla con input para que usuario llene y entregue la token)
# Implementar un get_report_raw en estado json expuesto público (para debugeo)