import json
import requests
import urllib.parse
import pandas as pd
from d2b_data.verbose_logger import Verbose
import time
import random
import os
import webbrowser

class TikTokMarketing():
    def __init__(self, token_path: str | None = None, verbose: bool = True):
        self.endpoint_base = "https://business-api.tiktok.com/open_api/v1.3/"
        self.token_path = token_path if token_path else "token_tiktok.json"
        self.token = None
        self.app_id = None
        self.secret = None
        self.headers = {"Content-Type": "application/json"}

        self.verbose = Verbose(
            active=verbose,
            alerts_enabled=False,
            workflow_name="TikTokMarketing"
        )

        if os.path.isfile(self.token_path):
            self._load_token_from_file()
            if self.token:
                # Testing connection (currently returns True via pass/None logic)
                if self._token_test_connection():
                    self.headers["Access-Token"] = self.token
                    self.verbose.log(f"TikTok Class instantiated with token from {self.token_path}")
                else:
                    self.verbose.log("Invalid token. New token must be generated")
                    self.token = None
        else:
            self.verbose.log(f"No token file found at {self.token_path}. New token must be generated")

    def _load_token_from_file(self):
        """Reads token and credentials from file"""
        try:
            with open(self.token_path, 'r') as file:
                token_data = json.load(file)
                
            self.app_id = token_data.get("app_id")
            self.secret = token_data.get("secret") # Fixed: was getting app_id
            self.token = token_data.get("access_token")
            return token_data
        except Exception as e:
            self.verbose.log(f"Error loading token from file: {e}")
            return None

    def _token_test_connection(self):
        """Placeholder for connection testing logic"""
        # For now, we return True to allow the __init__ to proceed
        return True

    def get_access_token(self, app_id: str, secret: str, auth_code: str):
        """Exchanges auth_code for access_token and saves to JSON"""
        url = f"{self.endpoint_base}oauth2/access_token/"
        payload = {
            "app_id": app_id,
            "secret": secret,
            "auth_code": auth_code
        }
        
        response = requests.post(url, json=payload)
        res_json = response.json()

        if res_json.get("code") == 0:
            token_data = res_json.get('data', {})
            
            # Prepare data for persistence
            save_data = {
                "access_token": token_data.get("access_token"),
                "app_id": app_id,
                "secret": secret,
                "scope": token_data.get("scope"),
                "advertiser_ids": token_data.get("advertiser_ids")
            }
            
            self.app_id = app_id
            self.secret = secret
            self.token = token_data.get("access_token")
            self.headers["Access-Token"] = self.token
            
            try:
                with open(self.token_path, 'w') as f:
                    json.dump(save_data, f, indent=4)
                self.verbose.log(f"Token saved successfully as {self.token_path}")
            except Exception as e:
                self.verbose.log(f"Error saving token to file: {e}")

            return token_data
        else:
            self.verbose.log(f"Error obtaining token: {res_json.get('message')}")
            return None

    def get_authorized_advertisers(self, app_id: str | None = None, secret: str | None = None):
        """Returns a list of advertisers accessible by the current token"""
        url = f"{self.endpoint_base}oauth2/advertiser/get/"

        # Update credentials if provided
        if app_id and secret:
            self.app_id = app_id
            self.secret = secret

        if not self.app_id or not self.secret:
            self.verbose.log("Missing app_id or secret. Cannot retrieve accounts")
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
        """Low level internal API call handler"""
        url = f"{self.endpoint_base}report/integrated/get/"

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params)

                if response.status_code == 429:
                    wait_time = (2 ** attempt) + random.random()
                    self.verbose.log(f"Rate limit exceeded. Retrying in {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                data = response.json()

                if data.get("code") != 0:
                    self.verbose.log(f"TikTok API Error: {data.get('message')} (Code: {data.get('code')})")
                    return None

                return data

            except Exception as e:
                self.verbose.log(f"Request failed: {e}")
                return None
        return None

    def get_report_json(self, params: dict, max_retries: int = 5):
        """Public method to retrieve raw JSON data for debugging"""
        return self._get_report_raw(params, max_retries)

    def get_report_dataframe(self, advertiser_id: str, start_date: str, end_date: str, dimensions: list, metrics: list, data_level: str = "AUCTION_AD"):
        """Extracts data and converts it into a formatted Pandas DataFrame"""
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        current_start = start_dt
        all_records = []

        while current_start <= end_dt:
            current_end = min(current_start + pd.Timedelta(days=29), end_dt)
            self.verbose.log(f"Extracting {current_start.date()} to {current_end.date()}")

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

                if not data or "list" not in data.get("data", {}):
                    break

                all_records.extend(data["data"]["list"])

                total_page = data.get("data", {}).get("page_info", {}).get("total_page", 1)
                if page < total_page:
                    page += 1
                else:
                    break

            current_start = current_end + pd.Timedelta(days=1)

        if all_records:
            df = pd.json_normalize(all_records)
            df.columns = [col.split('.')[-1] for col in df.columns]
            for col in metrics:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            self.verbose.log(f"Successfully extracted {len(df)} rows")
            return df
        
        self.verbose.log("No data found for the specified period")
        return pd.DataFrame()
