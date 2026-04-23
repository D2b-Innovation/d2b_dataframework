import json
import requests
import urllib.parse
import pandas as pd
import copy
import time
import random
import os
import webbrowser
# test de commit fuera
from d2b_data.verbose_logger import Verbose

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
            workflow_name="TikTokMarketing class"
        )

        if os.path.isfile(self.token_path):
            self._load_token_from_file()
            if self.token:
                if self._token_test_connection():
                    self.headers["Access-Token"] = self.token
                    self.verbose.log(f"TikTok Class instantiated with token from {self.token_path}, ready to retrieve data.")
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

            if not all(k in token_data for k in ("app_id", "secret", "access_token")):
                self.verbose.log("Token file is missing required fields (app_id, secret, access_token)")
                return None    
            self.app_id = token_data.get("app_id")
            self.secret = token_data.get("secret")
            self.token = token_data.get("access_token")
            return token_data
        except Exception as e:
            self.verbose.log(f"Error loading token from file: {e}")
            return None

    def _token_test_connection(self) -> bool:
        """
        Validates the token against the API.
        """
        url = f"{self.endpoint_base}oauth2/advertiser/get/"
        if not self.app_id or not self.secret or not self.token:
            return False
        
        params = {
            "app_id": self.app_id,
            "secret": self.secret
        }

        test_headers = {
            "Access-Token": self.token,
            "Content-Type": "application/json", 
        }

        try:
            connection_test_response = requests.get(url, headers=test_headers, params=params)
            connection_test_response_json = connection_test_response.json()
            return connection_test_response_json.get("code") == 0
        except Exception as e:
            self.verbose.log(f"Error during connection test: {e}")
            return False

    def get_access_token(self, app_id: str, secret: str, auth_code: str | None = None, redirect_uri: str = "https://tiktok.cl"):
        """Exchanges auth_code for access_token and saves to JSON. Interactively prompts if auth_code is missing."""
        if not auth_code:
                auth_url = f"https://business-api.tiktok.com/portal/auth?app_id={app_id}&state=auth_request&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
                self.verbose.log("Se requiere autorización manual. Mostrando instrucciones al usuario...")
                print("\n" + "="*60)
                print("TIKTOK AUTHENTICATION REQUIRED")
                print("1. Open the following URL in your browser to authorize the app:")
                print(f"\n{auth_url}\n")
                try:
                    webbrowser.open(auth_url)
                except Exception as e:
                    self.verbose.log(f"Couldn't open browser {e}")   
                
                print("\n2. Authorize the application. You will be redirected to a new URL (your callback).")
                print("3. Look for the parameter '?auth_code=XXXXXXXXX' in the address bar of that new URL.")    
                auth_code = input("\nPaste only the 'auth_code' value here and press Enter: ").strip()
                print("="*60 + "\n")

                if not auth_code:
                        self.verbose.log("No code provided, aborting authentication process")

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
            
            save_data = {
                "access_token": token_data.get("access_token"),
                "app_id": app_id,
                "secret": secret,
                "scope": token_data.get("scope")
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

            if self._token_test_connection():
                self.verbose.log("Token test connection succesfull")
                return 200
            else:
                self.verbose.log("New token generated but failed connection test. Must be checked")
            return 200
        else:
            self.verbose.log(f"Error obtaining token: {res_json.get('message')}")
            return True
    # Se debe implementar el input en el auth code

    def get_authorized_advertisers(self, app_id: str | None = None, secret: str | None = None):
        """Public method that returns a list of advertisers accessible by the current token"""
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
        # Revisar parámetros para evitar el json.dumps.
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
        """Public method to retrieve raw JSON data with date chunking and pagination for debugging"""
        
        if "start_date" not in params or "end_date" not in params:
            self.verbose.log("No start_date or end_date found in params. Making a direct request. IT MUST NOT EXCEED 365 DAYS PERIOD")
            return self._get_report_raw(params, max_retries)

        start_dt = pd.to_datetime(params["start_date"])
        end_dt = pd.to_datetime(params["end_date"])
        current_start = start_dt
        all_records = []

        # Usamos una copia para no mutar el diccionario original que pasa el usuario
        request_params = copy.deepcopy(params)

        for key in ["metrics", "dimensions", "filtering"]:
            if key in request_params and isinstance(request_params[key], list):
                request_params[key] = json.dumps(request_params[key])

        while current_start <= end_dt:
            # Límite de 30 días por petición (current_start + 29 días)
            current_end = min(current_start + pd.Timedelta(days=29), end_dt)
            self.verbose.log(f"Extracting JSON {current_start.date()} to {current_end.date()}")

            request_params["start_date"] = current_start.strftime('%Y-%m-%d')
            request_params["end_date"] = current_end.strftime('%Y-%m-%d')
            
            page = 1
            while True:
                request_params["page"] = page
                
                # Pasamos tanto los parámetros actualizados como max_retries
                data = self._get_report_raw(request_params, max_retries)

                if not data or "list" not in data.get("data", {}):
                    break

                # Acumulamos los registros de esta página/bloque de fechas
                all_records.extend(data["data"]["list"])

                total_page = data.get("data", {}).get("page_info", {}).get("total_page", 1)
                if page < total_page:
                    page += 1
                else:
                    break

            # Avanzamos al siguiente bloque de fechas
            current_start = current_end + pd.Timedelta(days=1)

        if not all_records:
            self.verbose.log("No raw JSON data found for the specified period")
            return {}

        self.verbose.log(f"Successfully extracted {len(all_records)} total raw records")
        
        # Reconstruimos la estructura JSON original consolidando todos los registros
        return {
            "data": {
                "list": all_records
            }
        }
    

    def get_report_dataframe(self, advertiser_id: str, start_date: str, end_date: str, dimensions: list, metrics: list, data_level: str = "AUCTION_AD"):
        """Extracts data and converts it into a formatted Pandas DataFrame"""
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        days_diff = (end_dt - start_dt).days
        
        # Whenever we want more complex queries, by default we use get_report_json
        if "stat_time_day" not in dimensions:
            if days_diff > 365:
                raise ValueError("Cannot retrieve more than 365 days when query acumulated data")
            
            all_records = []
            page = 1
            while True:
                
                params = {
                        "advertiser_id": advertiser_id,
                        "service_type": "AUCTION",
                        "report_type": "BASIC",
                        "data_level": data_level,
                        "start_date": start_dt.strftime('%Y-%m-%d'),
                        "end_date": end_dt.strftime('%Y-%m-%d'),
                        "metrics": json.dumps(metrics),
                        "dimensions": json.dumps(dimensions),
                        "page_size": 1000,
                        "page": page
                    }

                data = self._get_report_raw(params)

                if data == None:
                    raise RuntimeError("Critical error found while downloading data")
                
                if not data or "list" not in data.get("data", {}):
                    break

                all_records.extend(data["data"]["list"])

                total_page = data.get("data", {}).get("page_info", {}).get("total_page", 1)
                if page < total_page:
                    page += 1
                else:
                    break

            if all_records:    
                df = pd.json_normalize(all_records)
                df.columns = [col.split('.')[-1] for col in df.columns]
                self.verbose.log(f"Successfully extracted {len(df)} rows")
                return df
            
            self.verbose.log("No data found for this advertiser in the selected range.")
            return pd.DataFrame()
        
        else:
            all_records = []
            current_start = start_dt
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
                        "page": page,
                        "query_lifetime": False 
                    }

                    data = self._get_report_raw(params)

                    if data == None:
                        raise RuntimeError("Critical error found while downloading data")

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
                # Revisar si necesitamos eliminar los 0s
                df = df.sort_values(by=['ad_id', 'stat_time_day']).reset_index(drop=True)
                self.verbose.log(f"Successfully extracted {len(df)} rows")
                return df
            
            self.verbose.log("No data found for the specified period")
            return pd.DataFrame()




