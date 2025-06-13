# Pega este código completo en tu archivo d2b_data/Linkedin_Marketing.py

import pandas as pd
import json
import requests
from os.path import exists
from requests.structures import CaseInsensitiveDict
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

class Linkedin_Marketing():
  def __init__(self, APPLICATON_KEY, APPLICATON_SECRET):
    self.APPLICATON_KEY    = APPLICATON_KEY
    self.APPLICATON_SECRET = APPLICATON_SECRET
    self.RETURN_URL        = 'https://localhost:8888'
    self.SCOPE             = ['r_organization_social', 'r_ads_reporting', 'r_basicprofile', 'r_ads', 'w_member_social', 'w_organization_social']
    self.SCOPE_str         = "%20".join(self.SCOPE)
    self.OAUTH_URL         = f'https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id={self.APPLICATON_KEY}&redirect_uri={self.RETURN_URL}&state=foobar&scope={self.SCOPE_str}'
    self.HEADERS           = None
    self.token             = None
    self.verbose_logger    = None

  def set_headers_token(self, token_header):
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Bearer {token_header}"
    self.HEADERS = headers
    return self.HEADERS

  def get_token(self, filename=None):
    if filename is not None and exists(filename):
      with open(filename) as json_file:
        json_content = json.load(json_file)
      access_token = json_content.get('access_token')
      self.set_headers_token(access_token)
      if self.verbose_logger:
        self.verbose_logger.log("Token cargado desde archivo")
      return self.HEADERS
    raise ValueError(f"No se pudo obtener el token desde el archivo: {filename}")

  def get_report(self, account_id, start, end, metrics, pivot=None, time_granularity="DAILY"):
    account_id_encoded = f"urn%3Ali%3AsponsoredAccount%3A{account_id}"
    start_parts = start.split("-")
    end_parts   = end.split("-")
    start_url = f"&dateRange.start.day={start_parts[2]}&dateRange.start.month={start_parts[1]}&dateRange.start.year={start_parts[0]}"
    end_url   = f"&dateRange.end.day={end_parts[2]}&dateRange.end.month={end_parts[1]}&dateRange.end.year={end_parts[0]}"
    
    if pivot and "pivot" not in metrics:
        metrics += ",pivot,pivotValues"
    
    pivot_text = ''
    if pivot:
      for idx, val in enumerate(pivot.split(",")):
        pivot_text += f"pivots[{idx}]={val.strip()}&"

    url = (f'https://api.linkedin.com/v2/adAnalyticsV2?q=statistics&{pivot_text}'
           f'timeGranularity={time_granularity}{start_url}{end_url}&accounts={account_id_encoded}&fields={metrics}')
    
    if self.verbose_logger: self.verbose_logger.log(f"Ejecutando GET a URL de LinkedIn: {url}")
        
    res = requests.get(url, headers=self.HEADERS)
    if res.status_code != 200:
        error_content = res.content.decode()
        if self.verbose_logger: self.verbose_logger.log(f"Error en respuesta de API: {res.status_code} - {error_content}")
        raise Exception(f"Error en API de LinkedIn: {error_content}")
    return res.content

  def get_report_dataframe(self, account_id, start, end, metrics, unsampled=False, **kwargs):
    if unsampled:
        date_range = pd.date_range(start, end, freq='D')
        array_reports = []
        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            res = self.get_report(account_id, date_str, date_str, metrics, **kwargs)
            df = self._clean_and_transform_dataFrame(res, date_str=date_str)
            if not df.empty: array_reports.append(df)
        return pd.concat(array_reports, ignore_index=True) if array_reports else pd.DataFrame()
    else:
        res = self.get_report(account_id, start, end, metrics, **kwargs)
        return self._clean_and_transform_dataFrame(res)

  def _clean_and_transform_dataFrame(self, res, date_str=None):
    if isinstance(res, bytes): res = json.loads(res.decode("utf-8"))
    DF = pd.json_normalize(res.get("elements"), sep="_")
    if DF.empty: return DF

    if date_str: DF["date"] = date_str
    if "date" in DF.columns: DF["date"] = pd.to_datetime(DF["date"])

    if 'adentities' in DF.columns: DF['adentities'] = DF['adentities'].apply(lambda x: json.dumps(x) if pd.notna(x) else None)

    DF.columns = (DF.columns.str.strip().str.lower().str.replace(" ", "_", regex=False).str.replace("-", "_", regex=False).str.replace(r"[^\w]", "", regex=True))
    if self.verbose_logger: self.verbose_logger.log(f"DataFrame crudo pero limpio generado. Columnas: {DF.columns.tolist()}")
    return DF

  def upload_to_bigquery(self, df, bq_config, credentials_info, schema):
    logger = self.verbose_logger
    if df.empty:
        logger.log("DataFrame vacío, no se sube nada a BigQuery.")
        return

    project_id = bq_config["project-id"]
    dataset = bq_config["dataset"]
    table_prefix = bq_config["table-prefix"]
    destination_table = f"{dataset}.{table_prefix}"
    
    logger.log(f"Iniciando subida de {df.shape[0]} filas a la tabla particionada: {project_id}.{destination_table}")
    
    try:
        credentials_gbq = service_account.Credentials.from_service_account_info(credentials_info)
        to_gbq(
            dataframe=df, destination_table=destination_table, project_id=project_id,
            credentials=credentials_gbq, if_exists="append", table_schema=schema,
            progress_bar=False, api_method='load_csv'
        )
        logger.log("Subida a BigQuery completada exitosamente.")

        try:
            bq_client = bigquery.Client(project=project_id, credentials=credentials_gbq)
            table_ref = bq_client.get_table(destination_table)
            expiration_date = datetime.utcnow() + timedelta(days=1096)
            table_ref.expires = expiration_date
            bq_client.update_table(table_ref, ["expires"])
            logger.log("Expiración de la tabla actualizada.")
        except Exception as e_expires:
            logger.log(f"ADVERTENCIA: No se pudo actualizar la expiración. Error: {e_expires}")
    except Exception as e_upload:
        logger.critical(f"Error subiendo datos a BigQuery con to_gbq: {e_upload}")
        raise