import pandas as pd
import json
import requests
from os.path import exists
from requests.structures import CaseInsensitiveDict
from d2b_data.verbose_logger import Verbose  
from d2b_data.Google_Bigquery import Google_Bigquery
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
    self.verbose_logger    = None  # ← Logger personalizado, instanciable desde afuera

  def get_url(self):
    return self.OAUTH_URL

  def set_headers_token(self, token_header):
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Bearer {token_header}"
    self.HEADERS = headers
    return self.HEADERS

  def get_token(self, code=None, filename=None):
    if filename is not None and exists(filename):
      with open(filename) as json_file:
        json_content = json.load(json_file)
      access_token = json_content.get('access_token')
      self.set_headers_token(access_token)
      if self.verbose_logger:
        self.verbose_logger.log("Token cargado desde archivo")
      return self.HEADERS

    url_token_endpoint = "https://www.linkedin.com/oauth/v2/accessToken"
    headers = CaseInsensitiveDict()
    params = CaseInsensitiveDict()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    params["grant_type"]    = "authorization_code"
    params["code"]          = f"{code}"
    params["redirect_uri"]  = f"{self.RETURN_URL}"
    params["client_id"]     = f"{self.APPLICATON_KEY}"
    params["client_secret"] = f"{self.APPLICATON_SECRET}"
    res_token = requests.post(url_token_endpoint, params=params, headers=headers)
    if json.loads(res_token.content).get("error", None) is not None:
      raise ValueError('Error getting token: ' + str(res_token.content))
    
    json_token = json.loads(res_token.content)
    if json_token.get('access_token', False):
      if filename is not None:
        with open(filename, 'w') as f:
          json.dump(json_token, f)
      self.token = json_token.get('access_token')
      self.set_headers_token(self.token)
      if self.verbose_logger:
        self.verbose_logger.log("Nuevo token obtenido desde API")
    else:
      raise ValueError('Error getting token:' + res_token.content.decode())
    return self.token

  def get_report(self, account_id, start, end, metrics):
    pivot = "CREATIVE"
    time = "DAILY"
    account_id = f"urn%3Ali%3AsponsoredAccount%3A{account_id}"

    start = start.split("-")
    end   = end.split("-")
    start = f"&dateRange.start.day={start[2]}&dateRange.start.month={start[1]}&dateRange.start.year={start[0]}"
    end   = f"&dateRange.end.day={end[2]}&dateRange.end.month={end[1]}&dateRange.end.year={end[0]}"
    metrics = metrics + ",pivot,pivotValues"
    text = ''
    if pivot:
      for idx, val in enumerate(pivot.split(",")):
        text += f"pivots[{idx}]={val}&"

    url = f'https://api.linkedin.com/v2/adAnalyticsV2?q=statistics&{text}&timeGranularity={time}&{start}{end}&accounts={account_id}&fields={metrics}'
    res = requests.get(url, headers=self.HEADERS)
    if res.status_code != 200:
      if self.verbose_logger:
        self.verbose_logger.log(f"Error en respuesta de API: {res.status_code} - {res.content}")
      raise Exception(res.content)
    return res.content

  def _clean_and_transform_dataFrame(self, res, verbose_logger, date_str=None):
    # Asegurarse de que `res` sea un dict (puede venir como string o bytes)
    if isinstance(res, bytes):
        verbose_logger.log("Decodificando respuesta desde bytes.")
        res = json.loads(res.decode("utf-8"))
    elif isinstance(res, str):
        verbose_logger.log("Decodificando respuesta desde string.")
        res = json.loads(res)

    verbose_logger.log("Normalizando respuesta JSON.")
    DF = pd.json_normalize(res.get("elements"), sep="_")

    if date_str:
        verbose_logger.log(f"Asignando columna 'date' con valor: {date_str}")
        DF["date"] = date_str

    # Aplanar columnas y limpiar otras fechas
    date_cols = [
        "daterange_end_day",
        "daterange_end_month",
        "daterange_end_year",
        "daterange_start_day",
        "daterange_start_month",
        "daterange_start_year"
    ]
    for col in date_cols:
        if col in DF.columns:
            verbose_logger.log(f"Eliminando columna innecesaria: {col}")
            DF.drop(columns=col, inplace=True)

    verbose_logger.log("Normalizando nombres de columnas a minúsculas.")
    DF.columns = [x.lower() for x in DF.columns]

    verbose_logger.log(f"DataFrame limpio con shape final: {DF.shape}")
    return DF

  def get_report_dataframe(self, account_id, start, end, metrics, unsampled=False):
      if unsampled:
          if self.verbose_logger:
              self.verbose_logger.log("Extracción UNSAMPLED activada")
          date_range = pd.date_range(start, end, freq='D')
          array_reports = []

          for date in date_range:
              date_str = date.strftime('%Y-%m-%d')
              if self.verbose_logger:
                  self.verbose_logger.log(f"Extrayendo fecha {date_str}")
              res = self.get_report(account_id, date_str, date_str, metrics)

              # Decodificar si es necesario
              if isinstance(res, bytes):
                  res = json.loads(res.decode("utf-8"))
              elif isinstance(res, str):
                  res = json.loads(res)

              # ✅ Solo pasamos `date_str`, el método se encarga de agregarlo al DF
              df = self._clean_and_transform_dataFrame(res, date_str=date_str)
              array_reports.append(df)

          return pd.concat(array_reports)

      else:
          if self.verbose_logger:
              self.verbose_logger.log("Extracción con un solo llamado (sampled)")
          res = self.get_report(account_id, start, end, metrics)

          # Decodificar si es necesario
          if isinstance(res, bytes):
              res = json.loads(res.decode("utf-8"))
          elif isinstance(res, str):
              res = json.loads(res)

          res = self._clean_and_transform_dataFrame(res)
          return res
    
  def upload_to_bigquery_by_day(self, df, bq_config, credentials_info, schema, workflow_name="linkedin-cloud"):
    """
    Sube el DataFrame de LinkedIn Ads a una única tabla en BigQuery, escribiendo una partición por día.

    Args:
        df (pd.DataFrame): DataFrame con columna 'date'.
        bq_config (dict): Contiene dataset, project-id y table-prefix.
        credentials_info (dict): SA como dict para autenticación.
        schema (list): Lista de dicts con 'name', 'type' y 'description' para schema de tabla.
        workflow_name (str): Nombre del flujo para logs.
    """
    logger = self.verbose_logger
    if df.empty:
        msg = "DataFrame vacío, no se sube nada a BigQuery."
        logger.critical(msg, workflow_name)
        raise ValueError(msg)

    if "date" not in df.columns:
        msg = "Columna 'date' ausente en el DataFrame."
        logger.critical(msg, workflow_name)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    table_prefix = bq_config["table-prefix"]
    dataset = bq_config["dataset"]
    project_id = bq_config["project-id"]

    text_date_suffix = date_str.replace("-", "")
    table_id_suffix = f"{table_prefix}_{text_date_suffix}"
    full_table = f"{dataset}.{table_id_suffix}"

    client = bigquery.Client(project=project_id, credentials=service_account.Credentials.from_service_account_info(credentials_info))

    for date_str in df["date"].unique():
        iter_df = df[df["date"] == date_str].copy()
        logger.log(f"Subiendo {iter_df.shape[0]} filas para fecha {date_str} a tabla {full_table}")

        try:
            to_gbq(
                dataframe=iter_df,
                destination_table=full_table,
                project_id=project_id,
                credentials=client._credentials,
                if_exists="replace",
                table_schema=schema
            )
            # Set expiración
            table_ref = client.dataset(dataset).table(table_prefix)
            table_obj = client.get_table(table_ref)
            table_obj.expires = datetime.utcnow() + timedelta(days=1096)
            client.update_table(table_obj, ["expires"])

            logger.log(f"Fecha {date_str} cargada exitosamente a {full_table} y expiración actualizada.")
        except Exception as e:
            logger.critical(f"Error subiendo {date_str} a {full_table}: {e}", workflow_name)
            continue

    logger.log(f"Proceso finalizado para {len(df['date'].unique())} fechas en tabla única particionada.")