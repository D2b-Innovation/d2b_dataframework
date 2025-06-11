import pandas as pd
import json
import requests
from os.path import exists
from requests.structures import CaseInsensitiveDict
from d2b_data.verbose_logger import Verbose  
from d2b_data.Google_Bigquery import Google_Bigquery

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

  def _clean_and_transform_dataFrame(self, res, date_str=None):
      # Asegurarse de que `res` sea un dict (puede venir como string o bytes)
      if isinstance(res, bytes):
          res = json.loads(res.decode("utf-8"))
      elif isinstance(res, str):
          res = json.loads(res)

      DF = pd.json_normalize(res.get("elements"), sep="_")

      if date_str:
          DF["date"] = date_str  # AÑADIMOS AQUÍ LA COLUMNA DATE EXPLÍCITAMENTE

      # Aplanar columnas y limpiar otras fechas
      date_cols = [
          "dateRange.start.month", "dateRange.start.day", "dateRange.start.year",
          "dateRange.end.month", "dateRange.end.day", "dateRange.end.year"
      ]
      for col in date_cols:
          if col in DF.columns:
              DF.drop(columns=col, inplace=True)

      DF.columns = [x.lower() for x in DF.columns]

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
    
  def upload_to_bigquery(self, df, bq_config, credentials_info, workflow_name="linkedin-cloud"):
    """
    Sube el DataFrame de LinkedIn Ads a BigQuery, creando una tabla por día (YYYYMMDD) y sobrescribiendo si existe.

    Args:
        df (pd.DataFrame): DataFrame con datos limpios.
        bq_config (dict): Configuración de destino BigQuery (project-id, dataset, table-prefix).
        credentials_info (dict): Credenciales de servicio como dict.
        workflow_name (str): Nombre del flujo para logs.
    """
    logger = self.verbose_logger  # Alias para el logger

    if df.empty:
        msg = "upload_to_bigquery | DataFrame vacío. No se realizará carga."
        if logger:
            logger.critical(msg, workflow_name)
        raise ValueError(msg)

    if "date" not in df.columns:
        msg = "upload_to_bigquery | La columna 'date' es obligatoria en el dataframe."
        if logger:
            logger.critical(msg, workflow_name)
        raise ValueError(msg)

    try:
        dataset = bq_config["dataset"]
        table_prefix = bq_config["table-prefix"]
        project_id = bq_config["project-id"]
    except Exception as e:
        msg = f"upload_to_bigquery | Error en configuración BigQuery: {e}"
        if logger:
            logger.critical(msg, workflow_name)
        raise ValueError(msg)

    try:
        bq_client = Google_Bigquery(credentials_info=credentials_info, verbose=True)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")  # Asegura formato uniforme

        for date_str in df["date"].unique():
            iter_df = df[df["date"] == date_str].copy()
            suffix = date_str.replace("-", "")  # YYYYMMDD
            table_name = f"{table_prefix}_{suffix}"
            full_table = f"{dataset}.{table_name}"

            if logger:
                logger.log(f"upload_to_bigquery | Subiendo {iter_df.shape[0]} filas a {project_id}.{full_table} (modo replace).")

            bq_client.upload(
                df=iter_df,
                date_column="date",
                destination=full_table,
                project_id=project_id,
                if_exists="replace"  # Sobrescribe la tabla diaria
            )

            # Setear expiración opcional si tenés esa lógica implementada en tu clase Google_Bigquery
            # bq_client.set_table_expiration(full_table, days=1096)

        if logger:
            logger.log("upload_to_bigquery | Carga completa para todas las fechas.")

    except Exception as e_upload:
        msg = f"upload_to_bigquery | Error al subir a BigQuery: {e_upload}"
        if logger:
            logger.critical(msg, workflow_name)
        raise