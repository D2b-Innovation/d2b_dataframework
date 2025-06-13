import pandas as pd
import json
import requests
from os.path import exists
from requests.structures import CaseInsensitiveDict
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# NOTA: La importación de Verbose y Google_Bigquery no son necesarias en este archivo.

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
    
    if res_token.status_code != 200 or "error" in res_token.json():
        raise ValueError(f"Error getting token: {res_token.content.decode()}")
    
    json_token = res_token.json()
    self.token = json_token.get('access_token')
    
    if self.token:
        if filename is not None:
            with open(filename, 'w') as f:
                json.dump(json_token, f)
        self.set_headers_token(self.token)
        if self.verbose_logger:
            self.verbose_logger.log("Nuevo token obtenido desde API")
    else:
      raise ValueError(f'Error getting token: No se encontró access_token en la respuesta: {res_token.content.decode()}')
    return self.token

  # (MEJORA) Se añaden 'pivot' y 'time_granularity' como parámetros
  def get_report(self, account_id, start, end, metrics, pivot="CREATIVE", time_granularity="DAILY"):
    account_id = f"urn%3Ali%3AsponsoredAccount%3A{account_id}"

    start_parts = start.split("-")
    end_parts   = end.split("-")
    start_url = f"&dateRange.start.day={start_parts[2]}&dateRange.start.month={start_parts[1]}&dateRange.start.year={start_parts[0]}"
    end_url   = f"&dateRange.end.day={end_parts[2]}&dateRange.end.month={end_parts[1]}&dateRange.end.year={end_parts[0]}"
    
    metrics = metrics + ",pivot,pivotValues"
    
    pivot_text = ''
    if pivot:
      # Permite múltiples pivots separados por comas, ej: "CREATIVE,CAMPAIGN"
      for idx, val in enumerate(pivot.split(",")):
        pivot_text += f"pivots[{idx}]={val}&"

    url = f'https://api.linkedin.com/v2/adAnalyticsV2?q=statistics&{pivot_text}timeGranularity={time_granularity}{start_url}{end_url}&accounts={account_id}&fields={metrics}'
    
    if self.verbose_logger:
        self.verbose_logger.log(f"Ejecutando GET a URL de LinkedIn: {url}")
        
    res = requests.get(url, headers=self.HEADERS)
    if res.status_code != 200:
      if self.verbose_logger:
        self.verbose_logger.log(f"Error en respuesta de API: {res.status_code} - {res.content.decode()}")
      raise Exception(res.content)
    return res.content

  def _clean_and_transform_dataFrame(self, res, date_str=None):
    if isinstance(res, bytes):
        res = json.loads(res.decode("utf-8"))
    elif isinstance(res, str):
        res = json.loads(res)

    if self.verbose_logger:
        self.verbose_logger.log("Normalizando respuesta JSON.")
    
    DF = pd.json_normalize(res.get("elements"), sep="_")

    if DF.empty:
        if self.verbose_logger:
            self.verbose_logger.log("DataFrame vacío después de normalizar JSON.")
        return DF

    if date_str:
        DF["date"] = date_str
    elif "daterange_start_year" in DF.columns and "daterange_start_month" in DF.columns and "daterange_start_day" in DF.columns:
        date_df = DF[["daterange_start_year", "daterange_start_month", "daterange_start_day"]].copy()
        date_df.columns = ["year", "month", "day"]
        DF["date"] = pd.to_datetime(date_df)
    else:
        if self.verbose_logger:
            self.verbose_logger.log("Advertencia: No se encontró 'date_str' ni columnas 'daterange' para crear la columna 'date'.")

    date_cols_to_drop = ["daterange_start_month", "daterange_start_day", "daterange_start_year", "daterange_end_month", "daterange_end_day", "daterange_end_year"]
    cols_to_drop_existing = [col for col in date_cols_to_drop if col in DF.columns]
    if cols_to_drop_existing:
        DF.drop(columns=cols_to_drop_existing, inplace=True)

    # (MEJORA) Consolidar toda la limpieza de columnas aquí para mayor consistencia
    DF.columns = (
        DF.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.replace(r"[^\w]", "", regex=True)
    )

    if self.verbose_logger:
        self.verbose_logger.log(f"DataFrame limpio con shape final: {DF.shape}")
    return DF

  # (MEJORA) Se elimina la doble decodificación de JSON
  def get_report_dataframe(self, account_id, start, end, metrics, unsampled=False, **kwargs):
      # **kwargs permite pasar parámetros opcionales como 'pivot' a get_report
      if unsampled:
          if self.verbose_logger:
              self.verbose_logger.log("Extracción UNSAMPLED activada")
          date_range = pd.date_range(start, end, freq='D')
          array_reports = []

          for date in date_range:
              date_str = date.strftime('%Y-%m-%d')
              if self.verbose_logger:
                  self.verbose_logger.log(f"Extrayendo fecha {date_str}")
              
              res = self.get_report(account_id, date_str, date_str, metrics, **kwargs)
              df = self._clean_and_transform_dataFrame(res, date_str=date_str)
              
              if not df.empty:
                  self.verbose_logger.log(f"DataFrame para {date_str} con shape: {df.shape}")
                  array_reports.append(df)

          if not array_reports:
              return pd.DataFrame() # Devuelve DF vacío si no se extrajo nada
          return pd.concat(array_reports, ignore_index=True)

      else:
          if self.verbose_logger:
              self.verbose_logger.log("Extracción con un solo llamado (sampled)")
          
          res = self.get_report(account_id, start, end, metrics, **kwargs)
          return self.clean_and_transform_dataFrame(res)
    
  def upload_to_bigquery(self, df, bq_config, credentials_info, schema):
    """
    Sube un DataFrame a una tabla particionada en BigQuery y luego setea su expiración.
    """
    logger = self.verbose_logger
    if df.empty:
        logger.log("DataFrame vacío, no se sube nada a BigQuery.")
        return

    if "date" not in df.columns:
        msg = "Columna 'date' ausente en el DataFrame. No se puede subir a tabla particionada."
        logger.critical(msg)
        raise ValueError(msg)

    # Asegurarse que la columna de particionamiento es de tipo correcto
    df["date"] = pd.to_datetime(df["date"])

    table_prefix = bq_config["table-prefix"]
    dataset = bq_config["dataset"]
    project_id = bq_config["project-id"]
    destination_table = f"{dataset}.{table_prefix}"
    full_table_id = f"{project_id}.{dataset}.{table_prefix}"
    
    logger.log(f"Iniciando subida de {df.shape[0]} filas a la tabla particionada: {full_table_id}")

    try:
        # 1. CARGAR LOS DATOS
        credentials_gbq = service_account.Credentials.from_service_account_info(credentials_info)
        to_gbq(
            dataframe=df,
            destination_table=destination_table,
            project_id=project_id,
            credentials=credentials_gbq,
            if_exists="append",
            table_schema=schema,
            progress_bar=False,
            api_method='load_csv'
        )
        logger.log(f"Subida a {full_table_id} completada exitosamente.")

        # --- SETEAR EXPIRACIÓN ---
        # 2. Una vez que la carga es exitosa, se actualiza la expiración de la tabla.
        try:
            logger.log(f"Actualizando la expiración para la tabla {full_table_id}...")
            
            # Crear un cliente de BigQuery para operaciones de metadatos
            bq_client = bigquery.Client(project=project_id, credentials=credentials_gbq)
            
            # Obtener la referencia de la tabla
            table_ref = bq_client.get_table(destination_table)
            
            # Calcular la nueva fecha de expiración (aprox. 3 años)
            expiration_date = datetime.utcnow() + timedelta(days=1096)
            table_ref.expires = expiration_date
            
            # Actualizar la tabla en BigQuery, especificando que solo se cambia la expiración
            bq_client.update_table(table_ref, ["expires"])
            
            logger.log(f"Expiración para la tabla {full_table_id} actualizada a {expiration_date.strftime('%Y-%m-%d')}.")

        except Exception as e_expires:
            # Si falla, no es un error crítico. Solo loguear una advertencia.
            logger.log(f"ADVERTENCIA: No se pudo actualizar la expiración para la tabla {full_table_id}. Error: {e_expires}")

    except Exception as e_upload:
        logger.critical(f"Error subiendo datos a BigQuery con to_gbq: {e_upload}")
        raise # Volvemos a lanzar la excepción para que la Cloud Function falle y alerte
