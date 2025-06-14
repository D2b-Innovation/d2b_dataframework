import pandas as pd
import json
import requests
from os.path import exists
from requests.structures import CaseInsensitiveDict
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
from urllib.parse import quote

class Linkedin_Marketing():
  def __init__(self, APPLICATON_KEY, APPLICATON_SECRET):
    self.APPLICATON_KEY    = APPLICATON_KEY
    self.APPLICATON_SECRET = APPLICATON_SECRET
    self.verbose_logger    = None
    self.HEADERS           = None

  def set_headers_token(self, token_header):
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Bearer {token_header}"
    self.HEADERS = headers

  def get_token(self, filename=None):
    if filename and exists(filename):
      with open(filename) as json_file:
        access_token = json.load(json_file).get('access_token')
      self.set_headers_token(access_token)
      if self.verbose_logger: self.verbose_logger.log("Token cargado desde archivo")
      return self.HEADERS
    raise ValueError(f"No se pudo obtener el token desde el archivo: {filename}")

  def get_report(self, account_id, start, end, metrics, pivot=None, time_granularity="DAILY"):
    account_id_encoded = f"urn%3Ali%3AsponsoredAccount%3A{account_id}"
    start_parts = start.split("-")
    start_url = f"&dateRange.start.day={start_parts[2]}&dateRange.start.month={start_parts[1]}&dateRange.start.year={start_parts[0]}"
    end_parts   = end.split("-")
    end_url   = f"&dateRange.end.day={end_parts[2]}&dateRange.end.month={end_parts[1]}&dateRange.end.year={end_parts[0]}"
    
    if pivot and "pivot" not in metrics:
        metrics += ",pivot,pivotValues"
    
    pivot_text = ''
    if pivot:
      for idx, val in enumerate(pivot.split(",")):
        pivot_text += f"pivots[{idx}]={val.strip()}&"

    url = (f'https://api.linkedin.com/v2/adAnalyticsV2?q=statistics&{pivot_text}'
           f'timeGranularity={time_granularity}{start_url}{end_url}&accounts={account_id_encoded}&fields={metrics}')
    
    if self.verbose_logger: self.verbose_logger.log(f"Ejecutando GET a URL: {url}")
        
    res = requests.get(url, headers=self.HEADERS)
    if res.status_code != 200:
        error_content = res.content.decode()
        if self.verbose_logger: self.verbose_logger.log(f"Error en API: {res.status_code} - {error_content}")
        raise Exception(f"Error en API de LinkedIn: {error_content}")
    return res.content

  def get_report_dataframe(self, account_id, start, end, metrics, unsampled=False, **kwargs):
    if unsampled:
        date_range = pd.date_range(start, end, freq='D')
        array_reports = [self._clean_and_transform_dataFrame(self.get_report(account_id, d.strftime('%Y-%m-%d'), d.strftime('%Y-%m-%d'), metrics, **kwargs), date_str=d.strftime('%Y-%m-%d')) for d in date_range]
        return pd.concat([df for df in array_reports if not df.empty], ignore_index=True) if array_reports else pd.DataFrame()
    else:
        return self._clean_and_transform_dataFrame(self.get_report(account_id, start, end, metrics, **kwargs))

  def _clean_and_transform_dataFrame(self, res, date_str=None):
    if isinstance(res, bytes): res = json.loads(res.decode("utf-8"))
    DF = pd.json_normalize(res.get("elements"), sep="_")
    if DF.empty: return DF

    if date_str: DF["date"] = date_str
    if "date" in DF.columns: DF["date"] = pd.to_datetime(DF["date"])

    if 'adentities' in DF.columns: DF['adentities'] = DF['adentities'].apply(lambda x: json.dumps(x) if pd.notna(x) else None)

    DF.columns = (DF.columns.str.strip().str.lower().str.replace(" ", "_", regex=False).str.replace("-", "_", regex=False).str.replace(r"[^\w]", "", regex=True))
    if self.verbose_logger: self.verbose_logger.log(f"DataFrame crudo generado. Columnas: {DF.columns.tolist()}")
    return DF

  def upload_to_bigquery_by_day(self, df, bq_config, credentials_info, schema):
    """
    Sube el DataFrame a BigQuery creando una tabla separada para cada día.
    El nombre de cada tabla tendrá el sufijo _YYYYMMDD.
    """
    logger = self.verbose_logger
    if df.empty:
        logger.log("DataFrame vacío, no se sube nada a BQ.")
        return

    # Nos aseguramos de que 'date' sea un objeto datetime para poder iterar
    df['date'] = pd.to_datetime(df['date'])

    project_id = bq_config["project-id"]
    dataset = bq_config["dataset"]
    table_prefix = bq_config["table-prefix"]
    
    credentials_gbq = service_account.Credentials.from_service_account_info(credentials_info)

    # Iterar por cada fecha única en el DataFrame
    unique_dates = df['date'].dt.date.unique()
    logger.log(f"Se encontraron {len(unique_dates)} fechas únicas para procesar.")

    for single_date in unique_dates:
        # Formatear la fecha para el sufijo de la tabla (ej. 20250613)
        date_suffix = single_date.strftime('%Y%m%d')
        destination_table_name = f"{table_prefix}_{date_suffix}"
        full_destination_table = f"{dataset}.{destination_table_name}"
        
        # Filtrar el DataFrame para obtener solo los datos de este día
        df_for_day = df[df['date'].dt.date == single_date].copy()

        # Antes de subir, formateamos la columna 'date' a string para evitar errores
        df_for_day['date'] = df_for_day['date'].dt.strftime('%Y-%m-%d')
        
        logger.log(f"Subiendo {len(df_for_day)} filas para la fecha {single_date.strftime('%Y-%m-%d')} a la tabla {full_destination_table}")

        try:
            to_gbq(
                dataframe=df_for_day,
                destination_table=full_destination_table,
                project_id=project_id,
                credentials=credentials_gbq,
                if_exists='replace',  # REEMPLAZA la tabla del día si ya existe, evitando duplicados
                table_schema=schema,
                api_method='load_csv'
            )
            logger.log(f"Carga para el día {date_suffix} completada exitosamente.")

            # Opcional: Setear expiración para cada tabla diaria
            try:
                bq_client = bigquery.Client(project=project_id, credentials=credentials_gbq)
                table_ref = bq_client.get_table(full_destination_table)
                table_ref.expires = datetime.utcnow() + timedelta(days=1096)
                bq_client.update_table(table_ref, ["expires"])
            except Exception as e_expires:
                 logger.log(f"ADVERTENCIA: No se pudo actualizar expiración para {full_destination_table}. Error: {e_expires}")

        except Exception as e:
            # Si un día falla, registramos el error y continuamos con el siguiente
            logger.critical(f"FALLO la carga para el día {date_suffix}. Error: {e}")
            continue

  def get_campaign_names(self, campaign_ids):
    """
    Toma una lista de IDs de campaña y devuelve un diccionario mapeando
    cada ID a su nombre, usando el método de batch get.
    """
    logger = self.verbose_logger
    if not campaign_ids:
        return {}

    # La URL base para la consulta de campañas
    base_url = "https://api.linkedin.com/v2/adCampaignsV2"
    
    # Parámetros para la petición. La librería 'requests' se encargará de
    # formatearlo correctamente como: ?ids=123&ids=456...
    params = {
        'ids': [int(id) for id in campaign_ids],
        'fields': 'id,name'
    }
    
    logger.log(f"Consultando nombres para {len(campaign_ids)} campañas usando batch get...")
    
    try:
        res = requests.get(base_url, headers=self.HEADERS, params=params)
        res.raise_for_status() # Lanza un error si la petición falla (ej. 4xx, 5xx)
        
        # La respuesta para un batch get es un diccionario donde las claves son los IDs
        data = res.json().get('results', {})
        
        # Creamos el diccionario de mapeo: {12345: "Mi Campaña", ...}
        # Hay que convertir la clave (que viene como string) a integer para que coincida
        name_map = {int(id_str): details['name'] for id_str, details in data.items()}
        
        logger.log(f"Se encontraron {len(name_map)} nombres de campañas.")
        return name_map

    except Exception as e:
        logger.critical(f"No se pudieron obtener los nombres de las campañas. Error: {e}")
        return {}
        
  def get_campaign_group_names(self, group_ids):
    """
    Toma una lista de IDs de Grupos de Campaña y devuelve un diccionario mapeando cada ID a su nombre.
    """
    logger = self.verbose_logger
    if not group_ids:
        return {}

    ids_for_query = ",".join([str(int(id)) for id in group_ids])
    search_query = f"search=(id:(values:List({ids_for_query})))"
    
    # El endpoint para los Grupos de Campaña
    url = f"https://api.linkedin.com/v2/adCampaignGroupsV2?q={quote(search_query)}&fields=id,name"
    
    logger.log(f"Consultando nombres para {len(group_ids)} grupos de campaña...")
    
    try:
        res = requests.get(url, headers=self.HEADERS)
        res.raise_for_status()
        data = res.json().get('elements', [])
        name_map = {item['id']: item['name'] for item in data}
        logger.log(f"Se encontraron {len(name_map)} nombres de grupos de campaña.")
        return name_map
    except Exception as e:
        logger.critical(f"No se pudieron obtener los nombres de los grupos de campaña. Error: {e}")
        return {}

