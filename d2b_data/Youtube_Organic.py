import pandas as pd
import json
import time
import random
import d2b_data.Google_Token_MNG 
from googleapiclient.errors import HttpError


class youtubeOrganic():
    def __init__(self, client_secret, token_json, debug=False, use_service_account=False):
        """
        Constructor for YouTube Analytics API Connector.
        """
        self.default_api_name = 'youtubeAnalytics'
        self.default_version = 'v2'
        self.client_secret = client_secret
        self.debug_status = debug
        self.use_service_account = use_service_account
        self.service, self.service_data = self.create_services(self.client_secret, token_json, use_service_account)

    def get_service(self):
        '''return the service object'''
        return self.service

    def get_data_service(self):
        '''return the data service object'''
        return self.service_data

    def debug(self, message):
        '''Imprime mensaje si debug está activado'''
        if self.debug_status:
            print(message)

    def create_services(self, secrets, credentials, use_service_account=False):
        """Crea conexión con YouTube Analytics y Data API usando un solo Token Manager
        ARGS
        secrets <str> Path al client_secret.json
        credentials <str> Path al token.json o Service Account JSON
        use_service_account <bool> Si True, usa Service Account
        RETURNS
        tuple: (service_analytics, service_data)
        """
        scopes = [
            'https://www.googleapis.com/auth/yt-analytics.readonly',
            'https://www.googleapis.com/auth/youtube.readonly'
        ]
        
        token_mng = d2b_data.Google_Token_MNG.Google_Token_MNG(
            client_secret=secrets,
            token=credentials,
            scopes=scopes,
            api_version=self.default_version,
            api_name=self.default_api_name,
            use_service_account=use_service_account
        )
        
        service_analytics = token_mng.get_service()
        self.debug("Conectado a YouTube Analytics API v2")

        service_data = token_mng.create_api(
            api_name='youtube',
            api_version='v3',
            scopes=scopes,
            secrets=secrets,
            credentials=credentials,
            use_sa=use_service_account
        )
        self.debug("Conectado a YouTube Data API v3")
        
        return service_analytics, service_data

    def _to_DF(self, response):
        '''
        Transforma el response de YouTube Analytics a DataFrame
        ARGS
        response <dict> Response del API
        RETURN
        <DataFrame> Datos en formato pandas
        '''
        if not response or 'rows' not in response or not response['rows']:
            self.debug("No se encontraron datos en el response para convertir a DF.")
            return pd.DataFrame()

        columnHeaders = response.get('columnHeaders', [])
        columns = [header.get('name') for header in columnHeaders]
        
        df = pd.DataFrame(response['rows'], columns=columns)
        return df

    def _get_report_raw(self, query_params):
        '''
        Ejecuta query raw contra YouTube Analytics API con Exponential Backoff
        Maneja errores de quota y rate limits.
        '''
        max_retries = 5
        retry_count = 0

        while True:
            try:
                response = self.service.reports().query(**query_params).execute()
                return response
            
            except HttpError as e:
                status_code = e.resp.status
                reason = e._get_reason()
                
                # 403 / 429 = Rate limits o Quota. 500/503 = Errores internos.
                if status_code in [403, 429, 500, 503]:
                    if retry_count >= max_retries:
                        self.debug(f" Error {status_code} ({reason}): Se agotaron los {max_retries} reintentos.")
                        raise e 

                    sleep_time = (2 ** retry_count) + random.uniform(0, 1)

                    self.debug(f" Error {status_code}. Reintento {retry_count + 1}/{max_retries}. Esperando {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    retry_count += 1
                else:
                    self.debug(f" Error no recuperable {status_code}: {reason}")
                    raise e
                
            except Exception as e:
                error_str = str(e)      
                if "429" in error_str or "403" in error_str:
                    if retry_count >= max_retries: 
                        raise e
                    
                    sleep_time = (2 ** retry_count) + random.uniform(0, 1)
                    self.debug(f" Error genérico detectado. Esperando {sleep_time:.2f} s...")
                    time.sleep(sleep_time)
                    retry_count += 1
                else:
                    raise e

    def get_report_df(self, channel_id, start_date, end_date, metrics, dimensions=None, filters=None, sort=None, max_results=None, start_index=None):
        '''
        Obtiene el reporte de YouTube Analytics como DataFrame de Pandas.
        
        ARGS:
            channel_id <str>: ID del canal de YouTube (ej. 'UC1234567890') o 'MINE' si estás autenticado con OAuth de tu canal.
            start_date <str>: Fecha de inicio en formato 'YYYY-MM-DD'
            end_date <str>: Fecha de fin en formato 'YYYY-MM-DD'
            metrics <str>: Métricas separadas por comas (ej. 'views,estimatedMinutesWatched,averageViewDuration')
            dimensions <str>: Dimensiones separadas por comas (ej. 'day,video') (opcional)
            filters <str>: Filtros para el reporte (opcional)
            sort <str>: Ordenación separada por comas (opcional)
            max_results <int>: Número máximo de resultados (opcional)
            start_index <int>: Índice de inicio para paginación (1-based) (opcional)
        
        RETURN:
            <pd.DataFrame>: DataFrame con los resultados
        '''
        
        # Validar formato ids
        if channel_id.upper() == "MINE":
            ids = "channel==MINE"
        elif not channel_id.startswith("channel=="):
            ids = f"channel=={channel_id}"
        else:
            ids = channel_id

        query_params = {
            "ids": ids,
            "startDate": start_date,
            "endDate": end_date,
            "metrics": metrics
        }
        
        if dimensions:
            query_params["dimensions"] = dimensions
        if filters:
            query_params["filters"] = filters
        if sort:
            query_params["sort"] = sort
        if max_results:
            query_params["maxResults"] = max_results
        if start_index:
            query_params["startIndex"] = start_index

        self.debug(f"Ejecutando query YouTube Analytics:\n{query_params}")
        
        raw_response = self._get_report_raw(query_params)
        df_report = self._to_DF(raw_response)
        
        return df_report

    def list_channels(self, part="snippet,contentDetails,statistics", **kwargs):
        '''
        Obtiene los canales asociados según los parámetros enviados usando YouTube Data API v3.
        
        ARGS:
            part <str>: Las partes del channel a traer, por defecto 'snippet,contentDetails,statistics'.
            **kwargs: Puede ser mine=True, o id='CHANNEL_ID', forUsername='USERNAME'.
            
        RETURN:
            <pd.DataFrame>: DataFrame con la información de los canales.
        '''
        try:
            # Si no se especifica ni id, forUsername ni categoryId, por defecto pedimos el propio ('mine')
            if not any(k in kwargs for k in ['id', 'forUsername', 'categoryId']):
                kwargs.setdefault('mine', True)
                
            response = self.service_data.channels().list(
                part=part,
                **kwargs
            ).execute()
            
            items = response.get('items', [])
            if not items:
                self.debug("No se encontraron canales con esos parámetros.")
                return pd.DataFrame()
            
            return pd.json_normalize(items)
            
        except Exception as e:
            self.debug(f"Error al listar canales: {e}")
            raise e
