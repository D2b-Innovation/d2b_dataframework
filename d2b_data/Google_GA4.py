import pandas as pd
import json
import webbrowser
import httplib2
import os
import datetime
import d2b_data.Google_Token_MNG 

from datetime import timedelta
from googleapiclient.discovery import build
from oauth2client import client
from builtins import input
from google_auth_oauthlib import flow



class Google_GA4():
    def __init__(self, client_secret, token_json, debug=False, auto_paginate=True, extract_sampling=False, intraday_limit=30):
        self.default_api_name = 'analyticsdata'
        self.default_version = 'v1beta'
        self.client_secret = client_secret
        self.debug_status = debug
        self.auto_paginate = auto_paginate  # Paginación automática activada por defecto
        self.extract_sampling = extract_sampling  # Sampling info desactivada por defecto
        self.intraday_limit = intraday_limit * 100000
        self.service = self.create_service(self.client_secret, token_json)

    def _extract_sampling_info(self, report_data):
        '''
        Extrae información de sampling del response de GA4
        ARGS
        report_data <dict> Datos del report
        RETURN
        <dict> Diccionario con info de sampling
        '''
        info = {
            'samplesReadCounts': 0,
            'samplingSpaceSizes': 0,
            'sampled': False,
            'sampling_percentage': 100.0,
            'dataLossFromOtherRow': False
        }

        metadata = report_data.get("metadata", {})

        # Sampling
        sampling_metadatas = metadata.get("samplingMetadatas", [])
        if sampling_metadatas:
            s_data = sampling_metadatas[0]
            samples_read = int(s_data.get("samplesReadCount", 0))
            space_size = int(s_data.get("samplingSpaceSize", 0))

            if space_size > 0:
                info['samplesReadCounts'] = samples_read
                info['samplingSpaceSizes'] = space_size
                info['sampled'] = True
                info['sampling_percentage'] = (samples_read / space_size) * 100

        # Cardinalidad
        if metadata.get("dataLossFromOtherRow"):
            info['dataLossFromOtherRow'] = True

        return info

    def get_service(self):
        '''return the service object'''
        return self.service

    def get_token(self):
        '''return the token'''
        return self.token

    def get_credentials(self):
        '''return credentials'''
        return self.credentials

    def set_auto_paginate(self, auto_paginate=True):
        '''
        Activa/desactiva la paginación automática
        ARGS
        auto_paginate <bool> True para paginación automática
        RETURNS
        <bool> Estado actual
        '''
        if type(auto_paginate) == bool:
            self.auto_paginate = auto_paginate
        return self.auto_paginate

    def set_extract_sampling(self, extract_sampling=True):
        '''
        Activa/desactiva extracción de info de sampling
        ARGS
        extract_sampling <bool> True para extraer sampling info
        RETURNS
        <bool> Estado actual
        '''
        if type(extract_sampling) == bool:
            self.extract_sampling = extract_sampling
        return self.extract_sampling

    def create_service(self, secrets, credentials):
        """Crea conexión con GA4"""
        token_mng = d2b_data.Google_Token_MNG.Google_Token_MNG(
            secrets,
            credentials,
            scopes=['https://www.googleapis.com/auth/analytics.readonly'],
            api_version=self.default_version,
            api_name=self.default_api_name
        )
        self.service = token_mng.get_service()
        self.debug("Conectado a GA4")
        return self.service

    def _to_DF(self, raw_server_response):
        '''
        Transforma el response de GA4 a DataFrame
        ARGS
        raw_server_response <dict> Response del API
        RETURN
        <DataFrame> Datos en formato pandas
        '''
        if not raw_server_response.get("reports"):
            return pd.DataFrame()

        response = raw_server_response.get("reports")[0]
        
        cols = []
        for dimensions_cols in response.get("dimensionHeaders", []):
            cols.append(dimensions_cols.get("name"))
        for metrics_cols in response.get("metricHeaders", []):
            cols.append(metrics_cols.get("name"))
        
        results = []
        for row in response.get("rows", []):
            row_array = []
            for dimension in row.get("dimensionValues", []):
                row_array.append(dimension.get("value"))
            for metrics in row.get("metricValues", []):
                row_array.append(metrics.get("value"))
            results.append(row_array)
        
        return pd.DataFrame(results, columns=cols)

    def debug(self, message):
        '''Imprime mensaje si debug está activado'''
        if self.debug_status:
            print(message)

    def _get_report_raw(self, property_id, query):
        '''Ejecuta query raw contra GA4 API'''
        response = self.service.properties().batchRunReports(property=property_id, body=query).execute()
        return response

    def get_report_df(self, property_id, query, extract_sampling=None):
        '''
        ARGS
        extract_sampling <bool>: Si None, usa self.extract_sampling. 
                                Si True/False, sobrescribe para esta llamada.
        '''
        # Determinar si extraer sampling
        should_extract = extract_sampling if extract_sampling is not None else self.extract_sampling
        
        if not self.auto_paginate:
            return self._get_single_report(property_id, query, should_extract)
        
        return self._get_paginated_report(property_id, query, should_extract)

    def _get_single_report(self, property_id, query, extract_sampling=False):
        '''Obtiene un report simple sin paginación'''
        res = self._get_report_raw(property_id, query)
        df_report = self._to_DF(res)

        # Agregar info de sampling si está activada
        if extract_sampling and not df_report.empty:
            report_data = res.get("reports")[0]
            sampling_info = self._extract_sampling_info(report_data)
            df_report['samplesReadCounts'] = sampling_info['samplesReadCounts']
            df_report['samplingSpaceSizes'] = sampling_info['samplingSpaceSizes']
            df_report['sampling_percentage'] = sampling_info['sampling_percentage']
            df_report['sampled'] = sampling_info['sampled']
            df_report['dataLossFromOtherRow'] = sampling_info['dataLossFromOtherRow']

        return df_report

    def _get_paginated_report(self, property_id, query, extract_sampling=False):
        '''
        Obtiene report con paginación automática por días y offset
        '''
        # Extraer rango de fechas
        original_date_ranges = query['requests'][0]['dateRanges']
        start_date = original_date_ranges[0]['startDate']
        end_date = original_date_ranges[0]['endDate']

        self.debug(f"Paginando desde {start_date} hasta {end_date}")

        # Convertir a datetime
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Lista para almacenar DataFrames
        all_dataframes = []
        current_date = start

        # Iterar día por día
        while current_date <= end:
            day_str = current_date.strftime('%Y-%m-%d')
            self.debug(f"Consultando día: {day_str}")

            # Obtener todas las filas del día
            day_dataframes = self._get_all_rows_for_day(property_id, query, day_str, extract_sampling)

            if day_dataframes:
                all_dataframes.extend(day_dataframes)

            current_date += timedelta(days=1)

        # Combinar resultados
        if all_dataframes:
            result_df = pd.concat(all_dataframes, ignore_index=True)
            self.debug(f"Total de filas obtenidas: {len(result_df)}")
            return result_df
        else:
            self.debug("No se encontraron datos")
            return pd.DataFrame()

    def _get_all_rows_for_day(self, property_id, query, day_str, extract_sampling=False):
        '''
        Obtiene todas las filas de un día usando offset
        '''
        day_dataframes = []
        offset = 0
        limit_per_request = 250000

        while True:
            # Crear query para este día con offset
            daily_query = self._create_daily_query(query, day_str, day_str)
            daily_query['requests'][0]['offset'] = offset

            self.debug(f"  → offset {offset}")

            # Ejecutar query
            daily_df = self._get_single_report(property_id, daily_query, extract_sampling)

            if daily_df.empty:
                break

            rows_received = len(daily_df)
            day_dataframes.append(daily_df)

            # Si recibimos menos de 250k, no hay más datos
            if rows_received < limit_per_request:
                break

            offset += limit_per_request

        total_rows = sum(len(df) for df in day_dataframes)
        if total_rows > 0:
            self.debug(f"  ✓ Total del día: {total_rows} filas")

        return day_dataframes

    def _create_daily_query(self, original_query, start_date, end_date):
        '''
        Crea copia de query con fechas específicas
        '''
        daily_query = copy.deepcopy(original_query)
        daily_query['requests'][0]['dateRanges'] = [{
            'startDate': start_date,
            'endDate': end_date
        }]
        return daily_query