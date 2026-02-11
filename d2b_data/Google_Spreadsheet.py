import pandas as pd
# CORRECCIÓN 1: Importamos correctamente la clase desde el módulo
from d2b_data.Google_Token_MNG import Google_Token_MNG 

# CORRECCIÓN 2: Borramos todos los imports de googleapiclient/oauth2/json que sobran

class Google_Spreadsheet:
  def __init__(self, credentials_path, url_id=None, use_service_account=False):
    self.credentials_path = credentials_path
    self.url_id  = url_id
    
    # CORRECCIÓN 1 (Uso): Llamamos directamente a la clase importada
    # Asumiendo que el archivo se llama Google_Token_MNG.py y la clase también
    self.token_manager = Google_Token_MNG(
        client_secret=credentials_path, 
        token=None,                     
        scopes=['https://www.googleapis.com/auth/spreadsheets'], 
        api_name='sheets', 
        api_version='v4',
        use_service_account=use_service_account 
    )
    
    self.service = self.token_manager.get_service()

  def get_spreadsheet(self):
    request = self.service.spreadsheets()
    return request

  def read_data_dataframe(self,spreadsheetId,range_name):
    # Agregué un try/except básico porque esto falla si el rango está vacío
    try:
        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_name)
        response = request.execute()
        
        if 'values' not in response:
            return pd.DataFrame() # Retorna vacío si no hay datos
            
        df_response = pd.DataFrame(response.get('values'))
        df_response.columns = df_response.iloc[0]
        df_response.drop(df_response.index[0], inplace = True)
        return df_response
    except Exception as e:
        print(f"Error leyendo data: {e}")
        return pd.DataFrame()

  def delete_data(self,sheetid,spreadsheetId,vector,start_index,end_index):
    '''
    ADVERTENCIA: Esta función actualmente ignora vector, start_index y end_index.
    Tal como está, BORRA TODO el contenido de la hoja especificada en sheetid.
    '''
    body_request ={
      "requests": [
        {
          "updateCells": {
            "range": {
              "sheetId": sheetid
              # FALTARIA AQUI DEFINIR startRowIndex, endRowIndex, etc.
              # Si no se ponen, Google asume toda la hoja.
            },
            "fields": "*"
          }
        }
      ]
    }
    self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body = body_request).execute()
    print('Data eliminada')
    return True

  def update_data(self, spreadsheet_id, range_index, data_list):
    # Corrección menor: valueInputOption es obligatorio
    body_request = {'values': data_list}
    self.service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, 
        range=range_index, 
        valueInputOption='USER_ENTERED', 
        body= body_request
    ).execute()
    print('Data actualizada')
    return True

  def append_data(self, spreadsheet_id, range_index, data_list):
    print(f"Agregando {len(data_list)} filas...")
    body_request = {'values': data_list}
    self.service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, 
        range=range_index, 
        valueInputOption='USER_ENTERED', 
        body= body_request
    ).execute()
    print('Data agregada')
    return True