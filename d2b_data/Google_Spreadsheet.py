from googleapiclient.discovery import build
from d2b_data.Google_Token_MNG import Google_Token_MNG
import pandas as pd
from oauth2client import client
from google.oauth2 import service_account
import json

class Google_Spreadsheet:
  def __init__(self, credentials_path, url_id=None, use_service_account=False):
    self.credentials_path = credentials_path
    self.url_id  = url_id
    
    # Delegamos la autenticación al Manager Transversal
    token_mng = Google_Token_MNG(
        client_secret=credentials_path, # En modo SA, esto es la ruta al key.json
        token=None,                     # En modo SA, no necesitamos token de usuario
        scopes=['https://www.googleapis.com/auth/spreadsheets'], 
        api_name='sheets', 
        api_version='v4',
        use_service_account=use_service_account # El flag que controla todo
    )
    
    self.service = token_mng.get_service()

  def get_spreadsheet(self):
    return self.service.spreadsheets()

  def read_data_dataframe(self, spreadsheetId, range_name):
    try:
        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_name)
        response = request.execute()
        
        # Validación básica por si la hoja está vacía
        if 'values' not in response:
            return pd.DataFrame()
            
        df_response = pd.DataFrame(response.get('values'))
        
        # Asumimos que la primera fila es el header
        if not df_response.empty:
            df_response.columns = df_response.iloc[0]
            df_response = df_response.drop(df_response.index[0])
            
        return df_response
    except Exception as e:
        print(f"Error leyendo spreadsheet: {e}")
        return pd.DataFrame()

  def delete_data(self, sheetid, spreadsheetId, vector, start_index, end_index):
    '''
    OJO: REVISAR Esta función parece incompleta. Recibes vector, start_index y end_index
    pero NO los usas en el body_request. Actualmente esto intenta borrar/resetear
    propiedades de la hoja entera, no un rango específico.
    '''
    body_request ={
      "requests": [
        {
          "updateCells": {
            "range": {
              "sheetId": sheetid
            },
            "fields": "*" # Esto resetea formato y valores
          }
        }
      ]
    }
    self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=body_request).execute()
    print('Data eliminada')
    return True

  def update_data(self, spreadsheet_id, range_index, data_list):
    body_request = {'values': data_list}
    self.service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, 
        range=range_index, 
        valueInputOption='USER_ENTERED', 
        body=body_request
    ).execute()
    print('Data actualizada')
    return True

  def append_data(self, spreadsheet_id, range_index, data_list):
    body_request = {'values': data_list}
    self.service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, 
        range=range_index, 
        valueInputOption='USER_ENTERED', 
        body=body_request
    ).execute()
    print('Data agregada (Append)')
    return True