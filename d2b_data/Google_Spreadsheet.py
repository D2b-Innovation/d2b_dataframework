import pandas as pd
# CORRECCIÓN 1: Importamos correctamente la clase desde el módulo
from d2b_data.Google_Token_MNG import Google_Token_MNG 

# CORRECCIÓN 2: Borramos todos los imports de googleapiclient/oauth2/json que sobran

class Google_Spreadsheet:
  def __init__(self, credentials_path, url_id=None, use_service_account=False):
    self.credentials_path = credentials_path
    self.url_id  = url_id
    
    if use_service_account:
        arg_secrets = credentials_path
        arg_token = None 
    else:
        arg_token = credentials_path
        arg_secrets = credentials_path
    self.token_manager = Google_Token_MNG(
        client_secret=arg_secrets, 
        token=arg_token,                     
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
    try:
        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_name)
        response = request.execute()
        
        if 'values' not in response:
            return pd.DataFrame() 
            
        df_response = pd.DataFrame(response.get('values'))
        df_response.columns = df_response.iloc[0]
        df_response.drop(df_response.index[0], inplace = True)
        return df_response
    except Exception as e:
        print(f"Error leyendo data: {e}")
        return pd.DataFrame()

  def delete_data(self,sheetid,spreadsheetId,vector='ALL', start_index=None, end_index=None, mode="VALUES"):
    '''
    Borra datos de la hoja.
    - Si vector='ALL': Borra todo el contenido de la hoja.
    - Si vector='ROWS': Borra el rango de filas especificado.
    - Si vector='COLUMNS': Borra el rango de columnas especificado.
    Ej: 
    - gs.delete_data(sheetid=0, spreadsheetId='abc123') <- borra todo.
    - gs.delete_data(sheetid=0, spreadsheetId='abc123', vector='ROWS', start_index=1, end_index=10) <- borra filas 1 a 9.
    - gs.delete_data(sheetid=0, spreadsheetId='abc123', vector='COLUMNS', start_index=1, end_index=5) <- borra columnas B a D. 
    '''
    
    grid_range = {
       "sheetId": sheetid
    }

    if vector.upper() =='ROWS':
      if start_index is not None:grid_range["startRowIndex"] = start_index
      if end_index is not None:grid_range["endRowIndex"] = end_index
    elif vector.upper() == 'COLUMNS':
      if start_index is not None:grid_range["startColumnIndex"] = start_index
      if end_index is not None:grid_range["endColumnIndex"] = end_index
    
    if mode.upper() == "VALUES":
       target_fields = "userEnteredValue"
    elif mode.upper() == "FORMAT":
       target_fields = "userEnteredFormat"
    else:
       target_fields = "*"

    body_request ={
      "requests": [
        {
          "updateCells": {
            "range": grid_range,
            "fields": target_fields,
          }
        }
      ]
    }
    self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body = body_request).execute()
    print(f'Data eliminada en el rango {start_index}:{end_index} ({vector})')
    return True

  def update_data(self, spreadsheet_id, range_index, data_list):
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