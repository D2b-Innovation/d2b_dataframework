import pandas as pd
# CORRECCIÓN 1: Importamos correctamente la clase desde el módulo
from d2b_data.Google_Token_MNG import Google_Token_MNG 

# CORRECCIÓN 2: Borramos todos los imports de googleapiclient/oauth2/json que sobran

class Google_Spreadsheet:
  def __init__(self, credentials_path, url_id=None, use_service_account=False):
    self.credentials_path = credentials_path
    self.url_id  = url_id
    
    if use_service_account:
        # Modo ROBOT (Service Account)
        # El manager usa 'client_secret' como la ruta al key.json
        arg_secrets = credentials_path
        arg_token = None # En tu lógica nueva de SA, esto se permite ser None porque entra al if use_sa primero
    else:
        # Modo HUMANO (Legacy)
        # TRUCO: Para evitar que Token_MNG salte al modo Cloud Run (ADC),
        # debemos pasarle algo en ambos argumentos.
        
        # 1. 'token': Es tu archivo credentials_path real (el json con el token).
        arg_token = credentials_path
        
        # 2. 'client_secret': Le pasamos la misma ruta para que no sea None.
        # ¿Por qué funciona? Porque Token_MNG revisa si 'arg_token' existe como archivo.
        # Si existe (y existe), IGNORA el client_secret. Así que pasamos el mismo path para cumplir el protocolo.
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