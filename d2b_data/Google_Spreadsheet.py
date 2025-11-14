from googleapiclient.discovery import build
import pandas as pd
from oauth2client import client
import json

class Google_Spreadsheet:
  def __init__(self,token,url_id=None):
    self.token   = token
    self.url_id  = url_id
    with open(token, 'r') as f:
        creds_json = json.load(f)
    self.cre = client.Credentials.new_from_json(creds_json)
    self.service = build('sheets', 'v4', credentials=self.cre)

  def get_spreadsheet(self):
    request = self.service.spreadsheets()
    return request

  def read_data_dataframe(self,spreadsheetId,range_name):
    request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_name)
    response = request.execute()
    df_response = pd.DataFrame(response.get('values'))
    df_response.columns = df_response.iloc[0]
    df_response.drop(df_response.index[0], inplace = True)
    return df_response

  def delete_data(self,sheetid,spreadsheetId,vector,start_index,end_index):
    '''
    Esta funcion permite eliminar data, puede ser una columna, una fila, un rango
    de celdas o un celda especifica.
    @sheetid representa el número de serie del spreadsheet
      *ejemplo:
        gid = --> 365570799.
    @vector es lo que representa una columna o una fila, solo puede tomar 2 valores
    'ROWS' o 'COLUMNS'.
    @start index indica el indice de inicio
    @end_indes indica el indice de termino
    '''
    body_request ={
      "requests": [
        {
          "updateCells": {
            "range": {
              "sheetId": sheetid
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
    '''
    Esta funcion permite insertar data en un rango.
    @spreadsheet_id representa el valor de la url que se encuentra despues del '/d/' y despues de '/edit#gid=0', este valor
    debe ser ingresado como string.
      *ejemplo:
      https://docs.google.com/spreadsheets/d/1fgzA98yaZmfUTrA0HaTz5PSXJ9ZZ5mSmDH9gLBzqXaw/edit#gid=0
      spreadsheet_id = '1fgzA98yaZmfUTrA0HaTz5PSXJ9ZZ5mSmDH9gLBzqXaw'

    @range_index indica en que hoja y celda se va a insertar la data.
      *ejemplo:
        'Sheet2!A1'


    @data_list es la lista(array=[[]]) de data que se va a agregar.
    '''
    body_request = {'values': data_list}
    self.service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_index, valueInputOption='USER_ENTERED', body= body_request).execute()
    print('Data agregada')
    return True

  def append_data(self, spreadsheet_id, range_index, data_list):
    '''
    Esta funcion permite insertar data en un rango.
    @spreadsheet_id representa el valor de la url que se encuentra despues del '/d/' y despues de '/edit#gid=0', este valor
    debe ser ingresado como string.
      *ejemplo:
      https://docs.google.com/spreadsheets/d/1fgzA98yaZmfUTrA0HaTz5PSXJ9ZZ5mSmDH9gLBzqXaw/edit#gid=0
      spreadsheet_id = '1fgzA98yaZmfUTrA0HaTz5PSXJ9ZZ5mSmDH9gLBzqXaw'

    @range_index indica en que hoja y celda se va a insertar la data.
      *ejemplo:
        'Sheet2!A1'


    @list es la lista(array=[[]]) de data que se va a agregar.
    '''
    print(data_list)
    body_request = {'values': data_list}
    self.service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=range_index, valueInputOption='USER_ENTERED', body= body_request).execute()
    print('Data agregada')
    return True
