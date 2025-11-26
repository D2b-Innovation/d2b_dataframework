import pandas
from google.oauth2 import service_account
from tqdm import tqdm

class Google_Bigquery():
  def __init__(self,credentials_info=None,verbose=False):
      self.credentials_info = credentials_info
      if type(self.credentials_info) is not None:
        self.credentials = self._create_credentials(self.credentials_info)
      else:
        self.credentials = None
      self.verbose          = verbose

  def _create_credentials(self,credentials_info):
      if type(self.credentials_info) is None:
        raise ValueError('Credentials not provided')
      creds = service_account.Credentials.from_service_account_info(credentials_info,)
      return creds

  def debug(self,message):
      '''
      ARGS
      message <str>
      function to print debug msg
      '''
      if self.verbose:
        print(message)

  def _get_data(self, query, project_id):
      """
      Función para descargar datos desde BigQuery
      ARGS: 
      query: <str> query,
      project_id:  <str> project_id
      """
      self.debug('Downloading data from BigQuery...')
      bar_type = 'tqdm' if self.verbose else None

      try:
        dataframe = pandas.read_gbq(
            query,
            project_id=project_id,
            credentials=self.credentials,
            dialect='standard',
            progress_bar_type=bar_type
        )
        return dataframe
      
      except Exception as e:
        print(f'Error downloading data from BigQuery: {e}')
        return None

      except Exception as e:
        print(f'Error downloading data from BigQuery: {e}')
        return None

  def dataframe_clean_cols(self,dataframe):
      columns = dataframe.columns
      new_cols = []
      for column in columns:
          text = column
          text = text.lower()
          text = text.replace(" ","_")
          text = text.replace("ga:","")
          text = text.replace("&","_")
          text = text.replace("___","_")
          text = text.replace("ñ","n")
          new_cols.append(text)
      dataframe.columns = new_cols
      display(dataframe)
      return dataframe

  def clean_date(self,date):
    date = date.lower()
    date = date.replace(" ","_")
    date = date.replace("ga:","")
    date = date.replace("&","_")
    date = date.replace("___","_")
    date = date.replace("ñ","n")
    return date

  def upload(self,dataframe,date_column,destination,project_id,clean=True,if_exists="replace"):
      if clean:
          print("cleaning")
          dataframe = self.dataframe_clean_cols(dataframe)
          date_column = self.clean_date(date_column)
      dataframe[date_column] = dataframe[date_column].astype(str)

      unique_dates = dataframe[date_column].unique()

      if self.verbose:
          date_iterator = tqdm(unique_dates, desc='Uploading data by date') 
      else:
          date_iterator = unique_dates
          
      for date in date_iterator:
          self.debug('uploading {date} data to BigQuery...'.format(date=date))
          iter_df = dataframe[dataframe[date_column]==date]
          text_date = date.replace("-","")
          iter_df.to_gbq(destination+text_date,
                         project_id=project_id,
                         credentials=self.credentials,
                         if_exists=if_exists,
                         progress_bar=False
      )
      return
