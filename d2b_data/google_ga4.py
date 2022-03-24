from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google_auth_oauthlib import flow
import pickle
import pandas as pd
import os


class google_ga4():
  def __init__(self,client_secret,token_pickle, debug=False, unsampled=False,intraday_limit=30):
    self.client_secret = client_secret
    self.credentials   = self._get_token(self.client_secret,token_pickle)
    self.client        = self._create_client(self.credentials)
    self.debug_status  = debug
    self.unsampled     = unsampled
    self.intraday_limit = intraday_limit*100000

  def get_service(self):
    '''
    return the service object
    '''
    return self.service

  def get_token(self):
    '''
    return the token
    '''
    return self.token

  def get_credentials(self):
    '''
    return credentials
    '''
    return self.credentials

  def set_unsampled(self,unsampled=True):
    '''
    The usampled method split the data into daily query and in the case of multiple offsets in the same days will loop intradays
    ARGS
    unsampled <bool> True for unsampled or false for default method
    RETURNS
    <bool> returns the current state of sampled
    '''
    if type(unsampled) != bool:
      self.unsampled = unsampled
      return self.unsampled

  def _create_client(self,credentials):
    '''
    connect the service using the credentials provided
    ARGS
    credentials <obj> credential is the object that contains the autorization of the user
    RETURN
    service     <obj> service used in the function
    '''
    client = BetaAnalyticsDataClient(credentials=credentials)
    return client

  def _get_token(self,client_secret,token_pickle):
    '''
    To create the token and connect to the service
    ARGS
    client_secret <str>  path to the file where is located the client_secret
    token_pickle  <str>  path to the token pickle where the session will be stored
    RETURN
    <obj> Credentials
    '''
    SCOPES =  ['https://www.googleapis.com/auth/analytics.readonly']
    creds = None

    try:
        if os.path.exists(token_pickle):
            with open(token_pickle, 'rb') as token:
                creds = pickle.load(token)
            return creds
        if not creds or not creds.valid:
            flows = flow.InstalledAppFlow.from_client_secrets_file(client_secret, SCOPES)
            creds = flows.run_console()
            with open(token_pickle, 'wb') as token:
                pickle.dump(creds, token)
        return creds
    except Exception as e:
        print(e)
        return False
    return creds

  def _to_DF(self,response):
    '''
    Internal functions used to trasnform the GA4 report object to a dataFrame, this was created because the G4 object can't be cast
    ARGS
    response <obj> Response object provided by the librery of GA4
    RETURN
    <PD DataFrmae> return the GA4 object trasnformed to dataFrame
    '''
    cols = []
    for dimensions_cols in response.dimension_headers:
      cols.append(dimensions_cols.name)
    for metrics_cols in response.metric_headers:
      cols.append(metrics_cols.name)
    results = []
    for row in response.rows:
      row_array = []
      for dimension in row.dimension_values:
        row_array.append(dimension.value)
      for metrics in row.metric_values:
        row_array.append(metrics.value)
      results.append(row_array)
    return pd.DataFrame(results,columns=cols)

  def debug(self,message):
    '''
    ARGS
    message <str>
    function to print debug msg
    '''
    if self.debug_status:
      print(message)

  def get_report(self,property_id=None,dimensions=None,metrics=None,start_date=None,end_date=None,offset=None):
    '''
    Raw form of the reports, this can't be unsampled or offseted, this can be apply on the dataframe form
    ARGS
    property_id <STR> property provided by Google Analytics
    dimensions  <STR> list of dimensions split by comma
    metrics     <STR> list of metrics split by comma
    start_date  <STR> start date with formar yyyy-mm-dd
    end_date    <STR> end date with format yyyy-mm-dd
    offset      <INT> displacement value where the report start from
    RETURN
    report <objt> raw report
    '''
    self.debug(f'get_report | start')
    dimensions_array = []
    if dimensions.split(",") != ['']:

      for dimension in dimensions.split(","):
        dimensions_array.append(Dimension(name=dimension))


    metrics_array = []
    if metrics.split(",") != ['']:

      for metrics in metrics.split(","):
        metrics_array.append(Metric(name=metrics))

    property_id = property_id
    request = RunReportRequest(
      property=f"properties/{property_id}",
      dimensions=dimensions_array,
      metrics=metrics_array,
      date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
      limit = 100000,
      offset = offset)
    self.debug(f'get_report | downloading')
    response = self.client.run_report(request)
    self.debug(f'get_report | download {len(response.rows)}')
    if len(response.rows) == 100000:
      print(f"Warning Sampled Report {start_date} {end_date} with  {len(response.rows)}")
    return response

  def get_report_dataframe(self,property_id=None,dimensions=None,metrics=None,start_date=None,end_date=None):
    '''

    '''
    if self.unsampled:
      array_df = []
      self.debug(f"get_report unsampled")
      for date in [date.strftime('%Y-%m-%d') for date  in pd.date_range(start_date,end_date)]:
        offset = 0
        self.debug(f"get_report {date}")
        response = self.get_report(property_id,dimensions,metrics,date,date)
        # while (response.rows) == 10000:
        #   offset += offset 10000
        #   self.get_report(property_id,dimensions,metrics,date,date,offset=offset)
        array_df.append(self._to_DF(response))
      report = pd.concat(array_df)
      return report

    report = self.get_report(property_id,dimensions,metrics,start_date,end_date)
    return self._to_DF(report)
