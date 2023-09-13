import pandas as pd
import json
import webbrowser
import httplib2
import os
import datetime

from googleapiclient.discovery import build
from oauth2client import client
from builtins import input
from google_auth_oauthlib import flow



class Google_GA4():
  def __init__(self,client_secret,token_json, debug=False, unsampled=False,intraday_limit=30):
    self.default_api_name       = 'analyticsdata'
    self.default_version        = 'v1beta'
    self.client_secret = client_secret
    self.service   = self.create_service(self.client_secret,token_json)
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
    service = build('analyticsdata', 'v1beta', credentials=creds)
    return service

  def create_service(self,secrets, credentials):
      ""
      token_mng = google_auth_mng(secrets,
                              credentials,
                              scopes = ['https://www.googleapis.com/auth/analytics.readonly'],  
                              api_version   = self.default_version ,
                              api_name      = self.default_api_name
                                   )
      self.service = token_mng.get_service()
      print("conected")
      return self.service

  def _to_DF(self,raw_server_response):
    '''
    Internal functions used to trasnform the GA4 report object to a dataFrame, this was created because the G4 object can't be cast
    ARGS
    response <obj> Response object provided by the librery of GA4
    RETURN
    <PD DataFrmae> return the GA4 object trasnformed to dataFrame
    '''
    response = raw_server_response.get("reports")[0]
    cols = []
    for dimensions_cols in response.get("dimensionHeaders"):
      cols.append(dimensions_cols.get("name"))
    for metrics_cols in response.get("metricHeaders"):
      cols.append(metrics_cols.get("name"))
    results = []
    for row in response.get("rows"):
      row_array = []
      for dimension in row.get("dimensionValues"):
        row_array.append(dimension.get("value"))
      for metrics in row.get("metricValues"):
        row_array.append(metrics.get("value"))
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

  def _get_report_raw(self,property_id,query):
        response = self.service.properties().batchRunReports(property=property_id, body=query).execute()
        return response
  
  def get_report_df(self,property_id,query):
      res = self._get_report_raw(property_id,query)
      DF_report = self._to_DF(res)
      display(DF_report)
      return DF_report
