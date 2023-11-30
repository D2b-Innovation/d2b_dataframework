import json
import requests
import urllib.parse
import pandas as pd

class Tiktok():
  def __init__(self,app_id,secret, token =None, debugEnabled=False):
    self.endpoint_base = "https://business-api.tiktok.com/open_api/v1.2"
    self.app_id = app_id
    self.secret = secret
    self.token  = token
    self.redirect  = ""
    self.auth_code = None
    self.debugEnabled= debug

  def _debug(self,msg):
    if self.debugEnabled:
      print(msg)

  def get_authorization_url(self,redirect_uri=None):
    '''
    '''
    if redirect_uri is None:
      raise Exception("redirect uri is required")

    #if redirect_uri is None and self.redirect_uri is None:
    #  raise Exception("redirect uri is required")


    #if redirect_uri is None :
    #  redirect_uri = self.redirect_uri
    redirect_uri = urllib.parse.quote_plus(redirect_uri)
    authorization_url = f"https://business-api.tiktok.com/portal/auth?app_id={self.app_id}&state=your_custom_params&redirect_uri={redirect_uri}"
    return authorization_url

  def set_auth_code(self,auth_code):
    '''
    '''

    self.auth_code = auth_code
    return self.auth_code

  def set_token(self,token):
    '''
    '''

    self.token = token
    return self.token

  def get_token(self,code):
    '''
    '''
    endpoint_url = f"{self.endpoint_base}/oauth2/access_token/"
    params = {
      "auth_code" : self.auth_code,
      "secret"    : self.secret,
      "app_id"    : self.app_id
    } 
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=endpoint_url, params=params ,headers= headers)
    if json.loads(response.content).get("code") == 40002:
      msg =  json.loads(response.content)
      raise Exception(f"""Unable to get Token, response:
      {msg} """)

    self._debug(response.content)
    return response.content
  
  def auth_flow(self,redirect_uri=None,force_reset = None):
    '''
    '''
    if self.token is not None:
      print("Token is provided")
      return True

    if redirect_uri is None:
      redirect_uri = self.redirect

    print("Access to the following adreess and get the code:")
    print(self.get_authorization_url(redirect_uri))
    code = input("insert code: ")


    return True

  def get_report(self,advertiser_id, dimensions, metrics, report_type="BASIC",lifetime="true",data_level = "AUCTION_AD"):
    '''
    '''
    report_base_URL= f'https://business-api.tiktok.com/open_api/v1.2/reports/integrated/get/'
    query = {
      "advertiser_id"     : f'{advertiser_id}',
      "report_type"       : report_type,
      "lifetime"         : lifetime,
      "data_level"        : data_level,
      "dimensions"        : dimensions,
      "metrcs"            : metrics}


    headers = {'Content-Type': 'application/json',
           'Access-Token' : self.token}

    report_requests = requests.get( url =report_base_URL  ,headers=headers, params=query)
    json_report_requests = json.loads(report_requests.content)
    self._debug(json_report_requests)

    if json_report_requests.get("code")  == 40002:
      print("No Results")
      raise Exception("an error occurred", "No data in request, if you want to skip this add skip=True", 42)
    return json_report_requests

  def get_report_dataframe(self,advertiser_id, dimensions, metrics, report_type="BASIC",lifetime="true",data_level = "AUCTION_AD"):
    '''
    '''
    report_requests = self.get_report(advertiser_id, dimensions, metrics, report_type,lifetime,data_level)
    report_data = report_requests.get("data",None).get("list",None)

    DF = pd.json_normalize(report_data)
    return  DF
    
     
  def _export_token(self,filename, token=None):
      if token is None:
        token= self.token
      with open(filename, 'w') as f:
          f.write(token)

  def _import_token(self,filename):
      f = open(filename, "r")
      object = f.read()
      retun_string = self.set_token(object)
      return retun_string

