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
    self.debugEnabled= debugEnabled

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
    self._debug(f"get_token : Start with {code}")
    endpoint_url = f"{self.endpoint_base}/oauth2/access_token/"
    params = {
      "auth_code" : code,
      "secret"    : self.secret,
      "app_id"    : self.app_id
    } 
    
    headers = {'Content-Type': 'application/json'}
    self._debug(f"""get_token | start request | {endpoint_url} + {params}""")

    response = requests.post(url=endpoint_url, params=params ,headers= headers)
    
    if json.loads(response.content).get("code") == 40002:
      msg =  json.loads(response.content)
      raise Exception(f"""Unable to get Token, response:
      {msg} """)
    response_json = json.loads(response.content)
    self._debug(f"""get_token | end request  | response {response_json}""")
    token =  response_json.get("data",{}).get("access_token")
    self.token = token
    self._debug(f"""get_token | END | {self.token}""")

    return self.token 
  
  def auth_flow(self,redirect_uri=None,force_reset = False,token_filename="tiktok.token"):
    '''
    '''
    self._debug(f"auth_flow | start")

    if force_reset is False:
      if self.token is not None:
        print("Token is provided")
        return True

      if redirect_uri is None:
        redirect_uri = self.redirect

    print("Access to the following adreess and get the code:")
    print(self.get_authorization_url(redirect_uri))
    code = input("insert code: ")

    
    token = self.get_token(code)
    self._export_token(token_filename,token)
    self._debug(f"auth_flow: END with token = {token}")
    return token

  def get_report(self,advertiser_id, dimensions, metrics, report_type="BASIC",lifetime="true",data_level = "AUCTION_AD",start_date=None,end_date=None,skip=False):
    '''
    '''
    self._debug("get_report | starting query")
    report_base_URL= f'https://business-api.tiktok.com/open_api/v1.2/reports/integrated/get/'
    query = {
      "advertiser_id"     : f'{advertiser_id}',
      "report_type"       : report_type,
      "lifetime"          : lifetime,
      "data_level"        : data_level,
      "dimensions"        : dimensions,
      "metrics"           : metrics}

    if start_date is not None:
      query["start_date"] = start_date
    if end_date is not None:
      query["end_date"] = end_date

    headers = {'Content-Type': 'application/json',
           'Access-Token' : self.token}
    self._debug(query)
    report_requests = requests.get( url =report_base_URL  ,headers=headers, params=query)
    json_report_requests = json.loads(report_requests.content)
    self._debug(report_requests.url)
    self._debug("get_report | quering")
    self._debug(report_requests.content)


    if json_report_requests.get("code")  == 40002:
      print("No Results")
      raise Exception(json_report_requests.content, 42)
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

