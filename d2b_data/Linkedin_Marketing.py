import pandas as pd
import json
import requests
from os.path import exists


APPLICATON_KEY    = '771o2l44gxkg0e'
APPLICATON_SECRET = 'wLuLB6NdVdne9CCe'

class Linkedin_Marketing():
  def __init__(self,APPLICATON_KEY,APPLICATON_SECRET,verbose=False,verbose_level=False):
    '''
    '''

    self.APPLICATON_KEY    = APPLICATON_KEY
    self.APPLICATON_SECRET = APPLICATON_SECRET
    self.RETURN_URL        = 'https://localhost:8888'
    self.SCOPE             =  ['r_organization_social','r_ads_reporting','r_basicprofile','r_ads','w_member_social','w_organization_social']
    self.SCOPE_str         =   "%20".join(self.SCOPE)
    self.OAUTH_URL         = f'https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id={self.APPLICATON_KEY}&redirect_uri={self.RETURN_URL}&state=foobar&scope={self.SCOPE_str}'
    self.HEADERS           = None
    self.token             = None
    self.verbose           = verbose
    self.verbose_level     = verbose_level

  def get_url(self):

    return self.OAUTH_URL

  def set_headers_token(self,token_header):
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = f"Bearer {token_header}"
    self.HEADERS = headers
    return self.HEADERS


  def get_token(self,code=None,filename=None):
    '''

    '''
    #condition if the file exist
    #in this case the code will load the file
    # if filename is not None and not exists(filename):
    #     raise ValueError('file not found: '+ filename)

    if filename is not None and exists(filename):
      with open(filename) as json_file:
        json_content = json.load(json_file)
      access_token = json_content.get('access_token')
      self.set_headers_token(access_token)
      print(access_token)
      return self.HEADERS

    url_token_endpoint = "https://www.linkedin.com/oauth/v2/accessToken"
    headers = CaseInsensitiveDict()
    params = CaseInsensitiveDict()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    params["grant_type"]    = "authorization_code"
    params["code"]          = f"{code}"
    params["redirect_uri"]  = f"{self.RETURN_URL}"
    params["client_id"]     = f"{self.APPLICATON_KEY}"
    params["client_secret"] = f"{self.APPLICATON_SECRET}"
    res_token = requests.post(url_token_endpoint,params=params,headers=headers)
    if json.loads(res_token.content).get("error",None) is not None:
      raise ValueError('Error getting token: '+str(res_token.content))
    print(res_token.content)
    json_token = json.loads(res_token.content)
    if json_token.get('access_token',False):
      if filename is not None:
        with open(filename, 'w') as f:
          json.dump(json_token, f)
      self.token = json_token.get('access_token')
      self.set_headers_token(json_token.get('access_token'))

    else:
      raise ValueError('Error getting token:'+res_token.content)
    return self.token


  def get_report(self,account_id,start,end,metrics):
    pivot= "CREATIVE"
    time = "DAILY"
    account_id = f"urn%3Ali%3AsponsoredAccount%3A{account_id}"

    start.split("-")
    start = start.split("-")
    end   = end.split("-")
    start = f"&dateRange.start.day={start[2]}&dateRange.start.month={start[1]}&dateRange.start.year={start[0]}"
    end   = f"&dateRange.end.day={end[2]}&dateRange.end.month={end[1]}&dateRange.end.year={end[0]}"
    metrics = metrics+",pivot,pivotValues"
    text = ''
    if pivot !="":
      for idx, val in enumerate(pivot.split(",")):
        text +=f"pivots[{idx}]={val}&"
    text

    url = f'https://api.linkedin.com/v2/adAnalyticsV2?q=statistics&{text}&timeGranularity={time}&{start}{end}&accounts={account_id}&fields={metrics}'
    res = requests.get(url,headers=lk.HEADERS)
    if res.status_code != 200:
      raise Exception(res.content)
    return res.content

  def _clean_and_transform_dataFrame(self,res):
    res = json.loads(res)
    DF = pd.json_normalize(res.get("elements"))

    #creating column date if is needed
    if "dateRange.start.day" in DF.columns:
      DF["date"] = pd.to_datetime(DF["dateRange.start.year"].astype(str)+"-"+DF["dateRange.start.month"].astype(str)+"-"+DF["dateRange.start.day"].astype(str),format='%Y-%m-%d')

      #define the columns to be drop
      date_cols = ["dateRange.start.month",
                "dateRange.start.day",
                "dateRange.start.year",
                "dateRange.end.month",
                "dateRange.end.day",
                "dateRange.end.year"]
      #drop cols
      for col in date_cols:
        if col in DF.columns:
          DF = DF.drop(columns=col)

    #due d2b_standar we need all the columns in lowecase
    DF.columns = [x.lower() for x in DF.columns]
    return DF

  def _debug(self,msg):
    if self.verbose and self.verbose_level==3:
      print(msg)
      return

    if self.verbose:
      print(msg)
    return

  def get_report_dataframe(self,account_id,start,end,metrics,unsampled=False):
    if unsampled:
      self._debug("unsampled")
      date_range = pd.date_range(start,end,freq='D')
      array_reports = []
      for date in date_range.strftime('%Y-%m-%d'):

        self._debug(date)
        res = self.get_report(account_id,date,date,metrics)
        res = self._clean_and_transform_dataFrame(res)
        array_reports.append(res)
      return pd.concat(array_reports)
    else:
      res = self.get_report(account_id,start,metrics,end)
      self._clean_and_transform_dataFrame(self.get_report)
