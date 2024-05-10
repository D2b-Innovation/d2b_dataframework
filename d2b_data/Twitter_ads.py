#__AUTHOR = ["KemeN"]
#__version = alpha test release

import sys
import requests
from requests_oauthlib import OAuth1Session
import json

import time

import pandas as pd

from datetime import datetime, timezone,date,timedelta
from twitter_ads.client import Client
from twitter_ads.campaign import LineItem
from twitter_ads.enum import METRIC_GROUP,GRANULARITY
from twitter_ads.utils import split_list
from twitter_ads.http import Request


class Twitter_ads:
  def __init__(self,api_key,api_key_secret,access_token=None, access_token_secret=None , screen_name=None):
    ''' Method: __init__ (constructor)
    Description:
    This method is responsible for setting up the object's initial state by assigning values to its attributes.
    PARAMS
      @api_key            (str): A required string containing your Twitter API key. This key is used to authenticate your application with the Twitter API.
      @api_key_secret      (str): A required string containing your Twitter API key secret. Along with the API key, it's used for authentication.
      @access_token        (str, optional): An optional string containing your Twitter access token. If provided, it allows you to make authorized requests to the Twitter API on behalf of a specific user account.
      @access_token_secret (str, optional): An optional string containing your Twitter access token secret. Used in conjunction with the access token for authorized requests.
      @screen_name         (str, optional): An optional string specifying the Twitter screen name of the user account you want to query ad campaign performance for. If not provided, it may default to the authenticated user's account.
    --
    RETURN @None
    '''
    self.api_key  = self.consumer_key           = api_key
    self.api_key_secret = self.consumer_secret  = api_key_secret
    self.access_token        = access_token
    self.access_token_secret = access_token_secret
    self.screen_name         = screen_name
    self.user_id             = None
    self.client              = None

  def get_connection_vars(self):
    '''
    The get_connection_vars method of the Twitter class creates and returns a dictionary containing the essential Twitter API connection variables. 
    This dictionary can be used to configure other libraries or tools that interact with the Twitter API.
    --
    RETURN <DIC>
    '''
    return {'consumer_key'        : self.api_key,
            'consumer_secret'     : self.api_key_secret,
            'access_token'        : self.access_token,
            'access_token_secret' : self.access_token_secret,
            #'screen_name'        : self.screen_name,
            #'user_id'            : self.user_id
            }

  def set_connection(self,api_key=None,
                          api_key_secret=None,
                          access_token=None,
                          access_token_secret=None,
                          screen_name=None,
                          user_id=None,
                          consumer_key=None,
                          consumer_secret=None):
      ''' Method: __init__ (constructor)
      Description:
      Store and Stablish the conection with the Twitter's client
      PARAMS
        @api_key            (str): A required string containing your Twitter API key. This key is used to authenticate your application with the Twitter API.
        @api_key_secret      (str): A required string containing your Twitter API key secret. Along with the API key, it's used for authentication.
        @access_token        (str, optional): An optional string containing your Twitter access token. If provided, it allows you to make authorized requests to the Twitter API on behalf of a specific user account.
        @access_token_secret (str, optional): An optional string containing your Twitter access token secret. Used in conjunction with the access token for authorized requests.
        @screen_name         (str, optional): An optional string specifying the Twitter screen name of the user account you want to query ad campaign performance for. If not provided, it may default to the authenticated user's account.
      --
      RETURN @None
      '''    
      if api_key is not None:
        self.api_key  = api_key
      if api_key_secret is not None:
        self.api_key_secret     = api_key_secret
      if access_token is not None:
        self.access_token= access_token
      if access_token_secret is not None:
        self.access_token_secret= access_token_secret
      if screen_name is not None:
        self.screen_name= screen_name
      if user_id is not None:
        self.user_id= user_id

      self.client   = Client(**twitter.get_connection_vars())
      return self.get_connection_vars()

  def authenticate(self):
    '''  Flow to create the token provided by the user and used to authenticate the API
    '''
    resource_owner_oauth_token, resource_owner_oauth_token_secret = self.request_token()
    authorization_pin = self.get_user_authorization(resource_owner_oauth_token)
    access_token, access_token_secret, user_id, screen_name = self.get_user_access_tokens(resource_owner_oauth_token, resource_owner_oauth_token_secret, authorization_pin)

  # Request an OAuth Request Token. This is the first step of the 3-legged OAuth flow. This generates a token that you can use to request user authorization for access.
  def request_token(self):
    '''
    Initiates the OAuth 1.0a authorization process for the Twitter API. It obtains temporary credentials (request token and secret) that are used in the first step of user authorization.
    '''
    oauth = OAuth1Session(self.api_key, self.api_key_secret, callback_uri='oob')

    url = "https://api.twitter.com/oauth/request_token"

    try:
        response = oauth.fetch_request_token(url)
        resource_owner_oauth_token = response.get('oauth_token')
        resource_owner_oauth_token_secret = response.get('oauth_token_secret')
    except requests.exceptions.RequestException as e:
            print(e)
            sys.exit(120)

    return resource_owner_oauth_token, resource_owner_oauth_token_secret

  # Use the OAuth Request Token received in the previous step to redirect the user to authorize your developer App for access.
  def get_user_authorization(self,resource_owner_oauth_token):
      '''
      Initiates the user interaction part of the OAuth 1.0a authorization flow.
      It takes the temporary request token obtained from the request_token method and constructs a Twitter authorization URL.
      This URL is then presented to the user for authorization.
      '''

      authorization_url = f"https://api.twitter.com/oauth/authorize?oauth_token={resource_owner_oauth_token}"
      authorization_pin = input(f" \n Send the following URL to the user you want to generate access tokens for. \n → {authorization_url} \n This URL will allow the user to authorize your application and generate a PIN. \n Paste PIN here: ")

      return(authorization_pin)

  # Exchange the OAuth Request Token you obtained previously for the user’s Access Tokens.
  def get_user_access_tokens(self,resource_owner_oauth_token, resource_owner_oauth_token_secret, authorization_pin):
    '''
    Retrieves the long-lived access tokens and user information. If this success stores the values into the class
    It takes the temporary request token, secret, and the authorization PIN (verification code) obtained from the user.
    PARAMS
      @resource_owner_oauth_token (str): The temporary request token retrieved from the request_token method.
      @resource_owner_oauth_token_secret (str): The secret associated with the temporary request token.
      @authorization_pin (str): The verification code (PIN) entered by the user after authorization.
    --
    RETURN 
      @access_token (str): The long-lived access token for authorized requests.
      @access_token_secret (str): The secret associated with the access token.
      @user_id (str): The user ID of the authorized Twitter account.
      @screen_name (str): The screen name of the authorized Twitter account.
    '''
    oauth = OAuth1Session(  self.consumer_key,
                            client_secret=self.consumer_secret,
                            resource_owner_key=resource_owner_oauth_token,
                            resource_owner_secret=resource_owner_oauth_token_secret,
                            verifier=authorization_pin)

    url = "https://api.twitter.com/oauth/access_token"

    try:
        response = oauth.fetch_access_token(url)
        access_token = response['oauth_token']
        access_token_secret = response['oauth_token_secret']
        user_id = response['user_id']
        screen_name = response['screen_name']
    except requests.exceptions.RequestException as e:
            print(e)
            sys.exit(120)

    self.access_token_secret = access_token_secret
    self.access_token        = access_token
    self.user_id             = user_id
    self.screen_name         = screen_name
    export_token_filename    = "access_token_twitter_x.json"
    self._export_token(export_token_filename,json.dumps(self.get_connection_vars()))

    return(access_token, access_token_secret, user_id, screen_name)


  def summary_account(self,acc_id):
    '''
    Retrieves and summarizes information about a specified Twitter advertising account
    PARAMS
      acc_id (str): The ID of the Twitter advertising account to summarize.
    --
    RETURNS
      @PD.DataFrame (DF) Summary List of the accounts and campaigns of the prodivided object
    '''
    cids = self.client.accounts(acc_id)
    pd_list = list()
    for campaigns in cids.campaigns():
      account_params  = campaigns.account.to_params()
      account_params["account_id"] = account_params.pop("id")
      account_params["account_name"] = account_params.pop("name")
      campaign_params = campaigns.to_params()
      pd_list.append(pd.json_normalize(campaign_params | account_params))
    return pd.concat(pd_list)

  def get_report(self,account_id,metrics_groups,start_date,end_date,delay_in_seconds = 1):
    '''
    Retrieves Twitter Ads campaign performance reports for a specified account within a given date range. 
    It iterates through various combinations of metrics, dates, and campaigns to fetch individual reports and combines them into a list.
    PARAMS
      @account_id (str): The ID of the Twitter advertising account for which to generate reports.
      @metrics_groups (list): A list of metric groups to include in the reports. Each metric group represents a set of related metrics (e.g., impressions, clicks, conversions). The specific available metric groups depend on the Twitter Ads API.
      @start_date (str, format YYYY-MM-DD): The start date for the reporting period (inclusive).
      @end_date (str, format YYYY-MM-DD): The end date for the reporting period (inclusive).
      @delay_in_seconds (int, optional): The number of seconds to wait between API requests (default: 1). This helps avoid overwhelming the Twitter Ads API with too many requests at once.
    RETURNS
      @list() list populated wuth twitter iterable object
    '''
    rep_list = list()
    resource = f"/12/stats/accounts/{ACCOUNT_ID}/"  
    
    #get the list 
    account = self.client.accounts(account_id)
    cids = list(map(lambda x: x.id.encode('utf-8'), account.campaigns()))

    date_range = pd.date_range( start=start_date, end=end_date).to_list()


    delay_in_seconds =  delay_in_seconds
    total_loops  = len(metrics_groups)*len(date_range)*len(cids)
    current_loop = 0

    for metric_group in metrics_groups:
      time.sleep(delay_in_seconds)
      for iter_date in date_range:
        str_iter_date_start = iter_date.strftime('%Y-%m-%d')
        str_iter_date_end = (iter_date + timedelta(days=1)).strftime('%Y-%m-%d')
        for cid in cids:
          params = { 'entity':'CAMPAIGN',
            'entity_ids':  cid,
            'start_time':  str_iter_date_start,
            'end_time'  :  str_iter_date_end,
            'granularity':'DAY',
            'metric_groups': metric_group,
            'placement':"ALL_ON_TWITTER"}
          req =Request(self.client, 'get', resource, params=params)
          response = req.perform()
          rep_list.append( response.body )
          current_loop = current_loop+1
          print(f"{str_iter_date_start } - {cid} - {metric_group}  -  {round((current_loop/total_loops)*100)}%   | {current_loop} from {total_loops}")
          time.sleep(delay_in_seconds)
    return rep_list  


  def get_report_dataframe(self,account_id,metrics_groups,start_date,end_date,delay_in_seconds = 1):
    '''
    Transform @self.get_report() funtion to a DataFrame, cleanded with the summary account element merged
    PARAMS
      @account_id (str): The ID of the Twitter advertising account for which to generate reports.
      @metrics_groups (list): A list of metric groups to include in the reports. Each metric group represents a set of related metrics (e.g., impressions, clicks, conversions). The specific available metric groups depend on the Twitter Ads API.
      @start_date (str, format YYYY-MM-DD): The start date for the reporting period (inclusive).
      @end_date (str, format YYYY-MM-DD): The end date for the reporting period (inclusive).
      @delay_in_seconds (int, optional): The number of seconds to wait between API requests (default: 1). This helps avoid overwhelming the Twitter Ads API with too many requests at once.
    RETURNS
      @pd.DataFrame DF DataFrame populated with summary information,aggregated and clean
    '''

    twitter_query = {}

    twitter_query['account_id']         = account_id
    twitter_query['metrics_groups']     = metrics_groups
    twitter_query['start_date']         = start_date
    twitter_query['end_date']           = end_date
    twitter_query['delay_in_seconds']  = delay_in_seconds

    rep = self.get_report(**twitter_query)

    df_list = list()
    for row in rep:
      df_list.append(pd.json_normalize(row))
    DF = pd.concat(df_list)
    
    DF = self._clean_columns(DF)

    list_explode = []
    for index,row in DF.iterrows():
      if len(row["data"]) != 0 :
        iter_DF = pd.json_normalize(row["data"][0].get("id_data"))
        iter_DF = iter_DF.explode(iter_DF.columns.to_list())
        iter_DF = iter_DF.fillna(0)
        #Add values to the row
      else:
        iter_DF = pd.DataFrame([])
      iter_DF["start_time"]    = str(row["start_time"])
      iter_DF["placement"]     = str(row["placement"])
      iter_DF["platform"]      = str(row["platform"])
      iter_DF["entity"]        = str(row["entity"])
      iter_DF["metric_groups"] = str(row["metric_groups"][0])
      iter_DF["entity_ids"]     = str(row["entity_ids"][0])
      #add to the list
      list_explode.append(iter_DF)

    DF_metrics_group = pd.concat(list_explode)

    for column in DF_metrics_group.columns.to_list():
      if "metrics." in column :
        DF_metrics_group[column]= DF_metrics_group[column].fillna(0)
      else:
        DF_metrics_group[column] = DF_metrics_group[column].astype(str)

    summary = self.summary_account(account_id)
    summary = summary[["created_at","currency","id","name","timezone","account_id","account_name"]]

    DF_Final = pd.merge(DF_metrics_group, summary, 
            left_on='entity_ids', right_on='id',
            how='left')

    return DF_Final

  def _export_token(self,filename, token=None):
    if token is None:
      token= self.get_connection_vars()
    with open(filename, 'w') as f:
        f.write(token)

  def _import_token(self,filename):
      f = open(filename, "r")
      object = f.read()
      con_vars = json.loads(object)
      self.set_connection(**con_vars)
      return self.get_connection_vars()

  def _clean_columns(self,DF):
    new_cols = []
    for col in DF.columns:
      new_col = col.replace(r".","_")
      new_col = new_col.replace(r"/","")
      new_col = new_col.replace(r"|","")
      new_col = new_col.replace(r",","")
      new_col = new_col.replace("request_params_","")
      new_cols.append(new_col)
    DF.columns = new_cols
    return DF

