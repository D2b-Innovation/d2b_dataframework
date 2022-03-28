import math
import time
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.campaign import Campaign as AdCampaign
from facebook_business.adobjects.adsinsights import AdsInsights
import pandas as pd
import math



class Facebook_Marketing():
  def __init__(self, app_id,app_secret ,access_token,debug=False,unsampled=False,debug_level=0):
    '''
    args
    app_id       <str>   App id provided by facebook
    app_secret   <str>   app secret provided by facebook
    access_token <str>   temporal string provided by facebook to logging
    return @NONE
    '''
    self.app_id       = app_id
    self.app_secret   = app_secret
    self.access_token = access_token
    self.cache_report = None
    self.max_tries    = 500
    self.service      = FacebookAdsApi.init(app_id, app_secret, access_token)
    self.debug        = debug
    self.unsampled    = unsampled
    self.debug_level  = debug_level

  def get_debug(self):
    '''
    '''
    return self.debug
  def get_debug_level(self):
    '''
    '''
    return self.debug_level

  def get_cache_report(self):
    '''
    '''
    return self.cache_report

  def get_access_token(self):
    '''
    '''
    return self.access_token

  def get_app_id(self):
    '''
    '''
    return self.app_id

  def get_service(self):
    '''
    '''
    return self.service
  def get_app_secret(self):
    '''
    '''
    return self.app_secret

  def set_service(self,app_id):
    '''
    '''
    if type(self,app_id) == str:
      self.app_id = app_id
    else:
     raise ValueError('app_id must be a string')
  def _debug(self,message,level=0 ):
    if self.debug:
      print(message)

  def get_report(self,params,id_account):
    my_account = AdAccount(id_account)
    async_job = my_account.get_insights(params=params, is_async=True)
    tries = 0
    ''' Areglar esta parte OMAR '''
    #while async_job.api_get().get('async_status','') != 'Job Completed' or tries<=self.max_tries:
    self._debug(f'get_report | Starting Download Report ')
    while async_job.api_get().get('async_status','') != 'Job Completed':
      self._debug(f'get_report | {tries+1} -> {self.max_tries}')
      self._debug(async_job.api_get())
      time.sleep(3)
      async_job.api_get()
      tries +=1
    result_job = async_job.get_result()
    return result_job

  def get_report_dataframe(self,params,id_account):
    '''


    '''
    if type(id_account) == list:
      self._debug("Report Type List")
      #This line is to jump to the multiple id accounts
      return self.def_report_array_accounts(params,id_account)

    df_facebook = pd.DataFrame() # create an empty element as defalt element

    # we detected 2 cases, the unsampled reports, that means multiple query to the same account, daily and itraday,
    # when this is complete we concat both dataframe , the second case is the sampled report, that is direct  query
    if self.unsampled:
      self._debug(f"get_report_dataframe | Unsampled")
      unsampled_array = []
      date_range = pd.date_range(start=date_start, end=date_stop)
      for date in date_range:
        str_date = date.strftime("%Y-%m-%d")
        if self.debug:
          print(f'{str_date}')
        params["time_range"] = {'since':str_date,'until':str_date}
        print(f'{params}')
        report = self.get_report(params,id_account)
        self._debug(f"get_report_dataframe | Report size {len(report)}")
        if len(report) > 999:
          print("warning Report Sampled")
        if len(report) == 0:
          column_defalult_facebook = params.get("fields") + params.get("breakdowns") + ["date_start", "date_stop", "account_id"]
          df_facebook = pd.DataFrame(columns=column_defalult_facebook)
        else:
          df_facebook = pd.DataFrame(report)

        unsampled_array.append(df_facebook)
      df_facebook = pd.concat(unsampled_array)

    else:
      report = self.get_report(params,id_account)
      if len(report) == 0:
        column_defalult_facebook = params.get("fields") + params.get("breakdowns") + ["date_start", "date_stop", "account_id"]
        df_facebook = pd.DataFrame(columns=column_defalult_facebook)
      else:
        df_facebook = pd.DataFrame(report)

    #unest the action field comming from Facebook
    if "actions" in df_facebook:
        for unique_action in self._unique_actions(df_facebook):
          df_facebook["_action_"+unique_action] = df_facebook["actions"].apply(lambda x :self._split_text(x,unique_action))
        df_facebook = df_facebook.drop(columns="actions")
    return df_facebook

  def def_report_array_accounts(self,params,id_account):
    self._debug("list in")
    array_df = []
    for account in id_account:
      if self.debug :
        print(f"{account}")
      DF_results = self.get_report_dataframe(params,account)
      #DF_results["id_account"] = account
      array_df.append(DF_results)
    return pd.concat(array_df)

  def _unique_actions(self,df):
    '''
    The functions is designed to get the unique values for the action_type field
    args <DataFrame>  DataFrame with action_type columns
    return
    set unique actions presented in the DataFrame
    '''
    if "actions" not in df:
        return []
    unique_actions = set()
    for all_actions in df["actions"].fillna(""):
      for single_action in all_actions:
        unique_actions.add(single_action.get("action_type"))
    return unique_actions

  def _split_text(self,text,action):
    if type(text)==float or type(text)==int and math.isnan(text):
      return 0
    else:
      for elements in text:
        if elements.get("action_type") == action:
          return elements.get("value")
    return 0

import math
def split_text(text,action):
  if type(text)==float or type(text)==int and math.isnan(text):
    return 0
  else:
    for elements in text:
      if elements.get("action_type") == action:
        return elements.get("value")
  return 0
