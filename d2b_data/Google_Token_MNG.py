from googleapiclient.discovery import build
from google.oauth2 import service_account
from oauth2client import client
from builtins import input
from google_auth_oauthlib import flow

import httplib2
import webbrowser
import os
import json
import time
 
class Google_Token_MNG():
  '''

  '''
  def __init__(self,client_secret=None,token=None,api_name=None,api_version=None,scopes = None, use_service_account=False):
    self.scopes         = scopes
    self.client_secret  = client_secret
    self.token          = token
    self.api_name       = api_name
    self.version        = api_version
    self.use_sa         = use_service_account
    self.service        = self.create_api     (  api_name    = self.api_name,
                                                api_version  = self.version,
                                                secrets      = self.client_secret,
                                                credentials  = self.token, 
                                                scopes       = self.scopes,
                                                use_sa       = self.use_sa)

  def saveJson(self,filename, object):
      with open(filename, 'w') as f:
          json.dump(object, f)


  def openJson(self,filename):
      with open(filename, 'r') as f:
          object = json.load(f)
      return object


  def getCredentials(self,secrets, credentials, scopes):
      if not os.path.isfile(credentials):
          flow = client.flow_from_clientsecrets(
                  secrets,
                  scope=scopes,
                  redirect_uri='urn:ietf:wg:oauth:2.0:oob')
          auth_uri = flow.step1_get_authorize_url()
          print("Auth url: {}".format(auth_uri))
          webbrowser.open(auth_uri)
          time.sleep(3)
          auth_code = input('Enter the auth code: ')
          time.sleep(3)
          cre = flow.step2_exchange(auth_code)
          self.saveJson(credentials,cre.to_json())
      else:
          cre = client.Credentials.new_from_json(self.openJson(credentials))
      return cre


  def create_api(self,api_name, api_version, scopes=None, secrets=None, credentials=None, use_sa=False):
      """
      Crea el servicio de Google.
      Si use_sa=True: Usa Service Account (Server-to-Server).
      Si use_sa=False: Usa OAuth2 legacy (User-Consent).
      """
      if use_sa:
        if not secrets or not os.path.exists(secrets):
             raise ValueError(f"Para Service Account, 'secrets' debe ser la ruta al JSON válido. Ruta recibida: {secrets}")
          
        creds = service_account.Credentials.from_service_account_file(
            secrets,
            scopes=scopes
        )
        return build(api_name, api_version, credentials=creds, cache_discovery=False)
      
      if None in (secrets, credentials, scopes):
          return build(api_name, api_version)
      
           
      http_auth = self.getCredentials(secrets, credentials, scopes).authorize(httplib2.Http())
      return build(api_name, api_version, http=http_auth)

  def get_service(self):
      return self.service
