from googleapiclient.discovery import build
from google.oauth2 import service_account
from oauth2client import client
from builtins import input
from google_auth_oauthlib import flow
from oauth2client.client import GoogleCredentials

import httplib2
import google.auth
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

  def getCredentials(self, secrets, credentials, scopes, allow_adc=False):
    # 1. PRIORIDAD ABSOLUTA: Si existe el archivo de credenciales, usarlo siempre.
    if os.path.isfile(credentials):
        return client.Credentials.new_from_json(self.openJson(credentials))

    # 2. Si no hay archivo, validamos si estamos en Cloud Run
    if os.environ.get('K_SERVICE'):
        if allow_adc:
            # Obtiene las ADC usando la misma librería oauth2client para evitar incompatibilidades
            print("Usando Application Default Credentials (ADC) en Cloud Run...")
            return GoogleCredentials.get_application_default()
        else:
            raise RuntimeError(
                "¡Error Crítico! No se encontró el archivo de credenciales en Cloud Run y "
                "allow_adc es False. No se puede iniciar un flujo interactivo en la nube."
            )

    # 3. Solo si NO estamos en Cloud Run y NO hay archivo, iniciamos el flujo interactivo local
    flow = client.flow_from_clientsecrets(
        secrets,
        scope=scopes,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'
    )
    auth_uri = flow.step1_get_authorize_url()
    print("Por favor, visita esta URL para autorizar la aplicación:\n{}".format(auth_uri))

    try:
        webbrowser.open(auth_uri)
    except Exception as e:
        print(f"No se pudo abrir el navegador automáticamente: {e}")

    time.sleep(3)
    auth_code = input('\nIngresa el código de autorización: ')
    time.sleep(3)
    
    cre = flow.step2_exchange(auth_code)

    if credentials:
        self.saveJson(credentials, cre.to_json())
    
    return cre

  def create_api(self, api_name, api_version, scopes=None, secrets=None, credentials=None, use_sa=False):
    """
    Crea el servicio de Google. Dirige el tráfico según el tipo de autenticación.
    """
    # FLUJO 1: Cuentas de Servicio (Service Account / ADC) -> Librería Moderna
    if use_sa:
        if secrets and os.path.exists(secrets):
            creds = service_account.Credentials.from_service_account_file(
                secrets,
                scopes=scopes
            )
        else:
            creds, project = google.auth.default(scopes=scopes)
            print(f"Usando ADC (Cloud Run/Functions). Proyecto detectado: {project}")
            
        return build(api_name, api_version, credentials=creds, cache_discovery=False)
    
    # FLUJO 2: APIs públicas (sin credenciales y sin scopes específicos)
    if not use_sa and None in (secrets, credentials, scopes):
            return build(api_name, api_version)
    
    # FLUJO 3: Usuario Final (OAuth2) -> Librería Legacy
    creds = self.getCredentials(secrets, credentials, scopes)
    http_auth = creds.authorize(httplib2.Http())
    return build(api_name, api_version, http=http_auth)

  def get_service(self):
    return self.service
