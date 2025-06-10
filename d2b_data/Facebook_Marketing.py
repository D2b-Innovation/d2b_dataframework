import time
import math
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

class Facebook_Marketing:
    def __init__(self, app_id, app_secret, access_token, id_account=None, unsampled=False, debug=False, verbose_logger=None):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.unsampled = unsampled
        self.debug = debug
        self.id_account = id_account  # opcional
        self.verbose = verbose_logger if verbose_logger else self._null_verbose()
        self.service = FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)

    def _null_verbose(self):
        class DummyVerbose:
            def log(self, *args, **kwargs): pass
            def critical(self, *args, **kwargs): pass
        return DummyVerbose()

    def _debug(self, message):
        if self.debug:
            print(message)
        self.verbose.log(message)

    def get_report_dataframe(self, params, id_account=None):
        id_account = id_account or self.id_account
        if isinstance(id_account, list):
            return self.def_report_array_accounts(params, id_account)

        df_facebook = pd.DataFrame()
        act_id = f"act_{id_account}"

        if self.unsampled:
            self._debug("get_report_dataframe | Unsampled")
            unsampled_array = []
            date_range = pd.date_range(start=params["time_range"]["since"], end=params["time_range"]["until"])

            for idx, date in enumerate(date_range):
                str_date = date.strftime("%Y-%m-%d")
                params["time_range"] = {"since": str_date, "until": str_date}
                self.verbose.log(f"[{idx+1}/{len(date_range)}] Extrayendo día {str_date} para {id_account}")

                tries = 0
                max_tries = 5
                report = []

                start_time = time.time()

                while tries < max_tries:
                    try:
                        report = self.get_report(params, act_id)
                        break
                    except Exception as e:
                        wait = 2 ** tries
                        self.verbose.log(f"[{id_account}] Retry {tries+1}/{max_tries} para {str_date}. Esperando {wait}s: {e}")
                        time.sleep(wait)
                        tries += 1

                if not report:
                    self.verbose.critical(f"[{id_account}] No se pudo obtener datos para {str_date} tras {max_tries} intentos")
                    default_cols = params.get("fields", []) + params.get("breakdowns", []) + ["date_start", "date_stop", "account_id"]
                    df_day = pd.DataFrame(columns=default_cols)
                else:
                    df_day = pd.DataFrame(report)
                    if len(report) > 999:
                        self.verbose.critical(f"[{id_account}] Día {str_date}: Facebook devolvió más de 999 filas. Posible muestreo.")

                duration = round(time.time() - start_time, 2)
                self.verbose.log(f"[{id_account}] {str_date} completado en {duration} segundos")

                unsampled_array.append(df_day)

                self._debug(f"get_report_dataframe | Report size {len(report)}")

            df_facebook = pd.concat(unsampled_array)

        else:
            report = self.get_report(params, act_id)
            if len(report) == 0:
                default_cols = params.get("fields", []) + params.get("breakdowns", []) + ["date_start", "date_stop", "account_id"]
                df_facebook = pd.DataFrame(columns=default_cols)
            else:
                df_facebook = pd.DataFrame(report)

        # Procesar acciones
        actions_dict = self._unique_actions(df_facebook)
        for column, actions in actions_dict.items():
            for action in actions:
                df_facebook[f"_action_{action}"] = df_facebook[column].apply(lambda x: self._split_text(x, action))

        return df_facebook

    def get_report(self, params, act_id, max_tries=10):
        my_account = AdAccount(act_id)
        for attempt in range(max_tries):
            try:
                async_job = my_account.get_insights(params=params, is_async=True)
                self._debug(f"get_report | Intento {attempt+1} - Job lanzado correctamente")
                break
            except Exception as e:
                self.verbose.critical(f"get_report | Error al iniciar job: {e}")
                time.sleep(2 ** attempt)
        else:
            raise Exception("get_report | No se pudo iniciar el job después de múltiples intentos")

        time.sleep(10)
        tries = 0
        while tries < 60:
            status = async_job.api_get().get('async_status', '')
            if status == 'Job Completed':
                self._debug("get_report | Job completado")
                return async_job.get_result()
            elif status == 'Job Failed':
                raise Exception("get_report | Job falló en el servidor de Meta")
            else:
                self._debug(f"get_report | Esperando... intento {tries+1}")
                time.sleep(20)
                tries += 1

        raise TimeoutError("get_report | Timeout esperando el job")

    def def_report_array_accounts(self, params, id_accounts):
        self._debug("def_report_array_accounts | Procesando múltiples cuentas")
        array_df = []
        for acc in id_accounts:
            self._debug(f"Cuenta: {acc}")
            df = self.get_report_dataframe(params, acc)
            array_df.append(df)
        return pd.concat(array_df)

    def _unique_actions(self, df):
        self._debug("_unique_actions")
        actions_per_column = {}
        for column in df.columns:
            if df[column].apply(lambda x: isinstance(x, list)).any():
                actions_per_column[column] = set()
                for all_actions in df[column].dropna():
                    for action in all_actions:
                        if "action_type" in action:
                            actions_per_column[column].add(action["action_type"])
        return actions_per_column

    def _split_text(self, text, action):
        if not isinstance(text, list):
            return 0
        for element in text:
            if element.get("action_type") == action:
                return element.get("value", 0)
        return 0