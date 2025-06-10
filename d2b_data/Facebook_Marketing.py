import time
import math
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

class Facebook_Marketing:
    def __init__(self, app_id, app_secret, access_token, unsampled=False, verbose_logger=None):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.unsampled = unsampled
        self.verbose = verbose_logger if verbose_logger else self._null_verbose()
        self.cache_report = None
        self.max_tries = 500
        FacebookAdsApi.init(app_id, app_secret, access_token)

    def _null_verbose(self):
        class DummyVerbose:
            def log(self, *args, **kwargs): pass
            def critical(self, *args, **kwargs): pass
        return DummyVerbose()

    def get_report(self, params, id_account):
        try:
            my_account = AdAccount(id_account)
            async_job = my_account.get_insights(params=params, is_async=True)
            self.verbose.log(f"[get_report] Descargando reporte para cuenta {id_account}")
            tries = 0
            while async_job.api_get().get('async_status', '') != 'Job Completed' and tries <= self.max_tries:
                self.verbose.log(f"[get_report] Esperando reporte... intento {tries + 1}")
                time.sleep(3)
                tries += 1

            result_job = async_job.get_result()
            return result_job
        except Exception as e:
            self.verbose.critical(f"[get_report] Error en la extracción del reporte: {str(e)}")
            return []

    def get_report_dataframe(self, params, id_account):
        if isinstance(id_account, list):
            return self._report_multiple_accounts(params, id_account)

        df_facebook = pd.DataFrame()

        if self.unsampled:
            self.verbose.log("[get_report_dataframe] Extrayendo datos no muestreados (unsampled)")
            date_range = pd.date_range(start=params["date_start"], end=params["date_stop"])
            unsampled_array = []

            for date in date_range:
                str_date = date.strftime("%Y-%m-%d")
                params["time_range"] = {"since": str_date, "until": str_date}
                self.verbose.log(f"[get_report_dataframe] Día: {str_date}")
                report = self.get_report(params, id_account)
                if not report:
                    self.verbose.log(f"[get_report_dataframe] Día {str_date} sin datos.")
                    empty_cols = params.get("fields", []) + params.get("breakdowns", []) + ["date_start", "date_stop", "account_id"]
                    df_day = pd.DataFrame(columns=empty_cols)
                else:
                    df_day = pd.DataFrame(report)

                unsampled_array.append(df_day)

            df_facebook = pd.concat(unsampled_array, ignore_index=True)

        else:
            report = self.get_report(params, id_account)
            if not report:
                empty_cols = params.get("fields", []) + params.get("breakdowns", []) + ["date_start", "date_stop", "account_id"]
                df_facebook = pd.DataFrame(columns=empty_cols)
            else:
                df_facebook = pd.DataFrame(report)

        # Procesamiento de acciones si existen
        if "actions" in df_facebook.columns:
            for action in self._unique_actions(df_facebook):
                df_facebook[f"_action_{action}"] = df_facebook["actions"].apply(lambda x: self._split_text(x, action))
            df_facebook.drop(columns=["actions"], inplace=True)

        return df_facebook

    def _report_multiple_accounts(self, params, id_accounts):
        array_df = []
        for account in id_accounts:
            self.verbose.log(f"[get_report_dataframe] Extrayendo datos para cuenta: {account}")
            df = self.get_report_dataframe(params, account)
            array_df.append(df)
        return pd.concat(array_df, ignore_index=True)

    def _unique_actions(self, df):
        if "actions" not in df:
            return []
        unique_actions = set()
        for actions in df["actions"].fillna(""):
            for action in actions:
                if "action_type" in action:
                    unique_actions.add(action["action_type"])
        return unique_actions

    def _split_text(self, text, action):
        if not isinstance(text, list):
            return 0
        for item in text:
            if item.get("action_type") == action:
                return item.get("value", 0)
        return 0