import pandas as pd
from prophet import Prophet


# Aquí agregamos los imports que vamos a usar
class ProphetForecaster:
    """Clase para generar pronósticos usando Prophet.

    ARGS:
        metricas (list): Lista de métricas a predecir.
    RETURNS:
        DataFrame con las predicciones para las métricas especificadas.
    """
    def __init__(self, df_topredict):
        self.df_beforepredict = df_topredict
        # añadir validar
        self.models = {}
        self.display = None
        self.df_ready = None
        self.df_postpredict = None

    def _data_validation(self, df_topredict):
        """Validates the DataFrame data.

        Args:
            df_to_predict (DataFrame): The DataFrame to be validated.
        """
        date_options = ['date', 'fecha']

        date_in_df = [col for col in df_topredict.columns if col in date_options]
        if not date_in_df:
            raise ValueError("No date columns found in the DataFrame. Please upload a 'date' or 'fecha' column.")
        date_to_predict = date_in_df[0]

        metrics_in_df = [col for col in df_topredict.columns if col not in date_options]

        prophet_dataframe = df_topredict.copy()

        # 3. Strict Type Checking
        for col in metrics_in_df:
            if not pd.api.types.is_numeric_dtype(prophet_dataframe[col]):
                raise TypeError(
                    f"\n[INTEGRITY ERROR]: Column '{col}' contains string or non-numeric data.\n"
                    f"Prophet requires numeric values to forecast. Please remove dimensions "
                    f"(like keywords, pages, or categories) and only pass date and numeric metrics."
                )

        prophet_dataframe[date_to_predict] = pd.to_datetime(prophet_dataframe[date_to_predict], format='%Y-%m-%d')
        prophet_dataframe.rename(columns={date_to_predict: 'ds'}, inplace=True)
        print( f" Using {date_to_predict} as date column")

        for col in metrics_in_df:
            prophet_dataframe[col] = pd.to_numeric(prophet_dataframe[col], errors='coerce')
            if prophet_dataframe[col].isnull().any():
                print(f" Column {col} has Null values, check for data integrity")

        new_date = ['ds']
        df_final_cols = new_date + metrics_in_df
        prophet_dataframe = prophet_dataframe[df_final_cols]
        self.df_ready = prophet_dataframe

    def get_forecast(self, days):
        """Generates forecasts for the specified metrics using Prophet.

        Args:
            df (DataFrame): DataFrame containing 'ds' and the metric columns to forecast.
            days (int): Number of days to generate the forecast for.

        Returns:
            DataFrame: A DataFrame containing the predictions for the specified metrics.
        """
        self._data_validation(self.df_beforepredict)
        results = pd.DataFrame()

        for metric in self.df_ready.columns[1:]:
            print(f" Predicting: {metric}...")

            df_train = self.df_ready[['ds', metric]].rename(columns = {metric: 'y'})
            print(f" Column {metric} renamed succesfully to 'y'")
            m = Prophet()
            print(f" Class Prophet instanciated")
            m.fit(df_train)
            print(f" Training successull")
            future = m.make_future_dataframe(periods=days)
            forecast = m.predict(future)
            print(f" future dates calculated")
            forecast_clean = forecast[['ds', 'yhat']].rename(columns = {'ds': 'date', 'yhat': metric})

            self.models[metric] = m

            if results.empty:
                results = forecast_clean
            else:
                results = pd.merge(forecast_clean, results, on='date', how='outer')
            print(f" Forecast para {metric} por {days} listo")
        self.df_postpredict = results.sort_values('date').round()

        print(f" Forecast ready for {self.df_postpredict.columns.to_list()}")
        return self.df_postpredict
        
