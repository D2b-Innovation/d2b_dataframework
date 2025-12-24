import pandas as pd
from prophet import Prophet


# Aquí agregamos los imports que vamos a usar
class ProphetForecaster:
    """Class for generating forecasts using Prophet.

        Args:
            metrics (list): List of metrics to be predicted.

        Returns:
            DataFrame: A DataFrame containing the predictions for the specified metrics.
        """
    def __init__(self, metricas):
        self.df_beforepedict = df_to_predict
        self.df_postpredict  = None
        self.m = None
        self.display = None
        self.df_ready = None

    
    def _data_validation(self, df_topredict):
        """Validates the DataFrame data.

        Args:
            df_to_predict (DataFrame): The DataFrame to be validated.
        """
        date_options = ['date', 'fecha']

        metrics_in_df = [col for col in df_topredict.columns if col not in date_options]
        date_in_df = [col for col in df_topredict.columns if col in date_options]
        prophet_dataframe = df_topredict.copy()

        if not date_in_df:
            raise ValueError("No date columns found in the DataFrame. Please upload a 'date' or 'fecha' column.")

        date_to_predict = date_in_df[0]
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

    def generar_pronostico(self, df, dias):
        """Genera pronósticos para las métricas especificadas usando Prophet.
        
        ARGS: 
            df (DataFrame): DataFrame con las columnas 'date' y las métricas a predecir.
            dias (int): Número de días para los cuales se desea generar el pronóstico.
        RETURNS:
            DataFrame con las predicciones para las métricas especificadas.
        """
        # Agregar validador de columnas
        print(f"Estoy prediciendo las columnas: {self.metricas}")
        resultados = pd.DataFrame()
    
        # Generamos el bucle para iterar (Prophet es single metric)
        
        for metric in self.metricas:
            # Primero que todo, instanciamos la clase dentro del loop
            m = Prophet()
            # A. Imprimo la métrica que estoy entrenando
            print(f"     Entrenando modelo para: {metric}")
            
            # B. Preparo los datos, generando el sub DataFrame
            df_train = df[['date', metric]].rename(columns = {'date': 'ds', metric: 'y'})
            
            # C. Corro el model.fit para generar el aprendizaje
            m.fit(df_train)

            # D. Generamos la tabla para la predicción
            futuro = m.make_future_dataframe(periods=dias)

            # E. Corremos la predicción
            prediccion = m.predict(futuro)

            # F. Guardamos el resultado en variable resultados
            forecast_limpio = prediccion[['ds', 'yhat']].rename(columns = {'ds': 'date', 'yhat': metric})

            if resultados.empty:
                resultados = forecast_limpio
            else:
                resultados = pd.merge(forecast_limpio, resultados, on='date', how='outer')


            # D. Imprimo el ok de la métrica
            print(f" Forecast para {metric} por {dias} listo")

        return resultados.sort_values('date').round()
    

    # Modificaciones: Agregar Yhat lower YHat upper.
    # Una opción independiente en cada producto del forecast.
    # fig1 = m.plot(forecast) <- clase que retorna el plot. 
    # fig2 = m.plot_components(forecast)
    # Pasar DataFrame y no métricas en el init.


    




        
