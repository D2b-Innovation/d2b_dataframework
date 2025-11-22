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
    def __init__(self, metricas):
        self.metricas = metricas

    def generar_pronostico(self, df, dias):
        """Genera pronósticos para las métricas especificadas usando Prophet.
        
        ARGS: 
            df (DataFrame): DataFrame con las columnas 'date' y las métricas a predecir.
            dias (int): Número de días para los cuales se desea generar el pronóstico.
        RETURNS:
            DataFrame con las predicciones para las métricas especificadas.
        """
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
        
