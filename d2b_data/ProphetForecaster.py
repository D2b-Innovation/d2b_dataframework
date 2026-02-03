import pandas as pd
from prophet import Prophet
import pickle
import os
from pathlib import Path


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
            print(f" Forecast para {metric} por {days} días listo")
        self.df_postpredict = results.sort_values('date').round()

        print(f" Forecast ready for {self.df_postpredict.columns.to_list()}")
        return self.df_postpredict
        
    def save_models(self, directory='prophet_models'):
        """Guarda todos los modelos entrenados en archivos pickle.
        
        Args:
            directory (str): Directorio donde se guardarán los modelos.
                           Por defecto crea una carpeta 'prophet_models'.
        
        Returns:
            dict: Diccionario con las rutas de los archivos guardados.
        """
        if not self.models:
            raise ValueError("No hay modelos entrenados. Ejecuta get_forecast() primero.")
        
        # Crear directorio si no existe
        Path(directory).mkdir(parents=True, exist_ok=True)
        
        saved_files = {}
        
        for metric, model in self.models.items():
            filename = f"{metric}_model.pkl"
            filepath = os.path.join(directory, filename)
            
            with open(filepath, 'wb') as f:
                pickle.dump(model, f)
            
            saved_files[metric] = filepath
            print(f" Modelo para '{metric}' guardado en: {filepath}")
        
        print(f"\n✓ {len(saved_files)} modelos guardados exitosamente")
        return saved_files
    
    def load_models(self, directory='prophet_models', metrics=None):
        """Carga modelos previamente guardados desde archivos pickle.
        
        Args:
            directory (str): Directorio donde están guardados los modelos.
            metrics (list, optional): Lista de métricas específicas a cargar.
                                    Si es None, carga todos los modelos disponibles.
        
        Returns:
            dict: Diccionario con los modelos cargados.
        """
        if not os.path.exists(directory):
            raise FileNotFoundError(f"El directorio '{directory}' no existe.")
        
        # Si no se especifican métricas, cargar todos los archivos .pkl
        if metrics is None:
            model_files = [f for f in os.listdir(directory) if f.endswith('_model.pkl')]
            metrics = [f.replace('_model.pkl', '') for f in model_files]
        
        loaded_models = {}
        
        for metric in metrics:
            filename = f"{metric}_model.pkl"
            filepath = os.path.join(directory, filename)
            
            if not os.path.exists(filepath):
                print(f"⚠ Advertencia: No se encontró el modelo para '{metric}' en {filepath}")
                continue
            
            with open(filepath, 'rb') as f:
                model = pickle.load(f)
            
            loaded_models[metric] = model
            print(f" Modelo para '{metric}' cargado desde: {filepath}")
        
        self.models = loaded_models
        print(f"\n✓ {len(loaded_models)} modelos cargados exitosamente")
        return loaded_models
    
    def predict_from_loaded_models(self, days):
        """Genera predicciones usando modelos previamente cargados.
        
        Args:
            days (int): Número de días a predecir.
        
        Returns:
            DataFrame: DataFrame con las predicciones.
        """
        if not self.models:
            raise ValueError("No hay modelos cargados. Usa load_models() primero.")
        
        results = pd.DataFrame()
        
        for metric, model in self.models.items():
            print(f" Prediciendo con modelo cargado: {metric}...")
            
            future = model.make_future_dataframe(periods=days)
            forecast = model.predict(future)
            forecast_clean = forecast[['ds', 'yhat']].rename(columns={'ds': 'date', 'yhat': metric})
            
            if results.empty:
                results = forecast_clean
            else:
                results = pd.merge(forecast_clean, results, on='date', how='outer')
            
            print(f" Predicción para {metric} completada")
        
        self.df_postpredict = results.sort_values('date').round()
        print(f"\n✓ Predicciones listas para {self.df_postpredict.columns.to_list()}")
        return self.df_postpredict