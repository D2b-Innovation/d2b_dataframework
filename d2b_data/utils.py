import os
import json
import pandas as pd
from unidecode import unidecode

def load_schema_from_csv(verbose_logger_func, wf_name_func):
    """
    Carga un schema desde un archivo CSV local llamado `schema.csv`.
    El archivo debe tener al menos las columnas: 'name' (o 'nombre') y 'type' (o 'tipo').
    Opcionalmente puede tener una columna 'description'.

    Args:
        verbose_logger_func: Instancia de logger personalizado con métodos `.log()` y `.critical()`.
        wf_name_func: Nombre del workflow actual (para trazabilidad en logs críticos).

    Returns:
        schema_table_local: lista de dicts con keys `name`, `type`, `description`, o None si no hay archivo o hay error.
    """
    schema_table_local = None
    schema_csv_path = "schema.csv"

    if os.path.isfile(schema_csv_path):
        verbose_logger_func.log(f"load_schema_from_csv | Cargando schema desde {schema_csv_path}")
        try:
            schema_df = pd.read_csv(schema_csv_path)
            rename_map = {
                c: "name" if "nombre" in c.lower()
                else "type" if "tipo" in c.lower()
                else "description" if "descrip" in c.lower()
                else c
                for c in schema_df.columns
            }
            schema_df.rename(columns=rename_map, inplace=True)

            if "name" not in schema_df.columns or "type" not in schema_df.columns:
                raise ValueError("schema.csv debe tener columnas 'name' (o 'nombre') y 'type' (o 'tipo').")

            if "description" not in schema_df.columns:
                verbose_logger_func.log("load_schema_from_csv | Columna 'description' no encontrada. Se usará vacía.")
                schema_df["description"] = ""

            schema_df["type"] = schema_df["type"].str.upper()
            schema_df["description"] = schema_df["description"].astype(str).map(unidecode)

            schema_table_local = json.loads(
                schema_df[["name", "type", "description"]].to_json(orient="records", force_ascii=False)
            )

            verbose_logger_func.log("load_schema_from_csv | Schema cargado y procesado exitosamente.")

        except Exception as e_schema:
            msg_schema_err = f"Error procesando {schema_csv_path}: {str(e_schema)}"
            verbose_logger_func.critical(msg_schema_err, current_workflow_name=wf_name_func)
            return None
    else:
        verbose_logger_func.log(f"load_schema_from_csv | {schema_csv_path} no encontrado, se usará autodetección de schema en BQ.")

    return schema_table_local