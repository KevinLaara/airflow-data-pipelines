# staging.py
import pandas as pd
import dask.dataframe as dd
import chardet
import os

# --- DEFINICIÓN DE RUTAS DENTRO DEL CONTENEDOR DE AIRFLOW ---
# Las rutas son relativas al path base donde se encuentra este script: /opt/airflow/dags/G1 - Economia general
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(BASE_DIR, 'raw')
STAGED_DATA_PATH = os.path.join(BASE_DIR, 'staged')
# -----------------------------------------------------------

CHUNK_SIZE_MB = 100 # Bloque de Dask de 100MB

def ensure_directory_exists(path):
    """Crea un directorio si no existe."""
    os.makedirs(path, exist_ok=True)

def detect_file_encoding(file_path):
    """Detecta el encoding del archivo usando chardet."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Archivo no encontrado en la ruta: {file_path}")
        
    print(f"Detectando encoding para {file_path}...")
    with open(file_path, 'rb') as rawdata:
        # Lee 200KB para detección
        result = chardet.detect(rawdata.read(204800))
    return result['encoding']

def stage_single_csv_file(csv_name):
    """Tarea de Staging: Lectura masiva con Dask y guardado en Parquet."""
    ensure_directory_exists(STAGED_DATA_PATH)
    
    file_path = os.path.join(RAW_DATA_PATH, csv_name)
    output_path = os.path.join(STAGED_DATA_PATH, csv_name.replace(".csv", "_staged.parquet"))
    
    # 1. Detección de Encoding
    try:
        encoding = detect_file_encoding(file_path)
    except Exception as e:
        print(f"Error en chardet: {e}. Usando 'latin-1' como fallback.")
        encoding = 'latin-1'
    
    # 2. Lectura Masiva con Dask
    print(f"Leyendo {csv_name} con Dask en bloques de {CHUNK_SIZE_MB}MB...")
    # Saltamos las primeras 2 filas de metadata no relevante
    ddf = dd.read_csv(
        file_path, 
        encoding=encoding, 
        dtype='object', 
        blocksize=f'{CHUNK_SIZE_MB}MB', 
        skiprows=range(1, 3) 
    )
    
    # 3. Limpieza de columnas
    ddf.columns = ddf.columns.str.replace('"', '').str.strip()

    # 4. Guardar en Parquet
    ddf.to_parquet(output_path, engine='pyarrow', overwrite=True)
    
    print(f"Archivo staged guardado en: {output_path}")