import pandas as pd
import os
import logging
import json
import chardet  # Para detectar codificación automáticamente

# Configuración de logging para ver mensajes en Airflow
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuración de Rutas y Archivos ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_PATH, "raw")
TRANSFORMATION_DIR = os.path.join(BASE_PATH, "transformacion")
INPUT_FILE_NAME = "Cierre_agricola_mun_2024.csv"
OUTPUT_FILE_NAME = "Produccion_Agropecuaria_MAESTRO.xlsx"
INPUT_FILE_PATH = os.path.join(RAW_DIR, INPUT_FILE_NAME)
OUTPUT_FILE_PATH = os.path.join(TRANSFORMATION_DIR, OUTPUT_FILE_NAME)

# --- Configuración de Tipos de Datos ---
DTYPE_CONFIG = {
    'Sembrada': 'float64',
    'Cosechada': 'float64',
    'Volumenproduccion': 'float64',
    'Preciomediorural': 'float64',
    'Valorproduccion': 'float64',
    'Anio': 'int64',
    'Idestado': 'int64',
    'Idmunicipio': 'int64',
}

# ----------------------------------------------------------------------
# 🟢 FUNCIÓN STAGING: Verificación y Preparación
# ----------------------------------------------------------------------
def staging_agropecuario():
    """
    [STAGING] - Verifica la existencia del archivo fuente y asegura la existencia de directorios.
    """
    logger.info(f"Iniciando Staging (Verificación): {INPUT_FILE_PATH}")
    
    os.makedirs(TRANSFORMATION_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

    if not os.path.exists(INPUT_FILE_PATH):
        raise FileNotFoundError(f"❌ ERROR: Archivo fuente no encontrado: {INPUT_FILE_PATH}")
    
    logger.info("✅ Archivo fuente encontrado. Directorios listos.")

# ----------------------------------------------------------------------
# 🟡 FUNCIÓN CORE: Transformación y FILTRO DE OAXACA
# ----------------------------------------------------------------------
def core_agropecuario():
    """
    [CORE] - Aplica las transformaciones, detecta la codificación del CSV, 
    filtra solo Oaxaca y genera el DataFrame listo.
    """
    logger.info("Iniciando Core (Transformación y Cálculos).")

    # Detectar codificación del CSV
    try:
        with open(INPUT_FILE_PATH, 'rb') as f:
            raw_data = f.read(10000)  # Leer los primeros 10 KB para detectar
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            logger.info(f"Codificación detectada: {encoding} (confianza: {confidence})")
            if not encoding:
                encoding = 'utf-8'  # fallback si no se detecta
    except Exception as e:
        logger.warning(f"No se pudo detectar codificación, se usará UTF-8 por defecto: {e}")
        encoding = 'utf-8'

    # Leer CSV con la codificación detectada
    try:
        df = pd.read_csv(
            INPUT_FILE_PATH,
            dtype=DTYPE_CONFIG,
            encoding=encoding,
        )
        logger.info(f"CSV cargado correctamente. Filas totales: {len(df)}")
    except UnicodeDecodeError as e:
        logger.error(f"Error de decodificación con {encoding}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error leyendo CSV: {e}")
        raise

    # Filtrar solo registros de Oaxaca
    df_oaxaca = df[df['Nomestado'] == 'Oaxaca']
    logger.info(f"✅ Filtro aplicado. Filas correspondientes a Oaxaca: {len(df_oaxaca)}")

    # Transformación final
    df_transformed = pd.DataFrame()
    df_transformed['Entidad'] = df_oaxaca['Nomestado']
    df_transformed['Municipio'] = df_oaxaca['Nommunicipio']
    df_transformed['Clave Entidad'] = df_oaxaca['Idestado']
    df_transformed['Clave Municipio'] = df_oaxaca['Idmunicipio']
    df_transformed['Concepto'] = 'Produccion agropecuaria'
    df_transformed['Unidad Medida'] = df_oaxaca['Nomunidad']
    df_transformed['Valor'] = df_oaxaca['Valorproduccion'].apply(lambda x: round(x, 2) if pd.notnull(x) else 0.0)
    df_transformed['Año'] = df_oaxaca['Anio']
    df_transformed['Id_Cultivo'] = df_oaxaca['Idcultivo']
    df_transformed['Cultivo'] = df_oaxaca['Nomcultivo']

    logger.info("Transformación a formato final completada.")

    return df_transformed.to_json(orient='split')

# ----------------------------------------------------------------------
# 🔴 FUNCIÓN MASTER: Carga Final
# ----------------------------------------------------------------------
def master_agropecuario(ti):
    """
    [MASTER] - Recupera el DataFrame transformado y lo guarda en el Excel de destino.
    """
    logger.info("Iniciando Master (Carga a Excel).")
    
    json_data = ti.xcom_pull(task_ids='t_core_agropecuario', key='return_value')
    if not json_data:
        raise ValueError("❌ ERROR: No se pudo recuperar el DataFrame de la etapa Core.")
    
    df_master = pd.read_json(json_data, orient='split')
    logger.info(f"DataFrame recuperado. Filas a escribir: {len(df_master)}")

    df_master.to_excel(
        OUTPUT_FILE_PATH,
        index=False,
        engine='xlsxwriter' 
    )
    logger.info(f"✅ Master completado. Archivo generado en:\n{OUTPUT_FILE_PATH}")
