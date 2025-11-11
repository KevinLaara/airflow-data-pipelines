import pendulum
import sys
import os # <-- ¡Nuevo! Necesario para manejar rutas

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

# ************************************************************
# *** SOLUCIÓN AL PROBLEMA DE IMPORTACIÓN EN AIRFLOW ***

# 1. Agrega el directorio actual (donde reside core_agropecuario.py) a la ruta de Python
# Esto permite que la importación directa funcione.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 2. Revertimos a la importación directa (SIN el punto)
from core_agropecuario import staging_agropecuario, core_agropecuario, master_agropecuario

# ************************************************************



# Configuración del DAG
with DAG(
    dag_id="dag_agropecuario", # Nombre del DAG
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=["g2", "agropecuaria", "core"],
    dagrun_timeout=timedelta(hours=1),
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    
    # ----------------------------------------------------
    # 🟢 STAGING: Tarea de Verificación
    # ----------------------------------------------------
    t_staging_agropecuario = PythonOperator(
        task_id="t_staging_agropecuario", # ID de tarea corto
        python_callable=staging_agropecuario,
        doc_md="Verifica la existencia del CSV de entrada (Cierre_agricola_mun_2024.csv).",
    )

    # ----------------------------------------------------
    # 🟡 CORE: Tarea de Transformación
    # ----------------------------------------------------
    t_core_agropecuario = PythonOperator(
        task_id="t_core_agropecuario", # ID de tarea corto
        python_callable=core_agropecuario,
        doc_md="Aplica las transformaciones y genera el DataFrame listo para el Master.",
    )

    # ----------------------------------------------------
    # 🔴 MASTER: Tarea de Carga Final
    # ----------------------------------------------------
    t_master_agropecuario = PythonOperator(
        task_id="t_master_agropecuario", # ID de tarea corto
        python_callable=master_agropecuario,
        doc_md="Guarda el resultado final del CORE en el archivo Excel MAESTRO.",
    )

    # ----------------------------------------------------
    # Definición del Flujo: Staging → Core → Master
    # ----------------------------------------------------
    t_staging_agropecuario >> t_core_agropecuario >> t_master_agropecuario 