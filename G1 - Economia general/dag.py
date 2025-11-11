# dag.py
from __future__ import annotations

import sys
import os
import pendulum

# 🔹 Agregar carpeta actual al path para importar módulos locales
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

# Importar las funciones de staging y core
from staging import stage_single_csv_file
from core import core_pibe_anual_calculations, core_itaee_trimestral_calculations, consolidate_and_export_to_excel

# --- CONFIGURACIÓN DE ARCHIVOS ---
PIBE_CSVS = ['PIBE_2.csv', 'PIBE_5.csv', 'PIBE_12.csv', 'PIBE_31.csv', 'PIBE_68.csv']
ITAEE_CSV = 'ITAEE_3.csv'

# ======================================================================
# DAGs Staging
# ======================================================================
def create_staging_dag(csv_name):
    dag_id = f"Staging_{csv_name.replace('.csv', '')}"
    with DAG(
        dag_id=dag_id,
        start_date=pendulum.datetime(2023, 1, 1),
        schedule=None,
        catchup=False,
        tags=["staging", "economia", "chunking"],
    ) as dag:
        PythonOperator(
            task_id=f"stage_{csv_name.replace('.csv', '')}_data",
            python_callable=stage_single_csv_file,
            op_kwargs={"csv_name": csv_name}
        )

for csv in PIBE_CSVS + [ITAEE_CSV]:
    create_staging_dag(csv)

# ======================================================================
# DAGs CORE
# ======================================================================
with DAG(
    dag_id="Core_PIBE_Anual_Calculos",
    start_date=pendulum.datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["core", "pibe", "calculos"]
) as core_pibe_dag:
    PythonOperator(
        task_id="calculate_pibe_annual_indicators",
        python_callable=core_pibe_anual_calculations,
    )

with DAG(
    dag_id="Core_ITAEE_Trimestral_Calculos",
    start_date=pendulum.datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["core", "itaee", "calculos"]
) as core_itaee_dag:
    PythonOperator(
        task_id="calculate_itaee_quarterly_indicators",
        python_callable=core_itaee_trimestral_calculations,
    )
