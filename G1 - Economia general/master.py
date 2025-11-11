# master.py
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

# Importar funciones desde core.py
from core import consolidate_and_export_to_excel

# Las listas de CSVs necesarias para construir los IDs de los DAGs a disparar
PIBE_CSVS = ['PIBE_2.csv', 'PIBE_5.csv', 'PIBE_12.csv', 'PIBE_31.csv', 'PIBE_68.csv']
ITAEE_CSV = 'ITAEE_3.csv'

# ======================================================================
# DEFINICIÓN DEL DAG ORQUESTADOR
# ======================================================================
with DAG(
    dag_id="dag_economia_general",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Mexico_City"),
    schedule=None,
    catchup=False,
    tags=["master", "economia", "general", "orquestador"],
) as master_dag:

    # 1. GRUPO STAGING PIBE
    with TaskGroup("Trigger_Staging_PIBE") as staging_pibe_group:
        trigger_pibe_staging = [
            TriggerDagRunOperator(
                task_id=f"trigger_stg_{csv.replace('.csv', '')}",
                trigger_dag_id=f"Staging_{csv.replace('.csv', '')}",
                poke_interval=10,
                execution_date="{{ ds }}"
            )
            for csv in PIBE_CSVS
        ]

    # 2. STAGING ITAEE
    trigger_itaee_staging = TriggerDagRunOperator(
        task_id="trigger_stg_itaee_3",
        trigger_dag_id="Staging_ITAEE_3",
        poke_interval=10,
        execution_date="{{ ds }}"
    )

    # 3. CORE PIBE
    run_core_pibe = TriggerDagRunOperator(
        task_id="run_core_pibe_annual_calculations",
        trigger_dag_id="Core_PIBE_Anual_Calculos",
        poke_interval=10,
        execution_date="{{ ds }}"
    )

    # 4. CORE ITAEE
    run_core_itaee = TriggerDagRunOperator(
        task_id="run_core_itaee_quarterly_calculations",
        trigger_dag_id="Core_ITAEE_Trimestral_Calculos",
        poke_interval=10,
        execution_date="{{ ds }}"
    )

    # 5. EXPORTACIÓN FINAL A EXCEL
    export_to_excel = PythonOperator(
        task_id="final_export_to_excel",
        python_callable=consolidate_and_export_to_excel
    )

    # FLUJO DE DEPENDENCIAS
    staging_pibe_group >> run_core_pibe
    trigger_itaee_staging >> run_core_itaee
    [run_core_pibe, run_core_itaee] >> export_to_excel
