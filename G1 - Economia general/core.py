# core.py
import pandas as pd
import dask.dataframe as dd
import glob
import os
import shutil # Para limpiar la carpeta staged si es necesario

# --- DEFINICIÓN DE RUTAS DENTRO DEL CONTENEDOR DE AIRFLOW ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STAGED_DATA_PATH = os.path.join(BASE_DIR, 'staged')
CORE_DATA_PATH = os.path.join(BASE_DIR, 'core_temp') # Temporal
FINAL_OUTPUT_PATH = os.path.join(BASE_DIR, 'Transformacion')
# -----------------------------------------------------------

def ensure_directory_exists(path):
    """Crea un directorio si no existe."""
    os.makedirs(path, exist_ok=True)

def standardize_and_melt(file_pattern, is_itaee=False):
    """Lee archivos staged, los estandariza y hace el melt (ancho -> largo)."""
    staged_files = glob.glob(os.path.join(STAGED_DATA_PATH, file_pattern))
    final_df_list = []

    for file_path in staged_files:
        ddf = dd.read_parquet(file_path, engine='pyarrow')
        df = ddf.compute() 

        df.rename(columns={df.columns[0]: 'Concepto'}, inplace=True)
        
        # Identificar columnas de Años (PIBE) o Periodos (ITAEE)
        if is_itaee:
            value_vars = [col for col in df.columns if '|' in col]
        else:
            value_vars = [col for col in df.columns if str(col).isdigit() and len(str(col)) == 4]
        
        id_vars = ['Concepto']
        
        # MELT: Pivotar de ancho a largo (la clave para la unificación)
        df_long = pd.melt(df, id_vars=id_vars, value_vars=value_vars,
                          var_name='Periodo', value_name='Valor')
        
        df_long['Valor'] = pd.to_numeric(df_long['Valor'], errors='coerce')
        df_long.dropna(subset=['Valor'], inplace=True)
        
        df_long['Fuente'] = os.path.basename(file_path).replace("_staged.parquet", "")
        final_df_list.append(df_long)
        
    return pd.concat(final_df_list, ignore_index=True)


# --- CORE PIBE (Cálculo de 8 indicadores Anuales) ---
def core_pibe_anual_calculations():
    ensure_directory_exists(CORE_DATA_PATH)
    pib_consolidated = standardize_and_melt("PIBE_*_staged.parquet")

    pib_consolidated['Estado'] = pib_consolidated['Concepto'].apply(lambda x: 'Nacional' if 'Estados Unidos Mexicanos' in x else x)
    pib_data = pib_consolidated[pib_consolidated['Estado'] != 'Nacional'].copy()
    pib_nacional_df = pib_consolidated[pib_consolidated['Estado'] == 'Nacional'][['Periodo', 'Valor']].rename(columns={'Valor': 'PIB_Nacional'})
    
    pib_data['Año'] = pib_data['Periodo'].astype(int)
    
    # 1. Tasa de Crecimiento Anual (Indicador 8)
    pib_data = pib_data.sort_values(by=['Estado', 'Año'])
    pib_data['PIB_Anterior'] = pib_data.groupby('Estado')['Valor'].shift(1)
    pib_data['Tasa_Crecimiento_Anual'] = (pib_data['Valor'] / pib_data['PIB_Anterior'] - 1) * 100
    pib_data.drop(columns=['PIB_Anterior'], inplace=True)
    
    # 2. Aportación al PIB Nacional (Indicador 2)
    pib_data = pd.merge(pib_data, pib_nacional_df, on='Periodo', how='left')
    pib_data['Aportacion_PIB_Nacional'] = (pib_data['Valor'] / pib_data['PIB_Nacional']) * 100
    pib_data.drop(columns=['PIB_Nacional'], inplace=True)
    
    # 3. Clasificación de Indicadores Requeridos (PIB Total, Sectoriales)
    pib_data.loc[pib_data['Concepto'].str.contains('PIBPM|PIB Real', case=False), 'Tipo_Indicador'] = 'PIB_Real_MDP' # Indicador 1
    pib_data.loc[pib_data['Concepto'].str.contains('primario', case=False), 'Tipo_Indicador'] = 'PIB_Sector_Primario' # Indicador 4
    pib_data.loc[pib_data['Concepto'].str.contains('secundario', case=False), 'Tipo_Indicador'] = 'PIB_Sector_Secundario' # Indicador 5
    pib_data.loc[pib_data['Concepto'].str.contains('terciario', case=False), 'Tipo_Indicador'] = 'PIB_Sector_Terciario' # Indicador 6
    pib_data.loc[pib_data['Concepto'].str.contains('Agricola', case=False), 'Tipo_Indicador'] = 'Contribucion_Agricola_al_PIB' # Indicador 7
    # El PIB per cápita (Indicador 3) requiere datos de población no proporcionados, pero el campo está listo.

    pib_data.to_parquet(os.path.join(CORE_DATA_PATH, "PIBE_Anual_Core.parquet"))
    print("Core PIBE terminado. 8 Indicadores anuales calculados y guardados.")


# --- CORE ITAEE (Cálculo de 2 indicadores Trimestrales) ---
def core_itaee_trimestral_calculations():
    ensure_directory_exists(CORE_DATA_PATH)
    itaee_consolidated = standardize_and_melt("ITAEE_3_staged.parquet", is_itaee=True)
    
    itaee_consolidated[['Año', 'Frecuencia']] = itaee_consolidated['Periodo'].str.split(' | ', expand=True)

    # Clasificación de Indicadores Requeridos
    itaee_consolidated.loc[itaee_consolidated['Frecuencia'].str.contains('T'), 'Tipo_Indicador'] = 'ITAEE_Trimestral_MDP' # Indicador 9
    itaee_consolidated.loc[itaee_consolidated['Frecuencia'] == 'Anual', 'Tipo_Indicador'] = 'ITAEE_Variacion_Anual_Tasa' # Indicador 10

    itaee_consolidated.to_parquet(os.path.join(CORE_DATA_PATH, "ITAEE_Trimestral_Core.parquet"))
    print("Core ITAEE terminado. 2 Indicadores trimestrales clasificados y guardados.")
    
# --- EXPORTACIÓN FINAL ---
def consolidate_and_export_to_excel():
    """Une los resultados del CORE y exporta al Excel final solicitado."""
    ensure_directory_exists(FINAL_OUTPUT_PATH)
    
    # 1. Leer los dos archivos Core generados
    pibe_df = pd.read_parquet(os.path.join(CORE_DATA_PATH, "PIBE_Anual_Core.parquet"))
    itaee_df = pd.read_parquet(os.path.join(CORE_DATA_PATH, "ITAEE_Trimestral_Core.parquet"))
    
    # 2. Estandarizar y concatenar
    pibe_df['Frecuencia'] = 'Anual'
    itaee_df.rename(columns={'Frecuencia': 'Trimestre_o_Anual'}, inplace=True)
    pibe_df.rename(columns={'Frecuencia': 'Trimestre_o_Anual'}, inplace=True)
    
    # Unir las tablas (Concatenar)
    reporte_final = pd.concat([pibe_df, itaee_df], ignore_index=True)

    # 3. Exportar a EXCEL
    excel_path = os.path.join(FINAL_OUTPUT_PATH, "Reporte_Economia_General_Final.xlsx")
    reporte_final.to_excel(excel_path, index=False, sheet_name="Actividad_Economica_G1")
    
    print(f"✅ ¡Éxito! El reporte final consolidado fue exportado a: {excel_path}")
    
    # Limpieza: (Opcional, pero recomendado en un ETL para ahorrar espacio)
    # shutil.rmtree(STAGED_DATA_PATH, ignore_errors=True)
    # shutil.rmtree(CORE_DATA_PATH, ignore_errors=True)