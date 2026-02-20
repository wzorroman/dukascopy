import pandas as pd
import os

# Definir la ruta de tu archivo en la carpeta DATA_RAW de Colab
# Se asume que el archivo está en /content/DATA_RAW/
# archivo_path = 'data_2010/XAUUSD_2010_01.ticks.parquet'
archivo_path = 'consolidated/XAUUSD_2010_FULL.parquet'

def verificar_campos_parquet(ruta):
    # 1. Verificar si el archivo existe en la ruta especificada [2]
    if not os.path.exists(ruta):
        return f"Error: El archivo no se encontró en {ruta}"

    try:
        # 2. Cargar el archivo Parquet usando Pandas para una manipulación eficiente [1, 6]
        df = pd.read_parquet(ruta)
        
        # 3. Lista de campos requeridos para el entrenamiento [3, 7]
        campos_requeridos = ["timestamp", "open", "high", "low", "close", "bid", "ask", "spread", "volume"]
        campos_actuales = df.columns.tolist()
        
        print(f"--- Análisis de campos en: {os.path.basename(ruta)} ---")
        print(f"Campos detectados: {campos_actuales}\n")
        
        # 4. Comprobación lógica de integridad [2]
        faltantes = [campo for campo in campos_requeridos if campo not in campos_actuales]
        
        if not faltantes:
            print("✅ Validación exitosa: Todos los campos requeridos están presentes.")
            # Mostrar información estadística básica para higiene de datos [2, 8]
            print("\nResumen de integridad de datos:")
            print(df[campos_requeridos].info())
        else:
            print(f"❌ Advertencia: Faltan los siguientes campos críticos: {faltantes}")
            
        return df.head() # Retorna vista previa para inspección visual [6]

    except Exception as e:
        return f"Error al leer el archivo Parquet: {e}"

# Ejecutar la validación
vista_previa = verificar_campos_parquet(archivo_path)
print(vista_previa)
