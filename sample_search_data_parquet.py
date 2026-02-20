import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from pathlib import Path

def read_parquet_head_tail_with_sampling(parquet_path, head_count=1000, tail_count=1000, random_sample=500, seed=42):
    """
    Lee datos de la cabecera, cola y muestra aleatoria de un archivo Parquet.
    
    Args:
        parquet_path (str): Ruta del archivo Parquet
        head_count (int): Número de filas a leer desde el inicio
        tail_count (int): Número de filas a leer desde el final
        random_sample (int): Tamaño de la muestra aleatoria
        seed (int): Semilla para reproducibilidad del muestreo aleatorio
    
    Returns:
        dict: Diccionario con head, tail y random_sample como DataFrames separados
    """
    
    try:
        # Verificar si el archivo existe
        if not Path(parquet_path).exists():
            raise FileNotFoundError(f"El archivo {parquet_path} no existe")
        
        # Leer el archivo Parquet usando pyarrow para eficiencia
        parquet_file = pq.ParquetFile(parquet_path)
        
        # Obtener el número total de filas
        num_rows = parquet_file.metadata.num_rows
        
        print(f"Archivo: {parquet_path}")
        print(f"Total de filas: {num_rows:,}")
        print(f"Esquema: {parquet_file.schema}")
        
        # 1. Leer las primeras filas (head)
        print(f"\nLeyendo primeras {head_count} filas...")
        head_df = parquet_file.read_row_groups(row_groups=[0]).head(head_count).to_pandas()
        
        # 2. Leer las últimas filas (tail)
        print(f"Leyendo últimas {tail_count} filas...")
        
        # Determinar qué grupos de filas contienen la cola
        row_groups_info = []
        accumulated_rows = 0
        
        for i in range(parquet_file.num_row_groups):
            row_group_metadata = parquet_file.metadata.row_group(i)
            row_group_rows = row_group_metadata.num_rows
            row_groups_info.append({
                'index': i,
                'start': accumulated_rows,
                'end': accumulated_rows + row_group_rows,
                'row_count': row_group_rows
            })
            accumulated_rows += row_group_rows
        
        # Encontrar los grupos de filas que contienen la cola
        tail_start_row = max(0, num_rows - tail_count)
        tail_row_groups = []
        
        for info in row_groups_info:
            if info['end'] > tail_start_row:
                tail_row_groups.append(info['index'])
        
        # Leer los grupos de filas de la cola
        tail_data = parquet_file.read_row_groups(tail_row_groups).to_pandas()
        tail_df = tail_data.tail(tail_count)
        
        # 3. Muestra aleatoria de todo el dataset
        print(f"Generando muestra aleatoria de {random_sample} filas...")
        
        # Si el archivo no es muy grande, cargarlo completamente para el muestreo
        if num_rows <= 100000:  # Ajusta este límite según tus necesidades
            full_df = parquet_file.read().to_pandas()
        else:
            # Para archivos grandes, usar muestreo estratificado por grupos de filas
            # Calcular cuántas muestras tomar de cada grupo de filas
            samples_per_group = max(1, random_sample // parquet_file.num_row_groups)
            random_samples = []
            
            np.random.seed(seed)
            
            for i in range(parquet_file.num_row_groups):
                row_group_df = parquet_file.read_row_groups([i]).to_pandas()
                if len(row_group_df) > samples_per_group:
                    sampled = row_group_df.sample(n=min(samples_per_group, len(row_group_df)), 
                                                random_state=seed + i)
                else:
                    sampled = row_group_df
                random_samples.append(sampled)
            
            full_df = pd.concat(random_samples, ignore_index=True)
        
        # Si aún necesitamos más muestras o el dataset era pequeño
        if len(full_df) > random_sample:
            random_df = full_df.sample(n=random_sample, random_state=seed)
        else:
            random_df = full_df
        
        # Información del muestreo
        print(f"\nResumen del muestreo:")
        print(f"- Head: {len(head_df)} filas (primera fila: {head_df.index[0] if len(head_df) > 0 else 'N/A'})")
        print(f"- Tail: {len(tail_df)} filas (última fila: {tail_df.index[-1] if len(tail_df) > 0 else 'N/A'})")
        print(f"- Random sample: {len(random_df)} filas")
        
        return {
            'head': head_df,
            'tail': tail_df,
            'random_sample': random_df,
            'metadata': {
                'total_rows': num_rows,
                'file_path': parquet_path,
                'head_count': len(head_df),
                'tail_count': len(tail_df),
                'sample_size': len(random_df)
            }
        }
        
    except Exception as e:
        print(f"Error al leer el archivo Parquet: {e}")
        return None

def read_parquet_simple_sampling(parquet_path, head_count=1000, tail_count=1000, random_sample=500, seed=42):
    """
    Versión simplificada para archivos que caben en memoria.
    
    Args:
        parquet_path (str): Ruta del archivo Parquet
        head_count (int): Número de filas a leer desde el inicio
        tail_count (int): Número de filas a leer desde el final
        random_sample (int): Tamaño de la muestra aleatoria
        seed (int): Semilla para reproducibilidad
    
    Returns:
        dict: Diccionario con head, tail y random_sample
    """
    
    try:
        # Leer todo el archivo (solo para archivos razonablemente pequeños)
        print(f"Cargando archivo completo: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        
        print(f"Total de filas cargadas: {len(df):,}")
        print(f"Columnas: {list(df.columns)}")
        
        # Obtener head
        head_df = df.head(head_count).copy()
        
        # Obtener tail
        tail_df = df.tail(tail_count).copy()
        
        # Muestra aleatoria
        np.random.seed(seed)
        if len(df) > random_sample:
            random_df = df.sample(n=random_sample, random_state=seed).copy()
        else:
            random_df = df.copy()
        
        # Agregar información de posición original
        head_df['_sample_type'] = 'head'
        tail_df['_sample_type'] = 'tail'
        random_df['_sample_type'] = 'random'
        
        print(f"\nMuestras obtenidas:")
        print(f"- Head: {len(head_df)} filas")
        print(f"- Tail: {len(tail_df)} filas")
        print(f"- Random: {len(random_df)} filas")
        
        # Crear un dataframe combinado para análisis
        combined_df = pd.concat([head_df, tail_df, random_df], ignore_index=True)
        
        return {
            'head': head_df,
            'tail': tail_df,
            'random_sample': random_df,
            'combined': combined_df,
            'full_dataframe': df,
            'metadata': {
                'total_rows': len(df),
                'file_path': parquet_path,
                'head_count': len(head_df),
                'tail_count': len(tail_df),
                'sample_size': len(random_df)
            }
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# Ejemplo de uso
if __name__ == "__main__":
    # Especificar la ruta del archivo
    parquet_path = "consolidated/XAUUSD_2013_FULL.parquet"
    
    print("=" * 60)
    print("MÉTODO 1: Lectura eficiente para archivos grandes")
    print("=" * 60)
    
    # Usar la función principal
    result = read_parquet_head_tail_with_sampling(
        parquet_path=parquet_path,
        head_count=1000,
        tail_count=1000,
        random_sample=500,
        seed=42
    )
    
    if result:
        print("\n" + "=" * 60)
        print("INFORMACIÓN DE HEAD (primeras filas):")
        print("=" * 60)
        print(result['head'].info())
        print("\nPrimeras 5 filas:")
        print(result['head'].head())
        
        print("\n" + "=" * 60)
        print("INFORMACIÓN DE TAIL (últimas filas):")
        print("=" * 60)
        print(result['tail'].info())
        print("\nÚltimas 5 filas:")
        print(result['tail'].tail())
        
        print("\n" + "=" * 60)
        print("INFORMACIÓN DE MUESTRA ALEATORIA:")
        print("=" * 60)
        print(result['random_sample'].info())
        print("\n5 filas aleatorias:")
        print(result['random_sample'].head())
    
    print("\n" + "=" * 60)
    print("MÉTODO 2: Versión simplificada")
    print("=" * 60)
    
    # Usar la versión simplificada
    result_simple = read_parquet_simple_sampling(
        parquet_path=parquet_path,
        head_count=1000,
        tail_count=1000,
        random_sample=500,
        seed=42
    )
    
    if result_simple:
        print("\nMuestra combinada con tipos:")
        print(result_simple['combined']['_sample_type'].value_counts())
        