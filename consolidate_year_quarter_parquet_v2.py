#!/usr/bin/env python3
"""
Script para consolidar archivos CSV mensuales de XAUUSD por trimestre o año y convertirlos a Parquet.

Uso:
    # Por trimestre
    python3 consolidate_year_quarter_parquet_v2.py --year 2019 --quarter 1 --input_dir ../DATOS_ORIGEN/data_2019 --output_dir ../DATOS_ORIGEN/consolidated_quarter_csv

    # Por año
    python3 consolidate_year_quarter_parquet_v2.py --year 2019 --period year --input_dir ../DATOS_ORIGEN/data_2019 --output_dir ../DATOS_ORIGEN/consolidated_quarter_csv

    # Trimestre con logging detallado
    python3 consolidate_year_quarter_parquet_v2.py --year 2020 --quarter 2 --input_dir ../DATOS_ORIGEN/data_2020 --output_dir ../DATOS_ORIGEN/consolidated_quarter_csv --log_level DEBUG
"""

import pandas as pd
import numpy as np
import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path
import glob

def setup_logging(log_dir, log_level=logging.INFO):
    """
    Configura el sistema de logging
    """
    # Crear directorio de logs si no existe
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Nombre del archivo de log con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"consolidar_{timestamp}.log"

    # Configurar logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def get_months_for_quarter(year, quarter):
    """
    Devuelve los meses correspondientes a un trimestre dado
    """
    quarter_months = {
        1: [1, 2, 3],   # Q1: Enero, Febrero, Marzo
        2: [4, 5, 6],   # Q2: Abril, Mayo, Junio
        3: [7, 8, 9],   # Q3: Julio, Agosto, Septiembre
        4: [10, 11, 12] # Q4: Octubre, Noviembre, Diciembre
    }

    if quarter not in quarter_months:
        raise ValueError(f"Trimestre inválido: {quarter}. Debe ser 1, 2, 3 o 4")

    return quarter_months[quarter]

def get_months_for_year():
    """
    Devuelve todos los meses del año
    """
    return list(range(1, 13))

def validate_csv_files(input_dir, year, months):
    """
    Valida que existan todos los archivos CSV para los meses especificados
    """
    input_path = Path(input_dir)
    missing_files = []
    existing_files = []

    for month in months:
        # Formato esperado: XAUUSD_YYYY_MM.csv (ej: XAUUSD_2019_01.csv)
        filename = f"XAUUSD_{year}_{month:02d}.csv"
        filepath = input_path / filename

        if not filepath.exists():
            missing_files.append(filename)
        else:
            existing_files.append(str(filepath))

    return existing_files, missing_files

def consolidate_csv_files(file_list, logger):
    """
    Consolida múltiples archivos CSV en un solo DataFrame
    """
    logger.info(f"Consolidando {len(file_list)} archivos CSV...")

    dataframes = []
    total_rows = 0

    for i, filepath in enumerate(file_list, 1):
        logger.info(f"  Procesando archivo {i}/{len(file_list)}: {Path(filepath).name}")

        try:
            # Leer CSV con optimizaciones
            df = pd.read_csv(
                filepath,
                parse_dates=['timestamp'],
                dtype={
                    'open': 'float32',
                    'high': 'float32',
                    'low': 'float32',
                    'close': 'float32',
                    'bid': 'float32',
                    'ask': 'float32',
                    'spread': 'float32',
                    'volume': 'float32'
                }
            )

            rows = len(df)
            total_rows += rows
            logger.info(f"    ✓ {rows:,} filas cargadas")

            # Verificar que no hay filas vacías
            if df.isnull().any().any():
                nulls = df.isnull().sum().sum()
                logger.warning(f"    ⚠️  Archivo tiene {nulls} valores nulos")

            dataframes.append(df)

        except Exception as e:
            logger.error(f"    ✗ Error procesando {filepath}: {str(e)}")
            raise

    # Concatenar todos los DataFrames
    logger.info(f"Concatenando {len(dataframes)} DataFrames...")
    df_consolidated = pd.concat(dataframes, ignore_index=True)

    # Ordenar por timestamp
    logger.info("Ordenando por timestamp...")
    df_consolidated = df_consolidated.sort_values('timestamp').reset_index(drop=True)

    logger.info(f"Total filas consolidadas: {len(df_consolidated):,}")
    logger.info(f"Rango de fechas: {df_consolidated['timestamp'].min()} -> {df_consolidated['timestamp'].max()}")

    return df_consolidated

def save_to_parquet(df, output_dir, year, period, logger, compression='snappy'):
    """
    Guarda el DataFrame consolidado en formato Parquet
    """
    # Crear directorio de salida si no existe
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Determinar nombre del archivo según el período
    if period == 'year':
        filename = f"XAUUSD_{year}_FULL.parquet"
    else:  # quarter
        filename = f"XAUUSD_{year}-Q{period}.parquet"

    filepath = output_path / filename

    logger.info(f"Guardando archivo: {filepath}")
    logger.info(f"  Compresión: {compression}")
    logger.info(f"  Filas: {len(df):,}")
    logger.info(f"  Columnas: {len(df.columns)}")

    # Guardar a Parquet
    try:
        df.to_parquet(
            filepath,
            compression=compression,
            index=False,
            engine='pyarrow'
        )

        # Verificar el archivo guardado
        file_size = filepath.stat().st_size / (1024 * 1024)  # Tamaño en MB
        logger.info(f"  ✓ Archivo guardado correctamente")
        logger.info(f"  📦 Tamaño: {file_size:.2f} MB")

        return filepath

    except Exception as e:
        logger.error(f"  ✗ Error guardando archivo: {str(e)}")
        raise

def generate_summary(df, filepath, logger):
    """
    Genera un resumen estadístico del archivo consolidado
    """
    logger.info("="*60)
    logger.info("RESUMEN DEL ARCHIVO CONSOLIDADO")
    logger.info("="*60)

    # Información básica
    logger.info(f"Archivo: {filepath}")
    logger.info(f"Total filas: {len(df):,}")
    logger.info(f"Período: {df['timestamp'].min()} -> {df['timestamp'].max()}")
    logger.info(f"Días en el período: {(df['timestamp'].max() - df['timestamp'].min()).days + 1}")

    # Verificar continuidad
    time_diffs = df['timestamp'].diff().dt.total_seconds()
    expected_diff = 60  # 60 segundos = 1 minuto
    gaps = time_diffs[time_diffs > expected_diff * 1.5]

    if len(gaps) > 0:
        logger.warning(f"⚠️  Gaps encontrados: {len(gaps)}")
        logger.warning(f"   Gap más grande: {time_diffs.max()/60:.1f} minutos")
        logger.warning(f"   Gaps > 5 min: {(time_diffs > 300).sum()}")
        logger.warning(f"   Gaps > 1 hora: {(time_diffs > 3600).sum()}")
    else:
        logger.info("✅ Secuencia temporal perfecta (sin gaps)")

    # Estadísticas de columnas clave
    logger.info("\n📊 Estadísticas:")
    logger.info(f"   Precio close - media: {df['close'].mean():.4f}")
    logger.info(f"                - std: {df['close'].std():.4f}")
    logger.info(f"                - min: {df['close'].min():.4f}")
    logger.info(f"                - max: {df['close'].max():.4f}")
    logger.info(f"   Spread - media: {df['spread'].mean():.6f}")
    logger.info(f"           - std: {df['spread'].std():.6f}")
    logger.info(f"   Volume - media: {df['volume'].mean():.4f}")
    logger.info(f"           - % cero: {(df['volume'] == 0).mean()*100:.2f}%")

    # Estadísticas por mes (si es consolidación anual)
    if len(df) > 100000:  # Solo para consolidaciones grandes
        logger.info("\n📅 Distribución por mes:")
        df['month'] = df['timestamp'].dt.month
        monthly_counts = df.groupby('month').size()
        for month, count in monthly_counts.items():
            logger.info(f"   Mes {month:02d}: {count:,.0f} filas")

def main():
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description='Consolida archivos CSV mensuales de XAUUSD por trimestre o año y los convierte a Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Consolidación por trimestre
  %(prog)s --year 2019 --quarter 1 --input_dir ../DATOS_ORIGEN/data_2019 --output_dir ../DATOS_ORIGEN/consolidated_quarter

  # Consolidación por año
  %(prog)s --year 2019 --period year --input_dir ../DATOS_ORIGEN/data_2019 --output_dir ../DATOS_ORIGEN/consolidated

  # Trimestre con compresión personalizada
  %(prog)s --year 2020 --quarter 2 --input_dir ../DATOS_ORIGEN/data_2020 --output_dir ../DATOS_ORIGEN/consolidated --compression zstd
        """
    )

    parser.add_argument(
        '--year',
        type=int,
        required=True,
        help='Año a procesar (ej: 2019)'
    )

    # Grupo exclusivo para período (trimestre o año)
    period_group = parser.add_mutually_exclusive_group(required=True)
    period_group.add_argument(
        '--quarter',
        type=int,
        choices=[1, 2, 3, 4],
        help='Trimestre a procesar (1, 2, 3 o 4)'
    )
    period_group.add_argument(
        '--period',
        type=str,
        choices=['year'],
        help='Período a procesar: "year" para año completo'
    )

    parser.add_argument(
        '--input_dir',
        type=str,
        required=True,
        help='Directorio de entrada con los archivos CSV mensuales'
    )

    parser.add_argument(
        '--output_dir',
        type=str,
        required=True,
        help='Directorio de salida para el archivo Parquet consolidado'
    )

    parser.add_argument(
        '--log_level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Nivel de logging (default: INFO)'
    )

    parser.add_argument(
        '--compression',
        type=str,
        default='snappy',
        choices=['snappy', 'gzip', 'brotli', 'lz4', 'zstd'],
        help='Compresión para archivo Parquet (default: snappy)'
    )

    # Parsear argumentos
    args = parser.parse_args()

    # Convertir nivel de logging
    log_level = getattr(logging, args.log_level.upper())

    # Configurar logging (usando output_dir para los logs)
    logger = setup_logging(args.output_dir, log_level)

    # Determinar período de consolidación
    if args.quarter:
        period_type = 'quarter'
        period_value = args.quarter
        period_desc = f"Q{args.quarter}"
    else:  # args.period == 'year'
        period_type = 'year'
        period_value = 'year'
        period_desc = f"Año completo"

    # Log de inicio
    logger.info("="*60)
    logger.info("INICIANDO CONSOLIDACIÓN")
    logger.info("="*60)
    logger.info(f"Año: {args.year}")
    logger.info(f"Período: {period_desc}")
    logger.info(f"Directorio entrada: {args.input_dir}")
    logger.info(f"Directorio salida: {args.output_dir}")
    logger.info(f"Compresión: {args.compression}")

    try:
        # Obtener meses según el período
        if args.quarter:
            months = get_months_for_quarter(args.year, args.quarter)
            logger.info(f"Meses del trimestre: {months}")
        else:
            months = get_months_for_year()
            logger.info(f"Meses del año: {months}")

        # Validar archivos CSV
        logger.info("Validando archivos CSV...")
        existing_files, missing_files = validate_csv_files(args.input_dir, args.year, months)

        if missing_files:
            logger.warning(f"Archivos faltantes: {missing_files}")
            if len(missing_files) == len(months):
                logger.error(f"No se encontró ningún archivo CSV para {period_desc}")
                sys.exit(1)
            logger.warning("Continuando solo con los archivos existentes...")

        if not existing_files:
            logger.error("No hay archivos CSV para procesar")
            sys.exit(1)

        logger.info(f"Archivos encontrados: {len(existing_files)} de {len(months)}")
        for f in existing_files:
            logger.info(f"  ✓ {Path(f).name}")

        # Consolidar archivos CSV
        df = consolidate_csv_files(existing_files, logger)

        # Guardar a Parquet
        output_file = save_to_parquet(
            df,
            args.output_dir,
            args.year,
            period_value,
            logger,
            args.compression
        )

        # Generar resumen
        generate_summary(df, output_file, logger)

        # Log de éxito
        logger.info("="*60)
        logger.info("✅ PROCESO COMPLETADO EXITOSAMENTE")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"❌ ERROR FATAL: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()