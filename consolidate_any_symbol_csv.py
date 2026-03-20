#!/usr/bin/env python3
"""
Script para consolidar archivos CSV mensuales de cualquier símbolo por trimestre o año completo y convertirlos a Parquet.

Uso:
    # Por trimestre
    python3 consolidate_symbol_csv.py --symbol AUDUSD --year 2019 --quarter 1 --input_dir ../DATOS_ORIGEN/AUDUSD --output_dir ../DATOS_ORIGEN/consolidated

    # Por año completo (por defecto XAUUSD si no se especifica símbolo)
    python3 consolidate_symbol_csv.py --year 2019 --full_year --input_dir ../DATOS_ORIGEN/XAUUSD --output_dir ../DATOS_ORIGEN/consolidated

    # Para EURUSD
    python3 consolidate_symbol_csv.py --symbol EURUSD --year 2020 --full_year --input_dir ../DATOS_ORIGEN/EURUSD --output_dir ../DATOS_ORIGEN/consolidated

    # Para USDJPY trimestre 1
    python3 consolidate_symbol_csv.py --symbol USDJPY --year 2021 --quarter 1 --input_dir ../DATOS_ORIGEN/USDJPY --output_dir ../DATOS_ORIGEN/consolidated
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
    """Configura el sistema de logging"""
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"consolidar_{timestamp}.log"

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def get_months_for_period(year, quarter=None, full_year=False):
    """Devuelve los meses correspondientes a un trimestre o año completo"""
    if full_year:
        return list(range(1, 13))

    if quarter is not None:
        quarter_months = {
            1: [1, 2, 3],
            2: [4, 5, 6],
            3: [7, 8, 9],
            4: [10, 11, 12]
        }

        if quarter not in quarter_months:
            raise ValueError(f"Trimestre inválido: {quarter}. Debe ser 1, 2, 3 o 4")

        return quarter_months[quarter]

    raise ValueError("Debe especificar quarter o full_year=True")

def validate_csv_files(input_dir, symbol, year, months):
    """Valida que existan los archivos CSV necesarios"""
    input_path = Path(input_dir)
    missing_files = []
    existing_files = []

    for month in months:
        # Formato: SYMBOL_YYYY_MM.csv (ej: AUDUSD_2019_01.csv)
        filename = f"{symbol}_{year}_{month:02d}.csv"
        filepath = input_path / filename

        if not filepath.exists():
            missing_files.append(filename)
        else:
            existing_files.append(str(filepath))

    return existing_files, missing_files

def consolidate_csv_files(file_list, logger):
    """Consolida múltiples archivos CSV en un DataFrame"""
    logger.info(f"Consolidando {len(file_list)} archivos CSV...")

    dataframes = []
    total_rows = 0

    for i, filepath in enumerate(file_list, 1):
        logger.info(f"  Procesando archivo {i}/{len(file_list)}: {Path(filepath).name}")

        try:
            # Detectar automáticamente las columnas del CSV
            df_sample = pd.read_csv(filepath, nrows=1)
            column_types = {}

            # Mapeo de tipos para columnas comunes
            for col in df_sample.columns:
                if col in ['timestamp']:
                    continue  # Se parsea después
                elif col in ['open', 'high', 'low', 'close', 'bid', 'ask', 'spread']:
                    column_types[col] = 'float32'
                elif col in ['volume', 'tick_volume']:
                    column_types[col] = 'float32'
                else:
                    column_types[col] = 'float32'  # Por defecto

            # Leer CSV con optimizaciones
            df = pd.read_csv(
                filepath,
                parse_dates=['timestamp'],
                dtype=column_types
            )

            rows = len(df)
            total_rows += rows
            logger.info(f"    ✓ {rows:,} filas cargadas")

            # Verificar valores nulos
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

def save_to_parquet(df, output_dir, symbol, year, quarter=None, full_year=False, logger=None, compression='snappy'):
    """Guarda el DataFrame consolidado en formato Parquet"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Nombre del archivo de salida según el período
    if full_year:
        filename = f"{symbol}_{year}.parquet"
        period_desc = f"Año {year}"
    else:
        filename = f"{symbol}_{year}-Q{quarter}.parquet"
        period_desc = f"{year}-Q{quarter}"

    filepath = output_path / filename

    logger.info(f"Guardando archivo: {filepath}")
    logger.info(f"  Símbolo: {symbol}")
    logger.info(f"  Período: {period_desc}")
    logger.info(f"  Compresión: {compression}")
    logger.info(f"  Filas: {len(df):,}")
    logger.info(f"  Columnas: {len(df.columns)}")

    try:
        df.to_parquet(
            filepath,
            compression=compression,
            index=False,
            engine='pyarrow'
        )

        file_size = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"  ✓ Archivo guardado correctamente")
        logger.info(f"  📦 Tamaño: {file_size:.2f} MB")

        return filepath

    except Exception as e:
        logger.error(f"  ✗ Error guardando archivo: {str(e)}")
        raise

def generate_summary(df, filepath, symbol, logger):
    """Genera un resumen del archivo consolidado"""
    logger.info("="*60)
    logger.info(f"RESUMEN DEL ARCHIVO CONSOLIDADO - {symbol}")
    logger.info("="*60)

    logger.info(f"Archivo: {filepath}")
    logger.info(f"Total filas: {len(df):,}")
    logger.info(f"Período: {df['timestamp'].min()} -> {df['timestamp'].max()}")

    # Calcular días en el período
    dias_totales = (df['timestamp'].max() - df['timestamp'].min()).days + 1
    logger.info(f"Días en el período: {dias_totales}")

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
    logger.info("📊 Estadísticas:")

    for col in ['open', 'high', 'low', 'close', 'spread']:
        if col in df.columns:
            logger.info(f"   {col.capitalize():6} - media: {df[col].mean():.6f}")
            logger.info(f"             - std:  {df[col].std():.6f}")

    if 'volume' in df.columns:
        logger.info(f"   Volume - media: {df['volume'].mean():.2f}")
        logger.info(f"           - % cero: {(df['volume'] == 0).mean()*100:.2f}%")

    # Estadísticas por mes (útil para año completo)
    if dias_totales > 100:
        logger.info("📅 Filas por mes:")
        df['mes'] = df['timestamp'].dt.month
        rows_per_month = df.groupby('mes').size()
        for mes, count in rows_per_month.items():
            logger.info(f"   Mes {mes:02d}: {count:>8,} filas")

def main():
    parser = argparse.ArgumentParser(
        description='Consolida archivos CSV mensuales por trimestre o año completo a Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Consolidar AUDUSD 2019 completo
  %(prog)s --symbol AUDUSD --year 2019 --full_year --input_dir ../DATOS_ORIGEN/AUDUSD --output_dir ../DATOS_ORIGEN/consolidated

  # Consolidar EURUSD Q1 2020
  %(prog)s --symbol EURUSD --year 2020 --quarter 1 --input_dir ../DATOS_ORIGEN/EURUSD --output_dir ../DATOS_ORIGEN/consolidated

  # Consolidar XAUUSD (por defecto)
  %(prog)s --year 2019 --full_year --input_dir ../DATOS_ORIGEN/XAUUSD --output_dir ../DATOS_ORIGEN/consolidated
        """
    )

    parser.add_argument(
        '--symbol',
        type=str,
        default='XAUUSD',
        help='Símbolo a procesar (default: XAUUSD)'
    )

    parser.add_argument(
        '--year',
        type=int,
        required=True,
        help='Año a procesar (ej: 2019)'
    )

    parser.add_argument(
        '--quarter',
        type=int,
        choices=[1, 2, 3, 4],
        help='Trimestre a procesar (1, 2, 3 o 4) - Requerido si no se usa --full_year'
    )

    parser.add_argument(
        '--full_year',
        action='store_true',
        help='Procesar año completo (ignora --quarter)'
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

    args = parser.parse_args()

    # Validar argumentos
    if not args.full_year and args.quarter is None:
        parser.error("Debe especificar --quarter o --full_year")

    if args.full_year and args.quarter:
        logging.warning("--full_year ignorará --quarter especificado")

    log_level = getattr(logging, args.log_level.upper())
    logger = setup_logging(args.output_dir, log_level)

    # Log de inicio
    logger.info("="*60)
    logger.info("INICIANDO CONSOLIDACIÓN")
    logger.info("="*60)
    logger.info(f"Símbolo: {args.symbol}")
    logger.info(f"Año: {args.year}")
    if args.full_year:
        logger.info(f"Período: Año completo")
    else:
        logger.info(f"Período: Q{args.quarter}")
    logger.info(f"Directorio entrada: {args.input_dir}")
    logger.info(f"Directorio salida: {args.output_dir}")
    logger.info(f"Compresión: {args.compression}")

    try:
        # Obtener meses del período
        months = get_months_for_period(args.year, args.quarter, args.full_year)
        logger.info(f"Meses a procesar: {months}")

        # Validar archivos CSV
        logger.info("Validando archivos CSV...")
        existing_files, missing_files = validate_csv_files(
            args.input_dir,
            args.symbol,
            args.year,
            months
        )

        if missing_files:
            logger.warning(f"Archivos faltantes: {missing_files}")
            if len(missing_files) == len(months):
                logger.error("No se encontró ningún archivo CSV para el período")
                sys.exit(1)
            logger.warning("Continuando solo con los archivos existentes...")

        if not existing_files:
            logger.error("No hay archivos CSV para procesar")
            sys.exit(1)

        logger.info(f"Archivos encontrados: {len(existing_files)}")
        for f in existing_files:
            logger.info(f"  ✓ {Path(f).name}")

        # Consolidar archivos CSV
        df = consolidate_csv_files(existing_files, logger)

        # Guardar a Parquet
        output_file = save_to_parquet(
            df,
            args.output_dir,
            args.symbol,
            args.year,
            args.quarter,
            args.full_year,
            logger,
            args.compression
        )

        # Generar resumen
        generate_summary(df, output_file, args.symbol, logger)

        logger.info("="*60)
        logger.info(f"✅ PROCESO COMPLETADO EXITOSAMENTE - {args.symbol}")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"❌ ERROR FATAL: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
