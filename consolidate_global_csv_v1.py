#!/usr/bin/env python3
"""
Global CSV Consolidator - Consolidación de archivos Parquet anuales
===================================================================
Consolida archivos Parquet de diferentes años en un único archivo global.

Uso:
    # Consolidar años específicos
    python consolidate_global_csv_v1.py --input 2019,2020,2021,2022 --output ../DATOS_ORIGEN/consolidated_csv/XAUUSD_2019-2022.parquet

    # Usar rango de años
    python consolidate_global_csv_v1.py --input 2019-2023 --output ../DATOS_ORIGEN/consolidated_csv/XAUUSD_2019-2023.parquet

    # Saltar validación (más rápido)
    python consolidate_global_csv_v1.py --input 2020-2022 --output ../DATOS_ORIGEN/consolidated_csv/XAUUSD_2020-2022.parquet --skip-validation

    # Directorio base personalizado
    python consolidate_global_csv_v1.py --input 2019-2023 --output ./mi_salida/XAUUSD_2019-2023.parquet --base-dir ./mis_datos

    # Años no consecutivos (funciona perfectamente)
    python consolidate_global_csv.py --input 2015,2018,2020,2023 --output ../DATOS_ORIGEN/consolidated_csv/XAUUSD_2015-2023_selected.parquet

    # Años sueltos
    python consolidate_global_csv.py --input 2020 --output ../DATOS_ORIGEN/consolidated_csv/XAUUSD_2020_only.parquet
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import argparse
import logging
from datetime import datetime
import numpy as np
from tqdm import tqdm
import sys
import os
import re

def setup_logging(log_dir, log_level=logging.INFO):
    """
    Configura el logging con salida a archivo y consola.
    Los logs se guardan en el directorio especificado.

    Args:
        log_dir (str/Path): Directorio donde guardar los archivos de log
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR)

    Returns:
        logging.Logger: Logger configurado
    """
    # Crear directorio de logs si no existe
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Nombre del archivo de log con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"consolidar_global_{timestamp}.log"

    # Configurar logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"📋 Archivo de log: {log_file}")

    return logger

class GlobalCSVConsolidator:
    """Consolida archivos Parquet anuales en un archivo global."""

    def __init__(self, input_years, output_file, base_dir="../DATOS_ORIGEN/consolidated_csv", logger=None):
        """
        Inicializa el consolidador global.

        Args:
            input_years (list): Lista de años a consolidar
            output_file (str): Archivo de salida global
            base_dir (str): Directorio base donde se encuentran los archivos anuales
            logger (logging.Logger, optional): Logger para registrar eventos
        """
        self.base_dir = Path(base_dir).resolve()
        self.input_years = input_years
        self.output_file = Path(output_file).resolve()
        self.symbol = self._extract_symbol_from_output()
        self.logger = logger or logging.getLogger(__name__)

        # Crear directorio de salida si no existe
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info("=" * 70)
        self.logger.info("🌍 CONSOLIDACIÓN GLOBAL DE ARCHIVOS PARQUET ANUALES")
        self.logger.info("=" * 70)
        self.logger.info(f"Años a consolidar: {', '.join(map(str, input_years))}")
        self.logger.info(f"Directorio base: {self.base_dir}")
        self.logger.info(f"Archivo salida: {self.output_file}")
        self.logger.info(f"Símbolo detectado: {self.symbol}")
        self.logger.info("=" * 70)

    def _extract_symbol_from_output(self):
        """Extrae el símbolo del nombre del archivo de salida."""
        # Asumir formato: SYMBOL_YYYY-YYYY.parquet o SYMBOL_GLOBAL.parquet
        filename = self.output_file.stem
        parts = filename.split('_')
        if len(parts) >= 1 and parts[0].isalpha():
            return parts[0]
        return "XAUUSD"  # Símbolo por defecto

    def _build_file_paths(self):
        """
        Construye las rutas completas de los archivos a consolidar.

        Returns:
            list: Lista de rutas válidas a archivos Parquet
        """
        files = []
        missing_years = []

        self.logger.info("Buscando archivos anuales...")

        for year in self.input_years:
            # Posibles formatos de nombre de archivo
            possible_names = [
                f"{self.symbol}_{year}.parquet",
                f"{self.symbol}_{year}_FULL.parquet",
                f"XAUUSD_{year}.parquet",  # Formato por defecto
                f"{year}.parquet"
            ]

            found = False
            for name in possible_names:
                file_path = self.base_dir / name
                if file_path.exists():
                    files.append(file_path)
                    self.logger.info(f"  ✓ Año {year}: {name}")
                    found = True
                    break

            if not found:
                missing_years.append(str(year))
                self.logger.warning(f"  ⚠️ Año {year}: No se encontró archivo")

        if missing_years:
            self.logger.warning(f"  Archivos no encontrados para años: {', '.join(missing_years)}")

        if not files:
            self.logger.error("❌ No se encontraron archivos para consolidar")
            return []

        # Ordenar cronológicamente
        files.sort()
        self.logger.info(f"✅ Total archivos encontrados: {len(files)}")

        return files

    def validate_files(self, files):
        """
        Valida que los archivos sean consistentes.

        Args:
            files (list): Lista de archivos a validar

        Returns:
            tuple: (bool, list, dict) - Éxito, lista de errores, estadísticas por año
        """
        if not files:
            return False, ["No se encontraron archivos para consolidar"], {}

        errors = []
        schemas = []
        year_stats = {}

        self.logger.info("Validando archivos Parquet...")

        for file_path in tqdm(files, desc="Validando archivos", file=sys.stdout):
            try:
                # Leer metadatos del archivo Parquet
                parquet_file = pq.ParquetFile(file_path)
                schema = parquet_file.schema_arrow
                num_rows = parquet_file.metadata.num_rows
                num_columns = len(schema)

                # Extraer año del nombre del archivo
                year_match = re.search(r'(\d{4})', file_path.name)
                year = int(year_match.group(1)) if year_match else None

                # Almacenar estadísticas
                year_stats[year] = {
                    'file': file_path.name,
                    'rows': num_rows,
                    'columns': num_columns,
                    'size_mb': file_path.stat().st_size / (1024 ** 2)
                }

                # Verificar columnas esenciales
                essential_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                schema_cols = schema.names
                missing_cols = [col for col in essential_cols if col not in schema_cols]

                if missing_cols:
                    errors.append(f"{file_path.name}: Faltan columnas esenciales {missing_cols}")

                # Guardar esquema para comparación
                schemas.append(schema)

                self.logger.debug(f"  {file_path.name}: {num_rows:,} filas, {num_columns} columnas")

            except Exception as e:
                errors.append(f"Error leyendo {file_path.name}: {str(e)}")

        # Validar consistencia de esquemas
        if len(schemas) > 1:
            first_schema = schemas[0]
            for i, schema in enumerate(schemas[1:], 1):
                if schema.names != first_schema.names:
                    errors.append(f"Esquema inconsistente: {files[i].name} tiene columnas diferentes")
                else:
                    # Verificar tipos de datos
                    for j, field in enumerate(schema):
                        if field.type != first_schema[j].type:
                            errors.append(f"Tipo inconsistente en columna '{field.name}': {first_schema[j].type} vs {field.type}")

        if not errors:
            self.logger.info("✅ Validación completada sin errores")
        else:
            self.logger.warning(f"⚠️ Se encontraron {len(errors)} errores de validación")

        return len(errors) == 0, errors, year_stats

    def consolidate(self, skip_validation=False, compression='snappy'):
        """
        Ejecuta la consolidación de archivos.

        Args:
            skip_validation (bool): Si True, salta la validación de archivos
            compression (str): Método de compresión para Parquet

        Returns:
            Path: Ruta al archivo consolidado, o None si falla
        """
        # Construir rutas de archivos
        files = self._build_file_paths()

        if not files:
            self.logger.error("❌ No se encontraron archivos para consolidar")
            return None

        self.logger.info(f"\n📁 Archivos a procesar: {len(files)}")

        # Validar archivos
        year_stats = {}
        if not skip_validation:
            valid, errors, year_stats = self.validate_files(files)

            if not valid:
                self.logger.error("❌ Errores de validación:")
                for error in errors[:5]:  # Mostrar solo primeros 5
                    self.logger.error(f"  - {error}")
                if len(errors) > 5:
                    self.logger.error(f"  ... y {len(errors) - 5} errores más")

                # Preguntar si continuar
                response = input("\n⚠️ ¿Desea continuar a pesar de los errores? (s/N): ")
                if response.lower() != 's':
                    self.logger.warning("Consolidación cancelada por el usuario")
                    return None
                self.logger.warning("Continuando a pesar de errores de validación...")
        else:
            # Estadísticas básicas sin validación profunda
            for file_path in files:
                year_match = re.search(r'(\d{4})', file_path.name)
                year = int(year_match.group(1)) if year_match else None
                try:
                    parquet_file = pq.ParquetFile(file_path)
                    year_stats[year] = {
                        'file': file_path.name,
                        'rows': parquet_file.metadata.num_rows,
                        'size_mb': file_path.stat().st_size / (1024 ** 2)
                    }
                except:
                    year_stats[year] = {
                        'file': file_path.name,
                        'rows': 'Desconocido',
                        'size_mb': file_path.stat().st_size / (1024 ** 2)
                    }

        # Consolidar archivos
        self.logger.info("\n" + "=" * 70)
        self.logger.info("📊 CONSOLIDANDO ARCHIVOS ANUALES")
        self.logger.info("=" * 70)

        start_time = datetime.now()
        total_rows = 0
        total_size_mb = 0
        dfs = []

        # Leer archivos con pandas
        self.logger.info("Leyendo archivos Parquet...")

        for file_path in tqdm(files, desc="Procesando archivos", file=sys.stdout):
            try:
                # Leer archivo Parquet
                df = pd.read_parquet(file_path)
                num_rows = len(df)
                file_size_mb = file_path.stat().st_size / (1024 ** 2)

                # Verificar que tenga la columna timestamp
                if 'timestamp' not in df.columns:
                    self.logger.error(f"  {file_path.name}: No tiene columna 'timestamp'")
                    continue

                # Asegurar que timestamp sea datetime
                if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                    df['timestamp'] = pd.to_datetime(df['timestamp'])

                dfs.append(df)
                total_rows += num_rows
                total_size_mb += file_size_mb

                self.logger.info(f"  ✓ {file_path.name}: {num_rows:,} filas cargadas")

            except Exception as e:
                self.logger.error(f"  ❌ Error leyendo {file_path.name}: {str(e)}")
                return None

        if not dfs:
            self.logger.error("❌ No se pudieron leer archivos")
            return None

        # Concatenar DataFrames
        self.logger.info("\n🔗 Concatenando DataFrames...")
        try:
            consolidated_df = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"  ✓ Total filas después de concatenar: {len(consolidated_df):,}")
        except Exception as e:
            self.logger.error(f"❌ Error concatenando DataFrames: {str(e)}")
            return None

        # Ordenar por timestamp
        self.logger.info("📅 Ordenando por timestamp...")
        try:
            consolidated_df = consolidated_df.sort_values('timestamp').reset_index(drop=True)
            self.logger.info("  ✓ Ordenamiento completado")
        except Exception as e:
            self.logger.error(f"❌ Error ordenando: {str(e)}")
            return None

        # Eliminar duplicados si existen
        initial_rows = len(consolidated_df)
        consolidated_df = consolidated_df.drop_duplicates(subset=['timestamp'], keep='first')
        if len(consolidated_df) < initial_rows:
            self.logger.info(f"  ✓ Eliminados {initial_rows - len(consolidated_df):,} duplicados")

        # Guardar archivo consolidado
        self.logger.info(f"\n💾 Guardando archivo: {self.output_file}")

        try:
            # Guardar como Parquet con compresión especificada
            consolidated_df.to_parquet(
                self.output_file,
                engine='pyarrow',
                compression=compression,
                index=False
            )

            # Verificar que se guardó correctamente
            if self.output_file.exists():
                file_size_mb = self.output_file.stat().st_size / (1024 ** 2)
                self.logger.info(f"  ✓ Archivo guardado correctamente")
                self.logger.info(f"  📦 Tamaño: {file_size_mb:.2f} MB")
            else:
                self.logger.error("  ❌ El archivo no se creó correctamente")
                return None

        except Exception as e:
            self.logger.error(f"❌ Error guardando archivo: {str(e)}")
            return None

        elapsed_time = (datetime.now() - start_time).total_seconds()

        # Mostrar resumen
        self._display_summary(
            consolidated_df=consolidated_df,
            year_stats=year_stats,
            total_size_mb=total_size_mb,
            output_size_mb=file_size_mb,
            elapsed_time=elapsed_time,
            compression=compression
        )

        return self.output_file

    def _display_summary(self, consolidated_df, year_stats, total_size_mb, output_size_mb, elapsed_time, compression):
        """Muestra un resumen detallado de la consolidación."""

        self.logger.info("\n" + "=" * 70)
        self.logger.info("📋 RESUMEN DE CONSOLIDACIÓN GLOBAL")
        self.logger.info("=" * 70)

        # Información general
        self.logger.info(f"📁 Archivo: {self.output_file}")
        self.logger.info(f"📊 Total filas: {len(consolidated_df):,}")
        self.logger.info(f"📋 Total columnas: {len(consolidated_df.columns)}")

        # Rango de fechas
        if 'timestamp' in consolidated_df.columns:
            min_date = consolidated_df['timestamp'].min()
            max_date = consolidated_df['timestamp'].max()
            days_range = (max_date - min_date).days
            self.logger.info(f"📅 Período: {min_date} -> {max_date}")
            self.logger.info(f"📆 Días en el período: {days_range}")
            self.logger.info(f"🗓️ Años cubiertos: {days_range/365.25:.1f} años")

        # Estadísticas de tamaño
        self.logger.info(f"\n💾 ESTADÍSTICAS DE TAMAÑO:")
        self.logger.info(f"   • Tamaño original total: {total_size_mb:.2f} MB")
        self.logger.info(f"   • Tamaño consolidado: {output_size_mb:.2f} MB")

        if total_size_mb > 0:
            compression_ratio = (1 - output_size_mb / total_size_mb) * 100
            self.logger.info(f"   • Ratio compresión: {compression_ratio:.1f}%")
            self.logger.info(f"   • Compresión usada: {compression}")

        # Tiempo de procesamiento
        self.logger.info(f"\n⏱️ TIEMPO DE PROCESAMIENTO:")
        self.logger.info(f"   • Tiempo total: {elapsed_time:.2f} segundos")

        if elapsed_time > 0:
            rows_per_second = len(consolidated_df) / elapsed_time
            mb_per_second = total_size_mb / elapsed_time
            self.logger.info(f"   • Rendimiento: {rows_per_second:,.0f} filas/segundo")
            self.logger.info(f"   • Throughput: {mb_per_second:.1f} MB/segundo")

        # Desglose por año
        self.logger.info(f"\n📅 FILAS POR AÑO:")

        # Calcular filas por año desde los datos consolidados
        if 'timestamp' in consolidated_df.columns:
            yearly_counts = consolidated_df['timestamp'].dt.year.value_counts().sort_index()
            for year, count in yearly_counts.items():
                # Buscar estadísticas del año
                year_stat = year_stats.get(year, {})
                file_name = year_stat.get('file', 'Desconocido')
                self.logger.info(f"   • {year}: {count:>10,} filas ({file_name})")

        # Verificar continuidad temporal
        if 'timestamp' in consolidated_df.columns:
            time_diffs = consolidated_df['timestamp'].diff().dt.total_seconds()
            expected_diff = 60  # 60 segundos = 1 minuto
            gaps = time_diffs[time_diffs > expected_diff * 1.5]

            self.logger.info(f"\n🔍 VERIFICACIÓN DE CONTINUIDAD:")
            if len(gaps) > 0:
                self.logger.warning(f"   • Gaps encontrados: {len(gaps)}")
                self.logger.warning(f"   • Gap más grande: {time_diffs.max()/60:.1f} minutos")
                self.logger.warning(f"   • Gaps > 5 min: {(time_diffs > 300).sum()}")
                self.logger.warning(f"   • Gaps > 1 hora: {(time_diffs > 3600).sum()}")
            else:
                self.logger.info(f"   • ✅ Secuencia temporal perfecta (sin gaps)")

        # Estadísticas básicas de precios
        self.logger.info(f"\n📈 ESTADÍSTICAS DE PRECIOS:")
        numeric_cols = ['open', 'high', 'low', 'close', 'spread', 'volume']
        available_cols = [col for col in numeric_cols if col in consolidated_df.columns]

        if available_cols:
            stats = consolidated_df[available_cols].describe()
            for col in available_cols[:3]:  # Mostrar primeras 3 columnas
                mean_val = stats[col]['mean']
                std_val = stats[col]['std']
                min_val = stats[col]['min']
                max_val = stats[col]['max']
                self.logger.info(f"   • {col.capitalize():6} - media: {mean_val:.6f}")
                self.logger.info(f"                 - std:  {std_val:.6f}")
                self.logger.info(f"                 - rango: [{min_val:.6f}, {max_val:.6f}]")

        # Estadísticas de volumen
        if 'volume' in consolidated_df.columns:
            zero_volume_pct = (consolidated_df['volume'] == 0).mean() * 100
            self.logger.info(f"   • Volume - % cero: {zero_volume_pct:.2f}%")

        self.logger.info("=" * 70)
        self.logger.info("✅ PROCESO COMPLETADO EXITOSAMENTE")
        self.logger.info("=" * 70)


def parse_years(years_str):
    """
    Parsea la cadena de años en una lista.

    Args:
        years_str (str): Cadena con años (ej: "2019,2020,2021" o "2019-2022")

    Returns:
        list: Lista de años como enteros
    """
    if '-' in years_str:
        # Formato rango: 2019-2022
        start, end = map(int, years_str.split('-'))
        return list(range(start, end + 1))
    else:
        # Formato lista separada por comas
        return [int(y.strip()) for y in years_str.split(',')]


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(
        description='Consolida archivos Parquet anuales en un archivo global',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS DE USO:
  # Consolidar años consecutivos (rango)
  python consolidate_global_csv.py --input 2019-2022 --output consolidated_csv/XAUUSD_2019-2022.parquet

  # Consolidar años específicos (no consecutivos)
  python consolidate_global_csv.py --input 2015,2018,2020,2023 --output consolidated_csv/XAUUSD_selected.parquet

  # Consolidar un solo año
  python consolidate_global_csv.py --input 2020 --output consolidated_csv/XAUUSD_2020.parquet

  # Saltar validación para proceso más rápido
  python consolidate_global_csv.py --input 2020-2023 --output consolidated_csv/XAUUSD_2020-2023.parquet --skip-validation

  # Usar compresión diferente
  python consolidate_global_csv.py --input 2019-2023 --output consolidated_csv/XAUUSD_2019-2023.parquet --compression zstd

  # Directorio base personalizado
  python consolidate_global_csv.py --input 2019-2023 --output ./salida/XAUUSD_2019-2023.parquet --base-dir ./mis_datos

  # Modo verboso (DEBUG)
  python consolidate_global_csv.py --input 2020-2022 --output XAUUSD_2020-2022.parquet --log-level DEBUG
        """
    )

    parser.add_argument('--input', '-i', required=True,
                       help='Años a consolidar (ej: 2019,2020,2021 o 2019-2022)')

    parser.add_argument('--output', '-o', required=True,
                       help='Archivo de salida (ej: consolidated_csv/XAUUSD_2019-2022.parquet)')

    parser.add_argument('--base-dir', '-b',
                       default='../DATOS_ORIGEN/consolidated_csv',
                       help='Directorio base con archivos anuales (default: ../DATOS_ORIGEN/consolidated_csv)')

    parser.add_argument('--skip-validation', action='store_true',
                       help='Saltar validación de archivos')

    parser.add_argument('--compression', '-c',
                       default='snappy',
                       choices=['snappy', 'gzip', 'brotli', 'lz4', 'zstd'],
                       help='Método de compresión para Parquet (default: snappy)')

    parser.add_argument('--log-level',
                       default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Nivel de logging (default: INFO)')

    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Modo verboso (equivalente a --log-level DEBUG)')

    args = parser.parse_args()

    # Configurar nivel de logging
    log_level = args.log_level
    if args.verbose:
        log_level = 'DEBUG'

    # Convertir nivel de logging
    numeric_log_level = getattr(logging, log_level.upper())

    # Configurar logging (los logs se guardan en el mismo directorio que el output)
    output_dir = Path(args.output).parent
    logger = setup_logging(output_dir, numeric_log_level)

    # Parsear años
    try:
        years = parse_years(args.input)
        logger.info(f"📅 Años solicitados: {years}")
    except ValueError as e:
        logger.error(f"❌ Error parseando años: {e}")
        sys.exit(1)

    # Crear consolidador
    consolidator = GlobalCSVConsolidator(
        input_years=years,
        output_file=args.output,
        base_dir=args.base_dir,
        logger=logger
    )

    # Ejecutar consolidación
    output_file = consolidator.consolidate(
        skip_validation=args.skip_validation,
        compression=args.compression
    )

    if output_file:
        logger.info(f"\n✨ ¡Consolidación global completada exitosamente!")
        logger.info(f"📁 Archivo creado: {output_file}")

        # Mostrar tamaño final
        if output_file.exists():
            size_mb = output_file.stat().st_size / (1024 ** 2)
            logger.info(f"📈 Tamaño final: {size_mb:.2f} MB")

        sys.exit(0)
    else:
        logger.error("\n❌ La consolidación global falló")
        sys.exit(1)


if __name__ == '__main__':
    main()
