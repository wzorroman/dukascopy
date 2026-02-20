#!/usr/bin/env python3
"""
Parquet Consolidator - Consolidación Avanzada de Archivos Mensuales
===================================================================
Consolida archivos Parquet mensuales en anuales con validación y optimización.

Uso:
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011
"""

import pandas as pd
import glob
import os
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import numpy as np
import argparse
import logging
from tqdm import tqdm
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('consolidation.log')
    ]
)
logger = logging.getLogger(__name__)

class ParquetConsolidator:
    """
    Clase para consolidar archivos Parquet mensuales en archivos anuales.
    Soporta validación de esquemas, optimización y particionamiento.
    """
    
    def __init__(self, input_dir, output_dir):
        """
        Inicializa el consolidador con directorios de entrada y salida.
        
        Args:
            input_dir (str/Path): Directorio con archivos mensuales
            output_dir (str/Path): Directorio para archivos consolidados
        """
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        
        # Crear directorios si no existen
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📂 Directorio de entrada: {self.input_dir}")
        logger.info(f"📂 Directorio de salida: {self.output_dir}")
    
    def find_monthly_files(self, symbol, year, pattern=None):
        """
        Encuentra archivos mensuales para un símbolo y año específicos.
        
        Args:
            symbol (str): Símbolo del instrumento (ej: XAUUSD)
            year (int): Año a buscar
            pattern (str, optional): Patrón personalizado para buscar archivos
            
        Returns:
            list: Lista ordenada de rutas a archivos encontrados
        """
        if pattern:
            # Usar patrón personalizado
            search_pattern = pattern
        else:
            # Patrones comunes para archivos mensuales
            patterns = [
                # Patrones para .parquet y .ticks.parquet
                f"{symbol}_{year}_*.parquet",
                f"{symbol}_{year}_*.ticks.parquet",  # ¡Este es el importante!
                
                # También buscar específicamente por meses
                f"{symbol}_{year}_[0-9][0-9].parquet",
                f"{symbol}_{year}_[0-9][0-9].ticks.parquet",
                f"{symbol}_{year}_[0-9].parquet",
                f"{symbol}_{year}_[0-9].ticks.parquet",
            ]
            
            # Buscar con cada patrón
            all_files = []
            for pat in patterns:
                full_pattern = str(self.input_dir / pat)
                all_files.extend(glob.glob(full_pattern))
            
            # Eliminar duplicados y ordenar
            files = sorted(set(all_files))
            
            # Si no encontramos con patrones automáticos, buscar manualmente
            if not files:
                files = self._find_files_manually(symbol, year)
        
        return [Path(f) for f in files if Path(f).exists()]
    
    def _find_files_manually(self, symbol, year):
        """Búsqueda manual de archivos por meses"""
        files = []
        for month in range(1, 13):
            # Intentar diferentes formatos
            formats = [
                f"{symbol}_{year}_{month:02d}.parquet",
                f"{symbol}_{year}_{month}.parquet",
                f"{symbol}-{year}-{month:02d}.parquet",
                f"{symbol}_{year}_{month:02d}.parquet.snappy",
                f"{symbol}_{year}_{month:02d}.parquet.gzip",
            ]
            
            for fmt in formats:
                file_path = self.input_dir / fmt
                if file_path.exists():
                    files.append(str(file_path))
                    break
        
        return files
    
    def validate_files(self, files):
        """
        Valida que los archivos sean Parquet válidos y tengan esquemas compatibles.
        
        Args:
            files (list): Lista de rutas a archivos
            
        Returns:
            tuple: (bool, list) - Éxito y lista de errores
        """
        if not files:
            return False, ["No se encontraron archivos"]
        
        errors = []
        schemas = []
        
        logger.info(f"🔍 Validando {len(files)} archivos...")
        
        for i, file_path in enumerate(tqdm(files, desc="Validando archivos")):
            try:
                # Verificar que el archivo existe y es legible
                if not file_path.exists():
                    errors.append(f"Archivo no existe: {file_path.name}")
                    continue
                
                # Leer esquema del archivo Parquet
                table = pq.read_table(file_path, memory_map=True)
                schemas.append(table.schema)
                
                # Verificar columnas mínimas
                required_cols = ['timestamp', 'bid', 'ask']
                missing_cols = [col for col in required_cols if col not in table.column_names]
                if missing_cols:
                    errors.append(f"{file_path.name}: Faltan columnas {missing_cols}")
                
                # Verificar que timestamp sea de tipo datetime
                if 'timestamp' in table.column_names:
                    ts_type = table.schema.field('timestamp').type
                    if not pa.types.is_timestamp(ts_type):
                        errors.append(f"{file_path.name}: Columna timestamp no es tipo datetime")
                
                # Estadísticas del archivo
                file_size = file_path.stat().st_size / (1024 ** 2)
                num_rows = len(table)
                logger.debug(f"  {file_path.name}: {num_rows:,} filas, {file_size:.1f} MB")
                
            except Exception as e:
                errors.append(f"Error leyendo {file_path.name}: {str(e)}")
        
        # Validar consistencia de esquemas
        if schemas:
            first_schema = schemas[0]
            for i, schema in enumerate(schemas[1:], 1):
                if schema != first_schema:
                    errors.append(f"Esquema inconsistente: archivo {i} difiere del primero")
        
        return len(errors) == 0, errors
    
    def consolidate_year(self, symbol, year, compression='snappy', 
                         delete_originals=False, validate=True):
        """
        Consolida archivos mensuales en un archivo anual.
        
        Args:
            symbol (str): Símbolo del instrumento
            year (int): Año a consolidar
            compression (str): Método de compresión ('snappy', 'gzip', 'lz4', 'zstd')
            delete_originals (bool): Si True, elimina archivos originales después de consolidar
            validate (bool): Si True, valida archivos antes de consolidar
            
        Returns:
            Path: Ruta al archivo consolidado, o None si falla
        """
        logger.info(f"🔄 Iniciando consolidación de {symbol} {year}")
        
        # Encontrar archivos
        files = self.find_monthly_files(symbol, year)
        
        if not files:
            logger.error(f"❌ No se encontraron archivos para {symbol} {year}")
            logger.info(f"   Buscados en: {self.input_dir}")
            return None
        
        logger.info(f"📁 Encontrados {len(files)} archivos mensuales")
        
        # Validar archivos si se solicita
        if validate:
            valid, errors = self.validate_files(files)
            if not valid:
                logger.error("❌ Validación fallida:")
                for error in errors:
                    logger.error(f"   - {error}")
                logger.warning("⚠️  Continuando a pesar de errores de validación...")
        
        # Leer y consolidar archivos
        logger.info("📖 Leyendo archivos...")
        
        total_rows = 0
        total_size_mb = 0
        tables = []
        
        # Leer con progreso
        for file_path in tqdm(files, desc="Leyendo archivos"):
            try:
                # Leer archivo Parquet
                table = pq.read_table(file_path, memory_map=True)
                num_rows = len(table)
                file_size_mb = file_path.stat().st_size / (1024 ** 2)
                
                total_rows += num_rows
                total_size_mb += file_size_mb
                tables.append(table)
                
                logger.debug(f"  {file_path.name}: {num_rows:,} filas, {file_size_mb:.1f} MB")
                
            except Exception as e:
                logger.error(f"❌ Error leyendo {file_path.name}: {str(e)}")
                return None
        
        if not tables:
            logger.error("❌ No se pudieron leer archivos")
            return None
        
        logger.info(f"📊 Total preliminar: {total_rows:,} filas, {total_size_mb:.1f} MB")
        
        # Concatenar tablas
        logger.info("🔗 Concatenando tablas...")
        start_time = datetime.now()
        
        try:
            consolidated_table = pa.concat_tables(tables, promote=True)
        except Exception as e:
            logger.error(f"❌ Error concatenando tablas: {str(e)}")
            logger.info("Intentando con pandas como alternativa...")
            
            # Fallback a pandas
            return self._consolidate_with_pandas(files, symbol, year, compression)
        
        # Optimizar esquema
        logger.info("⚙️  Optimizando esquema...")
        consolidated_table = self._optimize_schema(consolidated_table)
        
        # Crear nombre de archivo de salida
        output_filename = f"{symbol}_{year}_FULL.parquet"
        output_path = self.output_dir / output_filename
        
        # Guardar archivo consolidado
        logger.info(f"💾 Guardando archivo consolidado: {output_path}")
        
        try:
            pq.write_table(
                consolidated_table,
                output_path,
                compression=compression,
                row_group_size=100000,  # Tamaño óptimo para consultas
                write_statistics=True,
                data_page_version='2.0',
                use_dictionary=True,  # Mejor compresión para columnas con valores repetidos
                compression_level=None if compression == 'snappy' else 9
            )
        except Exception as e:
            logger.error(f"❌ Error guardando archivo: {str(e)}")
            return None
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # Calcular estadísticas
        output_size_mb = output_path.stat().st_size / (1024 ** 2)
        compression_ratio = (1 - output_size_mb / total_size_mb) * 100 if total_size_mb > 0 else 0
        
        # Mostrar resumen
        logger.info("\n" + "=" * 60)
        logger.info("✅ CONSOLIDACIÓN COMPLETADA EXITOSAMENTE")
        logger.info("=" * 60)
        logger.info(f"📊 Estadísticas:")
        logger.info(f"   • Archivos consolidados: {len(files)}")
        logger.info(f"   • Filas totales: {len(consolidated_table):,}")
        logger.info(f"   • Columnas: {consolidated_table.num_columns}")
        logger.info(f"   • Tiempo de procesamiento: {elapsed:.1f} segundos")
        logger.info(f"   • Tamaño original: {total_size_mb:.1f} MB")
        logger.info(f"   • Tamaño consolidado: {output_size_mb:.1f} MB")
        logger.info(f"   • Ratio de compresión: {compression_ratio:.1f}%")
        logger.info(f"   • Compresión utilizada: {compression}")
        logger.info(f"   • Archivo de salida: {output_path}")
        logger.info("=" * 60)
        
        # Eliminar archivos originales si se solicita
        if delete_originals:
            logger.warning("🗑️  Eliminando archivos originales...")
            deleted_count = 0
            for file_path in files:
                try:
                    file_path.unlink()
                    deleted_count += 1
                    logger.debug(f"  Eliminado: {file_path.name}")
                except Exception as e:
                    logger.error(f"  Error eliminando {file_path.name}: {str(e)}")
            
            logger.info(f"✓ Archivos originales eliminados: {deleted_count}/{len(files)}")
        
        return output_path
    
    def _consolidate_with_pandas(self, files, symbol, year, compression):
        """Consolidación usando pandas como fallback"""
        logger.info("🔄 Usando pandas para consolidación...")
        
        dfs = []
        for file_path in tqdm(files, desc="Leyendo con pandas"):
            try:
                df = pd.read_parquet(file_path)
                dfs.append(df)
                logger.debug(f"  {file_path.name}: {len(df):,} filas")
            except Exception as e:
                logger.error(f"  Error: {str(e)}")
        
        if not dfs:
            return None
        
        # Concatenar DataFrames
        consolidated_df = pd.concat(dfs, ignore_index=True)
        
        # Ordenar por timestamp si existe
        if 'timestamp' in consolidated_df.columns:
            consolidated_df = consolidated_df.sort_values('timestamp').reset_index(drop=True)
        
        # Guardar
        output_filename = f"{symbol}_{year}_FULL.parquet"
        output_path = self.output_dir / output_filename
        
        try:
            consolidated_df.to_parquet(
                output_path,
                engine='pyarrow',
                compression=compression,
                index=False
            )
            logger.info(f"✅ Archivo guardado con pandas: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"❌ Error guardando con pandas: {str(e)}")
            return None
    
    def _optimize_schema(self, table):
        """Optimiza los tipos de datos para ahorrar espacio"""
        logger.debug("Optimizando tipos de datos...")
        
        optimized_fields = []
        
        for field in table.schema:
            field_name = field.name
            field_type = field.type
            
            # Optimizar tipos según el contenido de la columna
            new_type = self._get_optimal_type(field_name, field_type, table.column(field_name))
            optimized_fields.append(pa.field(field_name, new_type))
        
        # Crear nueva tabla con tipos optimizados
        optimized_schema = pa.schema(optimized_fields)
        
        # Convertir columnas a los nuevos tipos
        columns = []
        for field in optimized_fields:
            col = table.column(field.name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type)
                except:
                    # Si no se puede convertir, mantener tipo original
                    pass
            columns.append(col)
        
        return pa.Table.from_arrays(columns, schema=optimized_schema)
    
    def _get_optimal_type(self, column_name, current_type, column_data):
        """Determina el tipo óptimo para una columna"""
        # Para columnas de precios (float64 -> float32)
        price_columns = ['bid', 'ask', 'open', 'high', 'low', 'close', 'spread', 'mid']
        if column_name in price_columns and pa.types.is_floating(current_type):
            # Verificar si float32 es suficiente
            try:
                if column_data.type == pa.float64():
                    return pa.float32()
            except:
                pass
        
        # Para columnas de volumen (float64 -> int32 o float32)
        if column_name == 'volume' and pa.types.is_floating(current_type):
            # Verificar si todos los valores son enteros
            try:
                # Si es pequeño, muestrear para verificar
                sample_size = min(1000, len(column_data))
                if sample_size > 0:
                    sample = column_data.slice(0, sample_size).to_pandas()
                    if (sample.dropna() % 1 == 0).all():
                        return pa.int32()
                    else:
                        return pa.float32()
            except:
                pass
        
        # Para timestamps
        if column_name == 'timestamp' and pa.types.is_timestamp(current_type):
            # Usar microsegundos en lugar de nanosegundos si es posible
            if current_type.unit == 'ns':
                # Verificar si necesitamos nanosegundos
                try:
                    sample = column_data.slice(0, 100).to_pandas()
                    if (sample.dt.microsecond == 0).all():
                        return pa.timestamp('us')
                except:
                    pass
        
        return current_type
    
    def create_daily_partitions(self, consolidated_file, output_subdir=None):
        """
        Crea particiones diarias a partir de un archivo consolidado.
        
        Args:
            consolidated_file (Path): Ruta al archivo consolidado
            output_subdir (str, optional): Subdirectorio para particiones
            
        Returns:
            Path: Directorio con particiones
        """
        if not consolidated_file.exists():
            logger.error(f"❌ Archivo consolidado no encontrado: {consolidated_file}")
            return None
        
        # Crear directorio para particiones
        if output_subdir:
            partition_dir = self.output_dir / output_subdir
        else:
            partition_dir = self.output_dir / f"{consolidated_file.stem}_daily"
        
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"🗂️  Creando particiones diarias en: {partition_dir}")
        
        try:
            # Leer tabla completa
            table = pq.read_table(consolidated_file)
            
            if 'timestamp' not in table.column_names:
                logger.error("❌ No se encontró columna 'timestamp' para particionar")
                return None
            
            # Convertir a pandas para particionamiento fácil
            logger.info("📊 Convirtiendo a pandas para particionamiento...")
            df = table.to_pandas()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date
            
            # Contar días únicos
            unique_dates = df['date'].unique()
            logger.info(f"📅 Encontrados {len(unique_dates)} días únicos")
            
            # Crear particiones por día
            partitions_created = 0
            for date in tqdm(sorted(unique_dates), desc="Creando particiones"):
                try:
                    date_str = date.strftime('%Y%m%d')
                    
                    # Obtener datos del día
                    day_data = df[df['date'] == date].copy()
                    day_data = day_data.drop('date', axis=1)
                    
                    if len(day_data) > 0:
                        # Nombre del archivo basado en el símbolo si está disponible
                        symbol = consolidated_file.stem.split('_')[0]
                        daily_file = partition_dir / f"{symbol}_{date_str}.parquet"
                        
                        # Guardar partición
                        day_data.to_parquet(
                            daily_file,
                            compression='snappy',
                            index=False,
                            engine='pyarrow'
                        )
                        partitions_created += 1
                        
                except Exception as e:
                    logger.error(f"❌ Error creando partición para {date}: {str(e)}")
            
            logger.info(f"✅ Particiones creadas: {partitions_created}/{len(unique_dates)}")
            logger.info(f"📁 Directorio: {partition_dir}")
            
            # Crear archivo de índice para consultas rápidas
            self._create_partition_index(partition_dir, df)
            
            return partition_dir
            
        except Exception as e:
            logger.error(f"❌ Error en particionamiento: {str(e)}")
            return None
    
    def _create_partition_index(self, partition_dir, df):
        """Crea un archivo de índice para las particiones"""
        try:
            # Extraer información de cada partición
            index_data = []
            for parquet_file in partition_dir.glob("*.parquet"):
                try:
                    file_stats = pq.read_metadata(parquet_file)
                    num_rows = file_stats.num_rows
                    
                    # Intentar extraer fecha del nombre del archivo
                    date_str = parquet_file.stem.split('_')[-1]
                    if len(date_str) == 8 and date_str.isdigit():
                        date = datetime.strptime(date_str, '%Y%m%d').date()
                        index_data.append({
                            'date': date,
                            'filename': parquet_file.name,
                            'rows': num_rows,
                            'size_mb': parquet_file.stat().st_size / (1024 ** 2)
                        })
                except:
                    pass
            
            if index_data:
                index_df = pd.DataFrame(index_data)
                index_df = index_df.sort_values('date')
                index_file = partition_dir / "partition_index.csv"
                index_df.to_csv(index_file, index=False)
                logger.info(f"📋 Índice creado: {index_file}")
                
        except Exception as e:
            logger.warning(f"⚠️  No se pudo crear índice: {str(e)}")


def main():
    """Función principal con argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description='Consolida archivos Parquet mensuales en archivos anuales',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Consolidación básica
  python %(prog)s --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011
  
  # Consolidación con compresión GZIP
  python %(prog)s --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011 --compression gzip
  
  # Consolidación y creación de particiones diarias
  python %(prog)s --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011 --partition-daily
  
  # Consolidación múltiples años
  python %(prog)s --input-dir data --output-dir consolidated --symbol XAUUSD --start-year 2010 --end-year 2015
  
  # Modo batch para múltiples símbolos
  python %(prog)s --input-dir data --output-dir consolidated --symbols XAUUSD,EURUSD,GBPUSD --year 2023
        """
    )
    
    # Argumentos principales
    parser.add_argument('--input-dir', '-i', required=True,
                       help='Directorio con archivos Parquet mensuales')
    parser.add_argument('--output-dir', '-o', required=True,
                       help='Directorio para archivos consolidados')
    
    # Opciones de símbolo/año
    symbol_group = parser.add_mutually_exclusive_group()
    symbol_group.add_argument('--symbol', help='Símbolo individual (ej: XAUUSD)')
    symbol_group.add_argument('--symbols', help='Múltiples símbolos separados por coma')
    
    # Mantener solo --year en un grupo exclusivo si quieres que sea opcional
    group_year = parser.add_mutually_exclusive_group()
    group_year.add_argument('--year', type=int, help='Procesar un solo año')

    # Colocar los límites del rango como argumentos independientes
    parser.add_argument('--start-year', type=int, help='Año de inicio del rango')
    parser.add_argument('--end-year', type=int, help='Año de fin del rango')
    
    # Opciones de procesamiento
    parser.add_argument('--compression', default='snappy',
                       choices=['snappy', 'gzip', 'lz4', 'zstd'],
                       help='Método de compresión (default: snappy)')
    parser.add_argument('--partition-daily', action='store_true',
                       help='Crear particiones diarias adicionales')
    parser.add_argument('--delete-originals', action='store_true',
                       help='Eliminar archivos originales después de consolidar (PELIGROSO)')
    parser.add_argument('--skip-validation', action='store_true',
                       help='Saltar validación de archivos')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Modo verboso con más detalles')
    
    args = parser.parse_args()
    
    # Configurar nivel de logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Validar argumentos
    if not args.symbol and not args.symbols:
        parser.error("Debe especificar --symbol o --symbols")
    
    if not args.year and not (args.start_year and args.end_year):
        parser.error("Debe especificar --year o --start-year/--end-year")
    
    # Preparar lista de símbolos
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    else:
        symbols = [args.symbol]
    
    # Preparar lista de años
    if args.year:
        years = [args.year]
    else:
        if args.start_year > args.end_year:
            parser.error("start-year debe ser menor o igual a end-year")
        years = list(range(args.start_year, args.end_year + 1))
    
    logger.info("=" * 60)
    logger.info("🔄 CONSOLIDADOR AVANZADO DE ARCHIVOS PARQUET")
    logger.info("=" * 60)
    logger.info(f"📂 Entrada: {args.input_dir}")
    logger.info(f"📂 Salida: {args.output_dir}")
    logger.info(f"📊 Símbolos: {', '.join(symbols)}")
    logger.info(f"📅 Años: {', '.join(map(str, years))}")
    logger.info(f"⚙️  Compresión: {args.compression}")
    logger.info(f"🗂️  Particiones diarias: {'SÍ' if args.partition_daily else 'NO'}")
    logger.info(f"🗑️  Eliminar originales: {'SÍ' if args.delete_originals else 'NO'}")
    logger.info(f"🔍 Validación: {'NO' if args.skip_validation else 'SÍ'}")
    logger.info("=" * 60)
    
    # Procesar cada combinación de símbolo y año
    consolidator = ParquetConsolidator(args.input_dir, args.output_dir)
    successful = []
    failed = []
    
    for symbol in symbols:
        for year in years:
            logger.info(f"\n🎯 Procesando {symbol} {year}...")
            
            try:
                # Consolidar año
                output_file = consolidator.consolidate_year(
                    symbol=symbol,
                    year=year,
                    compression=args.compression,
                    delete_originals=args.delete_originals,
                    validate=not args.skip_validation
                )
                
                if output_file:
                    # Crear particiones si se solicita
                    if args.partition_daily:
                        logger.info(f"🗂️  Creando particiones diarias para {symbol} {year}...")
                        partition_dir = consolidator.create_daily_partitions(output_file)
                        if partition_dir:
                            logger.info(f"✅ Particiones creadas en: {partition_dir}")
                    
                    successful.append((symbol, year, output_file))
                    logger.info(f"✅ {symbol} {year} consolidado exitosamente")
                else:
                    failed.append((symbol, year, "Consolidación fallida"))
                    logger.error(f"❌ {symbol} {year} falló")
                    
            except Exception as e:
                failed.append((symbol, year, str(e)))
                logger.error(f"❌ Error procesando {symbol} {year}: {str(e)}", exc_info=args.verbose)
    
    # Mostrar resumen final
    logger.info("\n" + "=" * 60)
    logger.info("📊 RESUMEN FINAL DE CONSOLIDACIÓN")
    logger.info("=" * 60)
    
    if successful:
        logger.info(f"✅ PROCESADOS EXITOSAMENTE: {len(successful)}")
        for symbol, year, output_file in successful:
            file_size = output_file.stat().st_size / (1024 ** 2)
            logger.info(f"   • {symbol} {year}: {output_file.name} ({file_size:.1f} MB)")
    
    if failed:
        logger.info(f"❌ FALLIDOS: {len(failed)}")
        for symbol, year, error in failed:
            logger.info(f"   • {symbol} {year}: {error}")
    
    logger.info("=" * 60)
    
    if failed:
        logger.warning("⚠️  Algunos procesamientos fallaron")
        sys.exit(1)
    else:
        logger.info("🎉 ¡Todas las consolidaciones completadas exitosamente!")
        sys.exit(0)


if __name__ == '__main__':
    # python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011

    main()
