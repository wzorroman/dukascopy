#!/usr/bin/env python3
"""
Global Year Consolidator - Consolidación Anual Global
=====================================================
Consolida archivos anuales en un archivo global con todos los años.

Uso:
    python consolidate_global.py --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet --years 2010-2024
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
import glob

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('global_consolidation.log')
    ]
)
logger = logging.getLogger(__name__)

class GlobalConsolidator:
    """Consolida archivos anuales en un archivo global."""
    
    def __init__(self, input_dir, output_file):
        """
        Inicializa el consolidador global.
        
        Args:
            input_dir (str/Path): Directorio con archivos anuales
            output_file (str/Path): Archivo de salida global
        """
        self.input_dir = Path(input_dir).resolve()
        self.output_file = Path(output_file).resolve()
        
        # Crear directorios si no existen
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📂 Directorio de entrada: {self.input_dir}")
        logger.info(f"📂 Archivo de salida: {self.output_file}")
    
    def find_yearly_files(self, symbol=None, years=None):
        """
        Encuentra archivos anuales en el directorio de entrada.
        
        Args:
            symbol (str, optional): Filtrar por símbolo
            years (list, optional): Filtrar por años específicos
            
        Returns:
            list: Lista ordenada de archivos anuales encontrados
        """
        # Patrones de búsqueda
        if symbol:
            patterns = [
                f"{symbol}_*_FULL.parquet",
                f"{symbol}_*_FULL.parquet.*",
                f"{symbol}_[0-9][0-9][0-9][0-9].parquet",
                f"{symbol}_[0-9][0-9][0-9][0-9]_FULL.parquet",
            ]
        else:
            patterns = [
                "*_FULL.parquet",
                "*_FULL.parquet.*",
                "*_[0-9][0-9][0-9][0-9].parquet",
            ]
        
        # Buscar archivos
        all_files = []
        for pattern in patterns:
            full_pattern = str(self.input_dir / pattern)
            all_files.extend(glob.glob(full_pattern))
        
        # Eliminar duplicados y ordenar
        files = sorted(set([Path(f) for f in all_files if Path(f).exists()]))
        
        # Filtrar por años si se especifica
        if years:
            filtered_files = []
            for file in files:
                # Extraer año del nombre del archivo
                year_from_file = self._extract_year_from_filename(file.name)
                if year_from_file and year_from_file in years:
                    filtered_files.append(file)
            files = filtered_files
        
        # Filtrar por símbolo si se especifica
        if symbol:
            filtered_files = []
            for file in files:
                # Extraer símbolo del nombre del archivo
                symbol_from_file = self._extract_symbol_from_filename(file.name)
                if symbol_from_file == symbol:
                    filtered_files.append(file)
            files = filtered_files
        
        return files
    
    def _extract_year_from_filename(self, filename):
        """Extrae el año del nombre del archivo."""
        import re
        # Buscar 4 dígitos consecutivos
        match = re.search(r'(\d{4})', filename)
        if match:
            try:
                return int(match.group(1))
            except:
                return None
        return None
    
    def _extract_symbol_from_filename(self, filename):
        """Extrae el símbolo del nombre del archivo."""
        # Asumir formato: SYMBOL_YYYY_FULL.parquet
        parts = filename.split('_')
        if len(parts) >= 2:
            return parts[0]
        return None
    
    def validate_files(self, files):
        """
        Valida que los archivos sean consistentes.
        
        Args:
            files (list): Lista de archivos a validar
            
        Returns:
            tuple: (bool, list) - Éxito y lista de errores
        """
        if not files:
            return False, ["No se encontraron archivos"]
        
        errors = []
        schemas = []
        symbols = set()
        
        logger.info(f"🔍 Validando {len(files)} archivos...")
        
        for i, file_path in enumerate(tqdm(files, desc="Validando archivos")):
            try:
                # Verificar que el archivo existe
                if not file_path.exists():
                    errors.append(f"Archivo no existe: {file_path.name}")
                    continue
                
                # Leer esquema y estadísticas
                table = pq.read_table(file_path, memory_map=True)
                schemas.append(table.schema)
                
                # Extraer símbolo
                symbol = self._extract_symbol_from_filename(file_path.name)
                if symbol:
                    symbols.add(symbol)
                
                # Verificar columnas esenciales
                essential_cols = ['timestamp']
                missing_cols = [col for col in essential_cols if col not in table.column_names]
                if missing_cols:
                    errors.append(f"{file_path.name}: Faltan columnas {missing_cols}")
                
                # Verificar tipo de timestamp
                if 'timestamp' in table.column_names:
                    ts_type = table.schema.field('timestamp').type
                    if not pa.types.is_timestamp(ts_type):
                        errors.append(f"{file_path.name}: timestamp no es tipo datetime")
                
                # Estadísticas del archivo
                file_size = file_path.stat().st_size / (1024 ** 2)
                num_rows = len(table)
                logger.debug(f"  {file_path.name}: {num_rows:,} filas, {file_size:.1f} MB")
                
            except Exception as e:
                errors.append(f"Error leyendo {file_path.name}: {str(e)}")
        
        # Validar que todos los archivos sean del mismo símbolo
        if len(symbols) > 1:
            errors.append(f"Múltiples símbolos encontrados: {symbols}")
        
        # Validar consistencia de esquemas
        if schemas:
            first_schema = schemas[0]
            for i, schema in enumerate(schemas[1:], 1):
                if schema != first_schema:
                    # Verificar diferencias específicas
                    diff = self._compare_schemas(first_schema, schema)
                    errors.append(f"Esquema inconsistente en archivo {i}: {diff}")
        
        return len(errors) == 0, errors
    
    def _compare_schemas(self, schema1, schema2):
        """Compara dos esquemas y devuelve diferencias."""
        diff = []
        
        # Comparar columnas
        cols1 = set(schema1.names)
        cols2 = set(schema2.names)
        
        missing_in_2 = cols1 - cols2
        missing_in_1 = cols2 - cols1
        
        if missing_in_2:
            diff.append(f"Columnas faltantes en segundo: {missing_in_2}")
        if missing_in_1:
            diff.append(f"Columnas extra en segundo: {missing_in_1}")
        
        # Comparar tipos para columnas comunes
        common_cols = cols1 & cols2
        for col in common_cols:
            type1 = schema1.field(col).type
            type2 = schema2.field(col).type
            if type1 != type2:
                diff.append(f"Columna '{col}': {type1} vs {type2}")
        
        return "; ".join(diff) if diff else "Sin diferencias"
    
    def consolidate_global(self, files, compression='snappy', 
                          optimize=True, validate=True):
        """
        Consolida archivos anuales en un archivo global.
        
        Args:
            files (list): Lista de archivos a consolidar
            compression (str): Método de compresión
            optimize (bool): Si True, optimiza tipos de datos
            validate (bool): Si True, valida archivos antes de consolidar
            
        Returns:
            Path: Ruta al archivo consolidado, o None si falla
        """
        if not files:
            logger.error("❌ No se proporcionaron archivos para consolidar")
            return None
        
        logger.info(f"🌍 Iniciando consolidación global de {len(files)} archivos anuales")
        
        # Validar archivos si se solicita
        if validate:
            valid, errors = self.validate_files(files)
            if not valid:
                logger.error("❌ Validación fallida:")
                for error in errors[:5]:  # Mostrar solo primeros 5 errores
                    logger.error(f"   - {error}")
                if len(errors) > 5:
                    logger.error(f"   ... y {len(errors) - 5} errores más")
                
                # Preguntar si continuar
                if not self._ask_continue():
                    logger.warning("Consolidación cancelada por el usuario")
                    return None
                else:
                    logger.warning("⚠️  Continuando a pesar de errores de validación...")
        
        # Leer y consolidar archivos
        logger.info("📖 Leyendo archivos anuales...")
        
        total_rows = 0
        total_size_mb = 0
        tables = []
        year_stats = []
        
        # Leer con progreso
        for file_path in tqdm(files, desc="Leyendo archivos anuales"):
            try:
                # Leer archivo Parquet
                table = pq.read_table(file_path, memory_map=True)
                num_rows = len(table)
                file_size_mb = file_path.stat().st_size / (1024 ** 2)
                
                # Extraer año del nombre del archivo
                year = self._extract_year_from_filename(file_path.name)
                
                # Estadísticas del año
                year_stats.append({
                    'year': year,
                    'file': file_path.name,
                    'rows': num_rows,
                    'size_mb': file_size_mb,
                    'start_date': None,
                    'end_date': None
                })
                
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
        logger.info("🔗 Concatenando tablas anuales...")
        start_time = datetime.now()
        
        try:
            consolidated_table = pa.concat_tables(tables, promote=True)
        except Exception as e:
            logger.error(f"❌ Error concatenando tablas: {str(e)}")
            logger.info("Intentando con pandas como alternativa...")
            
            # Fallback a pandas
            return self._consolidate_with_pandas(files, compression)
        
        # Optimizar esquema si se solicita
        if optimize:
            logger.info("⚙️  Optimizando esquema global...")
            consolidated_table = self._optimize_schema(consolidated_table)
        
        # Guardar archivo global
        logger.info(f"💾 Guardando archivo global: {self.output_file}")
        
        try:
            pq.write_table(
                consolidated_table,
                self.output_file,
                compression=compression,
                row_group_size=100000,
                write_statistics=True,
                data_page_version='2.0',
                use_dictionary=True,
                compression_level=None if compression == 'snappy' else 9
            )
        except Exception as e:
            logger.error(f"❌ Error guardando archivo global: {str(e)}")
            return None
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # Obtener estadísticas del archivo resultante
        output_size_mb = self.output_file.stat().st_size / (1024 ** 2)
        compression_ratio = (1 - output_size_mb / total_size_mb) * 100 if total_size_mb > 0 else 0
        
        # Extraer símbolo del primer archivo
        symbol = self._extract_symbol_from_filename(files[0].name) if files else "UNKNOWN"
        
        # Obtener rango de fechas si es posible
        date_range = self._get_date_range(consolidated_table)
        
        # Mostrar resumen detallado
        self._display_summary(
            files=files,
            consolidated_table=consolidated_table,
            year_stats=year_stats,
            symbol=symbol,
            output_size_mb=output_size_mb,
            total_size_mb=total_size_mb,
            compression_ratio=compression_ratio,
            compression=compression,
            elapsed=elapsed,
            date_range=date_range
        )
        
        # Guardar metadatos
        self._save_metadata(year_stats, symbol, date_range)
        
        return self.output_file
    
    def _consolidate_with_pandas(self, files, compression):
        """Consolidación usando pandas como fallback"""
        logger.info("🔄 Usando pandas para consolidación global...")
        
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
        try:
            consolidated_df.to_parquet(
                self.output_file,
                engine='pyarrow',
                compression=compression,
                index=False
            )
            logger.info(f"✅ Archivo global guardado con pandas: {self.output_file}")
            return self.output_file
        except Exception as e:
            logger.error(f"❌ Error guardando con pandas: {str(e)}")
            return None
    
    def _optimize_schema(self, table):
        """Optimiza los tipos de datos para ahorrar espacio."""
        logger.debug("Optimizando tipos de datos globales...")
        
        optimized_fields = []
        
        for field in table.schema:
            field_name = field.name
            field_type = field.type
            
            # Determinar tipo óptimo
            new_type = self._get_optimal_global_type(field_name, field_type, table.column(field_name))
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
                    logger.warning(f"  No se pudo convertir columna '{field.name}'")
            columns.append(col)
        
        return pa.Table.from_arrays(columns, schema=optimized_schema)
    
    def _get_optimal_global_type(self, column_name, current_type, column_data):
        """Determina el tipo óptimo para una columna en el dataset global."""
        
        # Para columnas de precios (siempre float32 para datos financieros)
        price_columns = ['bid', 'ask', 'open', 'high', 'low', 'close', 'spread', 'mid']
        if column_name in price_columns and pa.types.is_floating(current_type):
            return pa.float32()
        
        # Para volumen (int32 si son enteros, float32 si no)
        if column_name == 'volume' and pa.types.is_floating(current_type):
            # Verificar muestra para ver si todos son enteros
            try:
                sample_size = min(10000, len(column_data))
                if sample_size > 0:
                    sample = column_data.slice(0, sample_size).to_pandas()
                    # Verificar si todos los valores no-nulos son enteros
                    non_null = sample.dropna()
                    if len(non_null) > 0 and (non_null % 1 == 0).all():
                        return pa.int32()
                    else:
                        return pa.float32()
            except:
                pass
        
        # Para timestamps (usar microsegundos es suficiente)
        if column_name == 'timestamp' and pa.types.is_timestamp(current_type):
            if current_type.unit == 'ns':
                return pa.timestamp('us')  # Microsegundos es suficiente
        
        return current_type
    
    def _get_date_range(self, table):
        """Obtiene el rango de fechas del dataset consolidado."""
        if 'timestamp' not in table.column_names:
            return None
        
        try:
            ts_col = table.column('timestamp')
            if pa.types.is_timestamp(ts_col.type):
                # Convertir a pandas para obtener min/max fácilmente
                ts_series = ts_col.to_pandas()
                return {
                    'start': ts_series.min(),
                    'end': ts_series.max(),
                    'days': (ts_series.max() - ts_series.min()).days,
                    'years': (ts_series.max() - ts_series.min()).days / 365.25
                }
        except Exception as e:
            logger.warning(f"⚠️  No se pudo obtener rango de fechas: {e}")
        
        return None
    
    def _display_summary(self, **kwargs):
        """Muestra un resumen detallado de la consolidación."""
        logger.info("\n" + "=" * 70)
        logger.info("🌍 CONSOLIDACIÓN GLOBAL COMPLETADA EXITOSAMENTE")
        logger.info("=" * 70)
        
        # Información básica
        logger.info(f"📊 ESTADÍSTICAS GLOBALES:")
        logger.info(f"   • Archivos consolidados: {len(kwargs['files'])}")
        logger.info(f"   • Símbolo: {kwargs['symbol']}")
        logger.info(f"   • Filas totales: {len(kwargs['consolidated_table']):,}")
        logger.info(f"   • Columnas: {kwargs['consolidated_table'].num_columns}")
        
        # Rango de fechas
        if kwargs['date_range']:
            logger.info(f"   • Rango temporal: {kwargs['date_range']['start']} a {kwargs['date_range']['end']}")
            logger.info(f"   • Período cubierto: {kwargs['date_range']['days']} días ({kwargs['date_range']['years']:.1f} años)")
        
        # Tamaños y compresión
        logger.info(f"   • Tamaño original total: {kwargs['total_size_mb']:.1f} MB")
        logger.info(f"   • Tamaño consolidado: {kwargs['output_size_mb']:.1f} MB")
        logger.info(f"   • Ratio de compresión: {kwargs['compression_ratio']:.1f}%")
        logger.info(f"   • Compresión utilizada: {kwargs['compression']}")
        
        # Tiempo y rendimiento
        logger.info(f"   • Tiempo de procesamiento: {kwargs['elapsed']:.1f} segundos")
        if kwargs['elapsed'] > 0:
            rows_per_second = len(kwargs['consolidated_table']) / kwargs['elapsed']
            mb_per_second = kwargs['total_size_mb'] / kwargs['elapsed']
            logger.info(f"   • Rendimiento: {rows_per_second:,.0f} filas/seg, {mb_per_second:.1f} MB/seg")
        
        logger.info(f"   • Archivo de salida: {self.output_file}")
        
        # Desglose por año
        logger.info("\n📅 DESGLOSE POR AÑO:")
        for stat in sorted(kwargs['year_stats'], key=lambda x: x['year'] or 0):
            if stat['year']:
                logger.info(f"   • {stat['year']}: {stat['rows']:,} filas, {stat['size_mb']:.1f} MB")
        
        logger.info("=" * 70)
    
    def _save_metadata(self, year_stats, symbol, date_range):
        """Guarda metadatos de la consolidación."""
        metadata = {
            'symbol': symbol,
            'consolidation_date': datetime.now().isoformat(),
            'total_files': len(year_stats),
            'total_rows': sum(stat['rows'] for stat in year_stats),
            'total_size_mb': sum(stat['size_mb'] for stat in year_stats),
            'date_range': date_range,
            'yearly_stats': year_stats,
            'output_file': str(self.output_file),
            'output_size_mb': self.output_file.stat().st_size / (1024 ** 2) if self.output_file.exists() else 0
        }
        
        # Guardar como JSON
        metadata_file = self.output_file.with_suffix('.metadata.json')
        try:
            import json
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            logger.info(f"📋 Metadatos guardados en: {metadata_file}")
        except Exception as e:
            logger.warning(f"⚠️  No se pudieron guardar metadatos: {e}")
    
    def _ask_continue(self):
        """Pregunta al usuario si desea continuar."""
        response = input("\n⚠️  ¿Desea continuar a pesar de los errores? (s/N): ")
        return response.lower() == 's'


def parse_year_range(year_range_str):
    """Parsea un rango de años en formato '2010-2024'."""
    if '-' in year_range_str:
        start_year, end_year = map(int, year_range_str.split('-'))
        return list(range(start_year, end_year + 1))
    else:
        # Lista separada por comas
        return [int(y.strip()) for y in year_range_str.split(',')]


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(
        description='Consolida archivos anuales en un archivo global',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Consolidar todos los archivos anuales de XAUUSD
  python %(prog)s --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet
  
  # Consolidar años específicos
  python %(prog)s --input-dir consolidated --output-file XAUUSD_2010-2015.parquet --years 2010-2015
  
  # Consolidar con compresión GZIP
  python %(prog)s --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet --compression gzip
  
  # Consolidar múltiples símbolos
  python %(prog)s --input-dir consolidated --output-file ALL_GLOBAL.parquet --symbol ALL
  
  # Consolidar sin validación
  python %(prog)s --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet --skip-validation
        """
    )
    
    # Argumentos principales
    parser.add_argument('--input-dir', '-i', required=True,
                       help='Directorio con archivos anuales consolidados')
    parser.add_argument('--output-file', '-o', required=True,
                       help='Archivo de salida global (ej: XAUUSD_GLOBAL.parquet)')
    
    # Filtros
    parser.add_argument('--symbol', default=None,
                       help='Símbolo específico (ej: XAUUSD). Use "ALL" para todos')
    parser.add_argument('--years', default=None,
                       help='Rango de años (ej: 2010-2024) o lista separada por comas')
    
    # Opciones de procesamiento
    parser.add_argument('--compression', default='snappy',
                       choices=['snappy', 'gzip', 'lz4', 'zstd'],
                       help='Método de compresión (default: snappy)')
    parser.add_argument('--skip-optimization', action='store_true',
                       help='Saltar optimización de tipos de datos')
    parser.add_argument('--skip-validation', action='store_true',
                       help='Saltar validación de archivos')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Modo verboso con más detalles')
    
    args = parser.parse_args()
    
    # Configurar nivel de logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Parsear años si se especifican
    years = None
    if args.years:
        years = parse_year_range(args.years)
    
    # Determinar símbolo
    symbol = args.symbol
    if symbol == "ALL":
        symbol = None
    
    logger.info("=" * 70)
    logger.info("🌍 CONSOLIDADOR GLOBAL DE ARCHIVOS ANUALES")
    logger.info("=" * 70)
    logger.info(f"📂 Entrada: {args.input_dir}")
    logger.info(f"📂 Salida: {args.output_file}")
    logger.info(f"📊 Símbolo: {symbol if symbol else 'TODOS'}")
    logger.info(f"📅 Años: {years if years else 'TODOS'}")
    logger.info(f"⚙️  Compresión: {args.compression}")
    logger.info(f"🔧 Optimización: {'NO' if args.skip_optimization else 'SÍ'}")
    logger.info(f"🔍 Validación: {'NO' if args.skip_validation else 'SÍ'}")
    logger.info("=" * 70)
    
    # Inicializar consolidador
    consolidator = GlobalConsolidator(args.input_dir, args.output_file)
    
    # Buscar archivos
    files = consolidator.find_yearly_files(symbol=symbol, years=years)
    
    if not files:
        logger.error("❌ No se encontraron archivos anuales para consolidar")
        logger.info("   Patrones buscados:")
        logger.info("     - *_FULL.parquet")
        logger.info("     - *_[0-9][0-9][0-9][0-9].parquet")
        logger.info(f"   En directorio: {args.input_dir}")
        sys.exit(1)
    
    logger.info(f"📁 Encontrados {len(files)} archivos anuales:")
    for file in files[:10]:  # Mostrar primeros 10
        logger.info(f"   • {file.name}")
    if len(files) > 10:
        logger.info(f"   ... y {len(files) - 10} archivos más")
    
    # Consolidar
    output_file = consolidator.consolidate_global(
        files=files,
        compression=args.compression,
        optimize=not args.skip_optimization,
        validate=not args.skip_validation
    )
    
    if output_file:
        logger.info(f"🎉 ¡Consolidación global completada exitosamente!")
        logger.info(f"📁 Archivo creado: {output_file}")
        
        # Mostrar tamaño final
        if output_file.exists():
            size_mb = output_file.stat().st_size / (1024 ** 2)
            logger.info(f"📈 Tamaño final: {size_mb:.1f} MB")
        
        sys.exit(0)
    else:
        logger.error("❌ Consolidación global falló")
        sys.exit(1)


if __name__ == '__main__':
    main()
    