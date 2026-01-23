#!/usr/bin/env python3
from config_logging import get_logger
import subprocess
import time
import random
import argparse
from datetime import datetime, timedelta
import sys
import calendar
import os
import json

logger = get_logger(__name__)

# Archivo para guardar el progreso
PROGRESS_FILE = "download_progress.json"

def load_progress(symbol, year, output_dir='data'):
    """Carga el progreso previo desde archivo"""
    if not os.path.exists(PROGRESS_FILE):
        return {}
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            progress_data = json.load(f)
        
        # key = f"{symbol}_{year}"
        key = f"{symbol}_{year}_{output_dir}"
        if key in progress_data:
            logger.info(f"📂 Progreso anterior encontrado para {symbol} {year}")
            return progress_data[key]
        else:
            return {}
    except Exception as e:
        logger.warning(f"⚠️  Error cargando progreso: {str(e)}")
        return {}

def save_progress(symbol, year, progress_data, output_dir='data'):
    """Guarda el progreso actual en archivo"""
    try:
        # Cargar progreso existente
        existing_data = {}
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                existing_data = json.load(f)
        
        # Actualizar con nuevo progreso
        # key = f"{symbol}_{year}"
        key = f"{symbol}_{year}_{output_dir}"
        existing_data[key] = progress_data
        
        # Guardar
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(existing_data, f, indent=2)
        
        logger.debug(f"💾 Progreso guardado para {symbol} {year}")
        return True
    except Exception as e:
        logger.error(f"❌ Error guardando progreso: {str(e)}")
        return False

def get_month_range(year, month):
    """Obtiene el primer y último día del mes"""
    # Primer día del mes
    start_date = datetime(year, month, 1)
    
    # Último día del mes
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def format_file_size(bytes_size):
    """Formatea el tamaño del archivo de forma legible"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"

def parse_tqdm_progress(line):
    """
    Parsea líneas de progreso de tqdm para extraer porcentaje y estadísticas
    Ejemplo: "Downloading:  45%|████▌     | 325/721 [01:06<01:23,  4.72it/s]"
    """
    if 'Downloading:' not in line:
        return None
    
    try:
        # Extraer porcentaje
        if '%|' in line:
            percent_part = line.split('%|')[0]
            percent = float(percent_part.split(':')[-1].strip())
        else:
            percent = 0
        
        # Extraer estadísticas (X/Y [tiempo transcurrido<tiempo restante, velocidad])
        if '[' in line and ']' in line:
            stats_part = line.split('[')[1].split(']')[0]
            # Formato: "01:06<01:23,  4.72it/s"
            parts = stats_part.split(',')
            if len(parts) >= 2:
                time_parts = parts[0].split('<')
                elapsed = time_parts[0].strip()
                remaining = time_parts[1].strip() if len(time_parts) > 1 else "?"
                speed = parts[1].strip()
            else:
                elapsed = remaining = speed = "?"
        else:
            elapsed = remaining = speed = "?"
        
        return {
            'percent': percent,
            'elapsed': elapsed,
            'remaining': remaining,
            'speed': speed
        }
    except Exception:
        return None

def run_subprocess_with_detailed_output_v0(cmd, logger, month_name):
    """
    Ejecuta un subproceso con manejo detallado de salida
    Distingue claramente entre stdout (info) y stderr (errores)
    """
    logger.info(f"Ejecutando comando: {' '.join(cmd)}")
    
    try:
        # Variables para seguimiento
        start_time = time.time()
        last_progress_update = 0
        last_percent = 0
        
        # Ejecutar proceso
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Leer salida en tiempo real
        while True:
            # Leer stdout (información normal)
            stdout_line = process.stdout.readline()
            if stdout_line:
                cleaned_stdout = stdout_line.strip()
                if cleaned_stdout:
                    # Intentar parsear como progreso de tqdm
                    progress_info = parse_tqdm_progress(cleaned_stdout)
                    if progress_info:
                        # Actualizar progreso cada 5% o cada 10 segundos
                        current_time = time.time()
                        if (progress_info['percent'] - last_percent >= 5 or 
                            current_time - last_progress_update >= 10):
                            logger.info(
                                f"📊 {month_name}: {progress_info['percent']:.1f}% - "
                                f"Transcurrido: {progress_info['elapsed']} - "
                                f"Restante: {progress_info['remaining']} - "
                                f"Velocidad: {progress_info['speed']}"
                            )
                            last_progress_update = current_time
                            last_percent = progress_info['percent']
                    else:
                        # Otra información stdout (no progreso)
                        if cleaned_stdout.startswith("✅") or cleaned_stdout.startswith("💾"):
                            logger.info(f"✅ {month_name}: {cleaned_stdout}")
                        elif "ERROR" in cleaned_stdout or "❌" in cleaned_stdout:
                            logger.error(f"❌ {month_name}: {cleaned_stdout}")
                        elif "WARNING" in cleaned_stdout or "⚠️" in cleaned_stdout:
                            logger.warning(f"⚠️  {month_name}: {cleaned_stdout}")
                        elif any(x in cleaned_stdout for x in ["INFO", "Descargando", "Descargados", "Generadas"]):
                            # Info normal del proceso hijo
                            logger.info(f"ℹ️  {month_name}: {cleaned_stdout}")
            
            # Leer stderr (errores reales)
            stderr_line = process.stderr.readline()
            if stderr_line:
                cleaned_stderr = stderr_line.strip()
                if cleaned_stderr:
                    # Estos son errores reales, no solo información
                    logger.error(f"🔥 {month_name} - ERROR REAL: {cleaned_stderr}")
            
            # Verificar si el proceso terminó
            if process.poll() is not None:
                # Leer cualquier salida restante
                for line in process.stdout.readlines():
                    if line.strip():
                        logger.info(f"📄 {month_name} - Final: {line.strip()}")
                for line in process.stderr.readlines():
                    if line.strip():
                        logger.error(f"🔥 {month_name} - Error final: {line.strip()}")
                break
        
        # Obtener código de retorno
        return_code = process.wait()
        total_time = time.time() - start_time
        
        if return_code == 0:
            logger.info(f"✅ {month_name} completado exitosamente en {total_time:.1f}s")
            return True, ""
        else:
            error_msg = f"Proceso falló con código: {return_code}"
            logger.error(f"❌ {month_name} - {error_msg}")
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Error ejecutando subproceso: {str(e)}"
        logger.error(f"❌ {month_name} - {error_msg}")
        return False, error_msg

def run_subprocess_with_detailed_output(cmd, logger, month_name, verbose=False):
    """
    Ejecuta un subproceso con manejo detallado de salida
    Distingue claramente entre stdout (info) y stderr (errores)
    """
    logger.info(f"Ejecutando comando: {' '.join(cmd)}")
    
    # MODO VERBOSE: Mostrar toda la salida directamente
    if verbose:
        logger.info(f"🔧 MODO VERBOSE ACTIVADO para {month_name}")
        logger.info(f"📋 Comando completo: {' '.join(cmd)}")
        logger.info("-" * 50)
        
        try:
            # Ejecutar proceso y mostrar toda la salida en tiempo real
            process = subprocess.Popen(
                cmd,
                stdout=None,  # None = heredar stdout del padre
                stderr=None,  # None = heredar stderr del padre
                text=True
            )
            
            # Esperar a que termine
            return_code = process.wait()
            
            if return_code == 0:
                logger.info(f"✅ {month_name} completado exitosamente (modo verbose)")
                return True, ""
            else:
                error_msg = f"Proceso falló con código: {return_code}"
                logger.error(f"❌ {month_name} - {error_msg}")
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error ejecutando subproceso: {str(e)}"
            logger.error(f"❌ {month_name} - {error_msg}")
            return False, error_msg
    
    # MODO NORMAL (tu código actual)
    try:
        # Variables para seguimiento
        start_time = time.time()
        last_progress_update = 0
        last_percent = 0
        
        # Ejecutar proceso
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Leer salida en tiempo real
        while True:
            # Leer stdout (información normal)
            stdout_line = process.stdout.readline()
            if stdout_line:
                cleaned_stdout = stdout_line.strip()
                if cleaned_stdout:
                    # Intentar parsear como progreso de tqdm
                    progress_info = parse_tqdm_progress(cleaned_stdout)
                    if progress_info:
                        # Actualizar progreso cada 5% o cada 10 segundos
                        current_time = time.time()
                        if (progress_info['percent'] - last_percent >= 5 or 
                            current_time - last_progress_update >= 10):
                            logger.info(
                                f"📊 {month_name}: {progress_info['percent']:.1f}% - "
                                f"Transcurrido: {progress_info['elapsed']} - "
                                f"Restante: {progress_info['remaining']} - "
                                f"Velocidad: {progress_info['speed']}"
                            )
                            last_progress_update = current_time
                            last_percent = progress_info['percent']
                    else:
                        # Otra información stdout (no progreso)
                        if cleaned_stdout.startswith("✅") or cleaned_stdout.startswith("💾"):
                            logger.info(f"✅ {month_name}: {cleaned_stdout}")
                        elif "ERROR" in cleaned_stdout or "❌" in cleaned_stdout:
                            logger.error(f"❌ {month_name}: {cleaned_stdout}")
                        elif "WARNING" in cleaned_stdout or "⚠️" in cleaned_stdout:
                            logger.warning(f"⚠️  {month_name}: {cleaned_stdout}")
                        elif any(x in cleaned_stdout for x in ["INFO", "Descargando", "Descargados", "Generadas"]):
                            # Info normal del proceso hijo
                            logger.info(f"ℹ️  {month_name}: {cleaned_stdout}")
            
            # Leer stderr (errores reales)
            stderr_line = process.stderr.readline()
            if stderr_line:
                cleaned_stderr = stderr_line.strip()
                if cleaned_stderr:
                    # Estos son errores reales, no solo información
                    logger.error(f"🔥 {month_name} - ERROR REAL: {cleaned_stderr}")
            
            # Verificar si el proceso terminó
            if process.poll() is not None:
                # Leer cualquier salida restante
                for line in process.stdout.readlines():
                    if line.strip():
                        logger.info(f"📄 {month_name} - Final: {line.strip()}")
                for line in process.stderr.readlines():
                    if line.strip():
                        logger.error(f"🔥 {month_name} - Error final: {line.strip()}")
                break
        
        # Obtener código de retorno
        return_code = process.wait()
        total_time = time.time() - start_time
        
        if return_code == 0:
            logger.info(f"✅ {month_name} completado exitosamente en {total_time:.1f}s")
            return True, ""
        else:
            error_msg = f"Proceso falló con código: {return_code}"
            logger.error(f"❌ {month_name} - {error_msg}")
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Error ejecutando subproceso: {str(e)}"
        logger.error(f"❌ {month_name} - {error_msg}")
        return False, error_msg
    
def display_progress_bar(current, total, prefix="", length=50):
    """Muestra una barra de progreso en la consola"""
    filled_length = int(length * current // total)
    bar = '█' * filled_length + '░' * (length - filled_length)
    percent = 100.0 * current / total
    
    # Usar sys.stdout.write para evitar saltos de línea innecesarios
    sys.stdout.write(f'\r{prefix} |{bar}| {current}/{total} ({percent:.1f}%)')
    sys.stdout.flush()
    
    if current == total:
        print()  # Nueva línea al final

def main():
    parser = argparse.ArgumentParser(description="Bulk download Dukascopy data by months")
    parser.add_argument('--symbol', type=str, default='XAUUSD', help='Currency pair')
    parser.add_argument('--year', type=int, default=2010, help='Year to download')
    parser.add_argument('--start-month', type=int, default=1, help='Start month (1-12)')
    parser.add_argument('--end-month', type=int, default=12, help='End month (1-12)')
    parser.add_argument('--workers', type=int, default=6, help='Number of workers for download')
    parser.add_argument('--sleep-min', type=int, default=10, help='Min sleep between months (seconds)')
    parser.add_argument('--sleep-max', type=int, default=30, help='Max sleep between months (seconds)')
    parser.add_argument('--timeframe', type=str, default='1min', help='Timeframe (1min, 5min, etc)')
    parser.add_argument('--resume', action='store_true', help='Retomar descarga desde el último progreso guardado')
    parser.add_argument('--verbose', action='store_true', help='Mostrar toda la salida detallada del proceso hijo')
    parser.add_argument('--output-dir', type=str, default='data', 
                       help='Directorio de salida para los archivos (default: data)')
    
    args = parser.parse_args()

    symbol = args.symbol
    year = args.year
    output_dir = args.output_dir
    
    # Validar meses
    if args.start_month < 1 or args.start_month > 12:
        logger.error(f"Mes de inicio inválido: {args.start_month}. Debe ser entre 1-12")
        sys.exit(1)
    
    if args.end_month < 1 or args.end_month > 12:
        logger.error(f"Mes final inválido: {args.end_month}. Debe ser entre 1-12")
        sys.exit(1)
    
    if args.end_month < args.start_month:
        logger.error(f"Mes final ({args.end_month}) no puede ser menor que mes inicial ({args.start_month})")
        sys.exit(1)
    
    # Obtener lista de meses a procesar
    all_months = list(range(args.start_month, args.end_month + 1))
    
    # Cargar progreso previo si se usa --resume
    completed_months = []
    if args.resume:
        progress = load_progress(symbol, year, output_dir)
        completed_months = progress.get('completed_months', [])
        
        if completed_months:
            logger.info(f"📊 Progreso cargado: {len(completed_months)} meses ya completados")
            # Filtrar meses pendientes
            months_to_process = [m for m in all_months if m not in completed_months]
            
            if not months_to_process:
                logger.info("🎯 Todos los meses ya están completados. Nada que procesar.")
                sys.exit(0)
        else:
            months_to_process = all_months
    else:
        months_to_process = all_months
    
    total_months = len(months_to_process)
    
    logger.info("=" * 60)
    logger.info(f"🚀 {'REANUDANDO' if args.resume else 'INICIANDO'} DESCARGA MASIVA DE DATOS")
    logger.info(f"   Símbolo: {symbol}")
    logger.info(f"   Año: {year}")
    logger.info(f"   Meses pendientes: {len(months_to_process)} de {len(all_months)}")
    logger.info(f"   Timeframe: {args.timeframe}")
    logger.info(f"   Workers: {args.workers}")
    logger.info(f"   Pausa entre meses: {args.sleep_min}-{args.sleep_max} segundos")
    logger.info(f"   Modo verbose: {'SI' if args.verbose else 'NO'}")
    logger.info("=" * 60)
    
    successful_months = []
    failed_months = []
    
    # Crear directorio de datos si no existe
    os.makedirs("data", exist_ok=True)
    
    # Inicializar datos de progreso
    progress_data = {
        'symbol': symbol,
        'year': year,
        'completed_months': completed_months.copy(),
        'last_updated': datetime.now().isoformat(),
        'total_months': len(all_months)
    }
    
    for i, month in enumerate(months_to_process, 1):
        # Obtener fechas del mes
        start_date, end_date = get_month_range(year, month)
        month_name = calendar.month_name[month]
        # output_file = f"data/{symbol}_{year}_{month:02d}.csv"
        output_file = f"{output_dir}/{symbol}_{year}_{month:02d}.csv" 
        
        logger.info("─" * 50)
        logger.info(f"📅 PROCESANDO MES {i}/{total_months}: {month_name} {year}")
        logger.info(f"   📅 Rango: {start_date} → {end_date}")
        logger.info(f"   💾 Archivo: {output_file}")
        logger.info(f"   📂 Directorio: {output_dir}")
        
        # Crear directorio de salida si no existe
        os.makedirs(output_dir, exist_ok=True)

        # Verificar si el archivo ya existe
        if os.path.exists(output_file):
            file_size = os.path.getsize(output_file)
            size_str = format_file_size(file_size)
            logger.warning(f"   ⚠️  Archivo ya existe ({size_str}). Sobrescribiendo...")
        
        # Construir comando
        cmd = [
            "python", "download_dukascopy.py",
            "--symbol", symbol,
            "--start", start_date,
            "--end", end_date,
            "--output", output_file,
            "--timeframe", args.timeframe,
            "--workers", str(args.workers)
        ]
        
        # Mostrar barra de progreso general
        display_progress_bar(
            i - 1 + len(completed_months),
            len(all_months),
            prefix="📊 Progreso anual:",
            length=30
        )
        
        # Registrar inicio de descarga
        start_time = time.time()
        logger.info(f"\n   ⬇️  Iniciando descarga a las {datetime.now().strftime('%H:%M:%S')}")
        
        # Ejecutar proceso con manejo mejorado de salida
        success, error_message = run_subprocess_with_detailed_output(cmd, logger, month_name, args.verbose)
        
        # Calcular tiempo transcurrido
        elapsed_time = time.time() - start_time
        elapsed_str = f"{elapsed_time:.1f}s"
        if elapsed_time > 60:
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            elapsed_str = f"{minutes}m {seconds}s"
        
        if success:
            # Verificar que el archivo se creó
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                size_str = format_file_size(file_size)
                logger.info(f"   ✅ {month_name} completado en {elapsed_str}")
                logger.info(f"   📊 Tamaño del archivo: {size_str}")
                
                # Leer estadísticas del archivo si es posible
                try:
                    with open(output_file, 'r') as f:
                        lines = f.readlines()
                        logger.info(f"   📈 Filas de datos: {len(lines) - 1 if lines else 0}")  # -1 para header
                except:
                    pass
                
                # Actualizar listas
                successful_months.append(month)
                progress_data['completed_months'].append(month)
                
                # Guardar progreso inmediatamente después de cada mes exitoso
                save_progress(symbol, year, progress_data, output_dir)
                logger.info(f"   💾 Progreso guardado: {len(progress_data['completed_months'])}/{len(all_months)} meses")
            else:
                logger.error(f"   ❌ ERROR: Archivo no creado: {output_file}")
                failed_months.append((month, "Archivo no creado"))
                
                # Guardar error en progreso
                progress_data.setdefault('errors', []).append({
                    'month': month,
                    'error': "Archivo no creado",
                    'timestamp': datetime.now().isoformat()
                })
                save_progress(symbol, year, progress_data, output_dir)
        else:
            logger.error(f"   ❌ FALLO {month_name} después de {elapsed_str}")
            logger.error(f"   🐛 Error: {error_message}")
            failed_months.append((month, error_message))
            
            # Guardar error en progreso
            progress_data.setdefault('errors', []).append({
                'month': month,
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            })
            save_progress(symbol, year, progress_data)
        
        # Mostrar progreso general actualizado
        display_progress_bar(
            i + len(completed_months),
            len(all_months),
            prefix="📊 Progreso anual:",
            length=30
        )
        
        # Pausa entre meses (excepto después del último)
        if i < total_months:
            sleep_time = random.randint(args.sleep_min, args.sleep_max)
            logger.info(f"\n   😴 Pausando por {sleep_time} segundos...")
            
            # Mostrar cuenta regresiva durante la pausa
            for remaining in range(sleep_time, 0, -1):
                mins, secs = divmod(remaining, 60)
                countdown = f"   ⏳ Reanuda en: {mins:02d}:{secs:02d}"
                sys.stdout.write(f"\r{countdown}")
                sys.stdout.flush()
                time.sleep(1)
            print()  # Nueva línea después de la cuenta regresiva
    
    # Resumen final
    logger.info("\n" + "=" * 60)
    logger.info(f"🎉 DESCARGA COMPLETADA!")
    logger.info(f"📊 RESUMEN FINAL:")
    logger.info(f"   ✅ Meses exitosos: {len(successful_months) + len(completed_months)}/{len(all_months)}")
    
    all_successful = sorted(completed_months + successful_months)
    if all_successful:
        months_str = ", ".join([calendar.month_name[m] for m in all_successful])
        logger.info(f"   📅 Meses procesados: {months_str}")
    
    if failed_months:
        logger.warning(f"   ❌ Meses fallidos: {len(failed_months)}")
        for month, error in failed_months:
            logger.warning(f"      - {calendar.month_name[month]}: {error}")
    
    success_rate = ((len(successful_months) + len(completed_months)) / len(all_months)) * 100
    logger.info(f"   📈 Tasa de éxito: {success_rate:.1f}%")
    
    # Mostrar ubicación de archivos
    if all_successful:
        logger.info(f"   💾 Archivos guardados en: data/{symbol}_{year}_*.csv")
    
    # Mostrar tamaño total de datos descargados
    total_size = 0
    for month in all_successful:
        file_path = f"data/{symbol}_{year}_{month:02d}.csv"
        if os.path.exists(file_path):
            total_size += os.path.getsize(file_path)
    
    if total_size > 0:
        total_size_str = format_file_size(total_size)
        logger.info(f"   💽 Tamaño total de datos: {total_size_str}")
    
    # Eliminar archivo de progreso si todo fue exitoso
    if not failed_months:
        try:
            # Cargar datos existentes
            if os.path.exists(PROGRESS_FILE):
                with open(PROGRESS_FILE, 'r') as f:
                    all_progress = json.load(f)
                
                # Eliminar la clave de este símbolo/año
                key = f"{symbol}_{year}"
                if key in all_progress:
                    del all_progress[key]
                    
                    # Si queda algo, guardar; si no, eliminar el archivo
                    if all_progress:
                        with open(PROGRESS_FILE, 'w') as f:
                            json.dump(all_progress, f, indent=2)
                    else:
                        os.remove(PROGRESS_FILE)
                        
                logger.info(f"   🗑️  Archivo de progreso eliminado para {symbol} {year}")
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo eliminar archivo de progreso: {str(e)}")
    
    logger.info("=" * 60)
    
    # Retornar código de salida
    if failed_months:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    # # Descarga básica
    # python download_batch_year.py --symbol XAUUSD --year 2010
    
    # # Retomar descarga desde el último progreso
    # python download_batch_year.py --symbol XAUUSD --year 2010 --resume
    
    # # Modo verbose para ver toda la salida
    # python download_batch_year.py --symbol XAUUSD --year 2010 --verbose
    
    # # Con más workers y timeframe diferente
    # python download_batch_year.py --symbol XAUUSD --year 2010 --workers 8 --timeframe 5min --resume --verbose
    
    # python download_batch_year.py --symbol XAUUSD --year 2010 --workers 8 --sleep-min 15 --sleep-max 30 --resume --verbose
    
    # Directorio diferente para cada año:
    # python download_batch_year_v3.py --symbol XAUUSD --year 2010 --output-dir data_2010
    # python download_batch_year_v3.py --symbol XAUUSD --year 2011 --output-dir data_2011

    # ** OK FUNCIONA **- continuar la descarga y ver la barra de progreso
    # python3 download_batch_year_v3.py --symbol XAUUSD --year 2010 --workers 8 --sleep-min 15 --sleep-max 30 --resume --verbose
    # python3 download_batch_year_v3.py --symbol XAUUSD --year 2013 --workers 8 ----output-dir data_2013 --resume --verbose
    
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Descarga interrumpida por el usuario")
        logger.warning("💾 El progreso ha sido guardado automáticamente")
        logger.warning("▶️  Para reanudar, ejecuta con la opción --resume")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n❌ Error inesperado: {str(e)}", exc_info=True)
        sys.exit(1)
