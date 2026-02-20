#!/usr/bin/env python3

import subprocess
import time
import random
import argparse
from datetime import datetime, timedelta
import sys
import calendar
import os
import json

from config_logging import get_logger

logger = get_logger(__name__)

# Archivo para guardar el progreso
PROGRESS_FILE = "download_progress.json"

def load_progress(symbol, year):
    """Carga el progreso previo desde archivo"""
    if not os.path.exists(PROGRESS_FILE):
        return {}
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            progress_data = json.load(f)
        
        key = f"{symbol}_{year}"
        if key in progress_data:
            logger.info(f"📂 Progreso anterior encontrado para {symbol} {year}")
            return progress_data[key]
        else:
            return {}
    except Exception as e:
        logger.warning(f"⚠️  Error cargando progreso: {str(e)}")
        return {}

def save_progress(symbol, year, progress_data):
    """Guarda el progreso actual en archivo"""
    try:
        # Cargar progreso existente
        existing_data = {}
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                existing_data = json.load(f)
        
        # Actualizar con nuevo progreso
        key = f"{symbol}_{year}"
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

def run_subprocess_with_realtime_output(cmd, logger, month_name):
    """
    Ejecuta un subproceso mostrando su salida en tiempo real con barra de progreso
    """
    logger.info(f"Ejecutando comando: {' '.join(cmd)}")
    
    try:
        # Inicializar contadores para progreso
        lines_processed = 0
        start_time = time.time()
        
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
            # Leer stdout
            output = process.stdout.readline()
            if output:
                cleaned_output = output.strip()
                if cleaned_output:
                    # Mostrar con formato de progreso
                    lines_processed += 1
                    elapsed_time = time.time() - start_time
                    
                    # Actualizar cada 10 líneas para no saturar la consola
                    if lines_processed % 10 == 0:
                        logger.info(f"📥 {month_name}: {lines_processed} líneas procesadas ({elapsed_time:.1f}s)")
            
            # Leer stderr
            error = process.stderr.readline()
            if error:
                cleaned_error = error.strip()
                if cleaned_error:
                    logger.warning(f"⚠️  {month_name} - Error: {cleaned_error}")
            
            # Verificar si el proceso terminó
            if process.poll() is not None:
                # Leer cualquier salida restante
                for output in process.stdout.readlines():
                    if output.strip():
                        logger.info(f"📥 {month_name} - Final: {output.strip()}")
                for error in process.stderr.readlines():
                    if error.strip():
                        logger.warning(f"⚠️  {month_name} - Error final: {error.strip()}")
                break
        
        # Obtener código de retorno
        return_code = process.wait()
        total_time = time.time() - start_time
        
        if return_code == 0:
            logger.info(f"✅ {month_name} completado en {total_time:.1f}s ({lines_processed} líneas)")
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
    parser.add_argument('--sleep-min', type=int, default=45, help='Min sleep between months (seconds)')
    parser.add_argument('--sleep-max', type=int, default=120, help='Max sleep between months (seconds)')
    parser.add_argument('--timeframe', type=str, default='1min', help='Timeframe (1min, 5min, etc)')
    parser.add_argument('--resume', action='store_true', help='Retomar descarga desde el último progreso guardado')
    args = parser.parse_args()

    symbol = args.symbol
    year = args.year
    
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
        progress = load_progress(symbol, year)
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
        output_file = f"data/{symbol}_{year}_{month:02d}.csv"
        
        logger.info("─" * 50)
        logger.info(f"📅 PROCESANDO MES {i}/{total_months}: {month_name} {year}")
        logger.info(f"   📅 Rango: {start_date} → {end_date}")
        logger.info(f"   💾 Archivo: {output_file}")
        
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
        
        # Ejecutar proceso con salida en tiempo real
        success, error_message = run_subprocess_with_realtime_output(cmd, logger, month_name)
        
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
                
                # Actualizar listas
                successful_months.append(month)
                progress_data['completed_months'].append(month)
                
                # Guardar progreso inmediatamente después de cada mes exitoso
                save_progress(symbol, year, progress_data)
                logger.info(f"   💾 Progreso guardado: {len(progress_data['completed_months'])}/{len(all_months)} meses")
            else:
                logger.error(f"   ❌ ERROR: Archivo no creado: {output_file}")
                failed_months.append((month, "Archivo no creado"))
        else:
            logger.error(f"   ❌ FALLO {month_name} después de {elapsed_str}")
            logger.error(f"   🐛 Error: {error_message}")
            failed_months.append((month, error_message))
        
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
    
    all_successful = completed_months + successful_months
    if all_successful:
        months_str = ", ".join([calendar.month_name[m] for m in sorted(all_successful)])
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
    
    # # Con más workers y timeframe diferente
    # python download_batch_year.py --symbol XAUUSD --year 2010 --workers 8 --timeframe 5min --resume
    
    # # Con pausas personalizadas
    # python download_batch_year.py --symbol XAUUSD --year 2010 --sleep-min 60 --sleep-max 180 --resume
    
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
