#!/usr/bin/env python3
from multiprocessing import get_logger
import subprocess
import time
import random
import argparse
from datetime import datetime, timedelta
import sys
import calendar
import os

logger = get_logger(__name__)

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

def run_subprocess_with_realtime_output(cmd, logger):
    """
    Ejecuta un subproceso mostrando su salida en tiempo real
    """
    logger.info(f"Ejecutando comando: {' '.join(cmd)}")
    
    try:
        # Ejecutar proceso y capturar salida en tiempo real
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )
        
        # Leer salida en tiempo real
        while True:
            # Leer stdout
            output = process.stdout.readline()
            if output:
                # Limpiar y mostrar la línea
                cleaned_output = output.strip()
                if cleaned_output:
                    logger.info(f"Progreso: {cleaned_output}")
            
            # Leer stderr
            error = process.stderr.readline()
            if error:
                cleaned_error = error.strip()
                if cleaned_error:
                    logger.warning(f"Error stream: {cleaned_error}")
            
            # Verificar si el proceso terminó
            if process.poll() is not None:
                # Leer cualquier salida restante
                for output in process.stdout.readlines():
                    if output.strip():
                        logger.info(f"Final: {output.strip()}")
                for error in process.stderr.readlines():
                    if error.strip():
                        logger.warning(f"Error final: {error.strip()}")
                break
        
        # Obtener código de retorno
        return_code = process.wait()
        
        if return_code == 0:
            logger.info("Proceso completado exitosamente")
            return True, ""
        else:
            error_msg = f"Proceso falló con código: {return_code}"
            logger.error(error_msg)
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Error ejecutando subproceso: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

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
    
    months = range(args.start_month, args.end_month + 1)
    total_months = len(months)
    
    logger.info("=" * 60)
    logger.info(f"🚀 INICIANDO DESCARGA MASIVA DE DATOS")
    logger.info(f"   Símbolo: {symbol}")
    logger.info(f"   Año: {year}")
    logger.info(f"   Meses: {args.start_month} a {args.end_month} ({total_months} meses)")
    logger.info(f"   Timeframe: {args.timeframe}")
    logger.info(f"   Workers: {args.workers}")
    logger.info(f"   Pausa entre meses: {args.sleep_min}-{args.sleep_max} segundos")
    logger.info("=" * 60)
    
    successful_months = []
    failed_months = []
    
    # Crear directorio de datos si no existe
    os.makedirs("data", exist_ok=True)
    
    for i, month in enumerate(months, 1):
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
            logger.warning(f"   ⚠️  Archivo ya existe ({file_size:,} bytes). Sobrescribiendo...")
        
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
        
        # Registrar inicio de descarga
        start_time = time.time()
        logger.info(f"   ⬇️  Iniciando descarga a las {datetime.now().strftime('%H:%M:%S')}")
        
        # Ejecutar proceso con salida en tiempo real
        success, error_message = run_subprocess_with_realtime_output(cmd, logger)
        
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
                logger.info(f"   ✅ {month_name} completado en {elapsed_str}")
                logger.info(f"   📊 Tamaño del archivo: {file_size:,} bytes")
                successful_months.append(month)
            else:
                logger.error(f"   ❌ ERROR: Archivo no creado: {output_file}")
                failed_months.append((month, "Archivo no creado"))
        else:
            logger.error(f"   ❌ FALLO {month_name} después de {elapsed_str}")
            logger.error(f"   🐛 Error: {error_message}")
            failed_months.append((month, error_message))
        
        # Mostrar progreso general
        progress_percent = (i / total_months) * 100
        logger.info(f"   📈 Progreso anual: {progress_percent:.1f}% ({i}/{total_months})")
        
        # Pausa entre meses (excepto después del último)
        if i < total_months:
            sleep_time = random.randint(args.sleep_min, args.sleep_max)
            logger.info(f"   😴 Pausando por {sleep_time} segundos...")
            time.sleep(sleep_time)
    
    # Resumen final
    logger.info("=" * 60)
    logger.info(f"🎉 DESCARGA COMPLETADA!")
    logger.info(f"📊 RESUMEN FINAL:")
    logger.info(f"   ✅ Meses exitosos: {len(successful_months)}/{total_months}")
    
    if successful_months:
        months_str = ", ".join([calendar.month_name[m] for m in successful_months])
        logger.info(f"   📅 Meses procesados: {months_str}")
    
    if failed_months:
        logger.warning(f"   ❌ Meses fallidos: {len(failed_months)}")
        for month, error in failed_months:
            logger.warning(f"      - {calendar.month_name[month]}: {error}")
    
    success_rate = (len(successful_months) / total_months) * 100
    logger.info(f"   📈 Tasa de éxito: {success_rate:.1f}%")
    
    # Mostrar ubicación de archivos
    if successful_months:
        logger.info(f"   💾 Archivos guardados en: data/{symbol}_{year}_*.csv")
    
    logger.info("=" * 60)
    
    # Retornar código de salida
    if failed_months:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":

    # # Descarga básica
    # python download_batch_year.py --symbol XAUUSD --year 2010

    # # Con más workers y timeframe diferente
    # python download_batch_year.py --symbol XAUUSD --year 2010 --workers 8 --timeframe 5min

    # # Con pausas personalizadas
    # python download_batch_year.py --symbol XAUUSD --year 2010 --sleep-min 60 --sleep-max 180
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Descarga interrumpida por el usuario")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Error inesperado: {str(e)}", exc_info=True)
        sys.exit(1)
