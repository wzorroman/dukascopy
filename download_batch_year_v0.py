#!/usr/bin/env python3
import subprocess
import time
import random
import argparse
import logging
from datetime import datetime, timedelta
import sys
import calendar

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

def main():
    parser = argparse.ArgumentParser(description="Bulk download Dukascopy data by months")
    parser.add_argument('--symbol', type=str, default='XAUUSD', help='Currency pair')
    parser.add_argument('--year', type=int, default=2010, help='Year to download')
    parser.add_argument('--start-month', type=int, default=1, help='Start month (1-12)')
    parser.add_argument('--end-month', type=int, default=12, help='End month (1-12)')
    args = parser.parse_args()

    symbol = args.symbol
    
    months = range(args.start_month, args.end_month + 1)
    total_months = len(months)
    
    print(f"\n{'='*60}")
    print(f"📥 DESCARGA MASIVA DE DATOS DUKASCOPY")
    print(f"   Símbolo: {symbol}")
    print(f"   Año: {args.year}")
    print(f"   Meses: {args.start_month} a {args.end_month}")
    print(f"{'='*60}\n")
    
    for i, month in enumerate(months, 1):
        # Obtener fechas del mes
        start_date, end_date = get_month_range(args.year, month)
        month_name = calendar.month_name[month]
        output_file = f"data/{symbol}_{args.year}_{month:02d}.csv"
        
        print(f"\n📅 MES {i}/{total_months}: {month_name} {args.year}")
        print(f"   📅 Rango: {start_date} → {end_date}")
        print(f"   💾 Salida: {output_file}")
        print(f"   {'─'*40}")
        
        cmd = [
            "python", "download_dukascopy.py",
            "--symbol", symbol,
            "--start", start_date,
            "--end", end_date,
            "--output", output_file,
            "--timeframe", "1min",
            "--workers", "8"
        ]
        
        try:
            # Ejecutar con barra de progreso visible
            print("   ⬇️  Descargando...")
            result = subprocess.run(
                cmd, 
                check=True, 
                text=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            
            # Mostrar estadísticas de la descarga
            for line in result.stdout.split('\n'):
                if "Descargando" in line or "Downloading" in line:
                    print(f"   📊 {line}")
            
            print(f"   ✅ {month_name} completado!")
            
        except subprocess.CalledProcessError as e:
            print(f"   ❌ ERROR en {month_name}: {e}")
            if e.stderr:
                print(f"   🔍 Error details: {e.stderr[:100]}")
        
        # Barra de progreso anual
        progress = i / total_months * 100
        filled = int(30 * i / total_months)
        bar = '█' * filled + '░' * (30 - filled)
        print(f"\n   📈 Progreso anual: [{bar}] {progress:.1f}% ({i}/{total_months})")
        
        # Pausa entre meses (aleatoria)
        if i < total_months:
            sleep_time = random.randint(45, 120)  # 45-120 segundos
            print(f"   😴 Pausa de {sleep_time}s...\n")
            time.sleep(sleep_time)
    
    print(f"\n{'='*60}")
    print("🎉 ¡DESCARGA COMPLETADA!")
    print(f"   Archivos guardados en: data/{symbol}_{args.year}_*.csv")
    print(f"{'='*60}")

if __name__ == "__main__":
    # # Descargar todo un año
    # python download_batch_year.py --symbol XAUUSD --year 2010

    # # Descargar meses específicos
    # python download_batch_year.py --symbol XAUUSD --year 2010 --start-month 6 --end-month 12

    # # Con pausas personalizadas
    # python download_batch_year.py --symbol XAUUSD --year 2010 --sleep-min 60 --sleep-max 180
    main()