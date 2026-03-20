#!/usr/bin/env python3
"""
================================================================================
🕵️ DUKASCOPY SEARCH SYMBOLS v1.0
================================================================================
Busca símbolos disponibles en Dukascopy y verifica disponibilidad por año
================================================================================
"""

from pathlib import Path

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
import os

# Configuración
BASE_URL = "https://datafeed.dukascopy.com/datafeed/{symbol}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
BASE_DIR = Path("dukascopy_data")
BASE_DIR.mkdir(exist_ok=True)

# Configuración de rate limiting
MIN_DELAY = 0.5
MAX_DELAY = 3.0
BATCH_DELAY = 2.0
MAX_RETRIES = 3

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(BASE_DIR / f"search_symbols_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# LISTA AMPLIA DE SÍMBOLOS PARA BUSCAR
# ============================================

# Metales preciosos y industriales
METALS = [
    "XAUUSD",  # Oro
    "XAGUSD",  # Plata
    "XPTUSD",  # Platino
    "XPDUSD",  # Paladio
    "XCUUSD",  # Cobre
    "XALUSD",  # Aluminio
    "XNIUSD",  # Níquel
    "XZNUSD",  # Zinc
    "XPBUSD",  # Plomo
    "XSNUSD",  # Estaño
]

# Pares de Forex mayores
FOREX_MAJORS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", 
    "USDCAD", "AUDUSD", "NZDUSD"
]

# Pares de Forex cruzados
FOREX_CROSSES = [
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD",
    "AUDJPY", "AUDCHF", "AUDCAD", "AUDNZD",
    "NZDJPY", "NZDCHF", "NZDCAD",
    "CADJPY", "CADCHF",
    "CHFJPY"
]

# Índices (CFDs)
INDICES = [
    "US30", "SPX500", "NAS100", "US2000",  # USA
    "GER30", "UK100", "FRA40", "EU50",     # Europa
    "JPN225", "AUS200", "HKG50",           # Asia-Pacífico
    "ESP35", "SWI20", "NETH25"              # Otros
]

# Commodities (materias primas)
COMMODITIES = [
    "UKOIL", "USOIL",                       # Petróleo
    "NGAS",                                  # Gas Natural
    "WHEAT", "CORN", "SOYBEAN",              # Granos
    "COFFEE", "COCOA", "SUGAR",              # Softs
    "LCO", "LGO"                             # Otros
]

# Criptomonedas (algunas pueden no estar disponibles)
CRYPTO = [
    "BTCUSD", "ETHUSD", "LTCUSD", "XRPUSD",
    "BCHUSD", "ADAUSD", "DOTUSD", "LINKUSD"
]

# Bonos y tasas
BONDS = [
    "US10Y", "US30Y", "UK10Y", "GER10Y",
    "JGB10Y", "AUD10Y"
]

# Energía
ENERGY = [
    "BRENT", "WTI", "HEATOIL", "GASOLINE"
]

# Unir todas las listas
ALL_SYMBOLS = (
    METALS + FOREX_MAJORS + FOREX_CROSSES + 
    INDICES + COMMODITIES + CRYPTO + 
    BONDS + ENERGY
)

# Eliminar duplicados manteniendo orden
ALL_SYMBOLS = list(dict.fromkeys(ALL_SYMBOLS))

# ============================================
# FUNCIONES DE VERIFICACIÓN
# ============================================

def get_test_dates(year: int) -> List[Tuple[int, int, int]]:
    """
    Genera fechas de prueba para un año específico
    """
    # Fechas clave para probar (días laborables típicos)
    test_dates = [
        (1, 8),   # 8 de enero (evitar festivos)
        (3, 15),  # 15 de marzo
        (6, 10),  # 10 de junio
        (9, 12),  # 12 de septiembre
        (11, 5),  # 5 de noviembre
    ]
    
    return [(year, month, day) for month, day in test_dates]

def check_symbol_availability_old(symbol: str, year: int, test_hour: int = 12) -> Dict:
    """
    Verifica disponibilidad de un símbolo para un año específico
    """
    test_dates = get_test_dates(year)
    available_dates = []
    
    for year, month, day in test_dates:
        # Construir URL
        url = BASE_URL.format(
            symbol=symbol,
            year=year,
            month=month,
            day=day,
            hour=test_hour
        )
        
        try:
            # Usar HEAD request para verificar existencia
            response = requests.head(url, timeout=10, allow_redirects=True)
            
            if response.status_code == 200:
                available_dates.append(f"{year}-{month:02d}-{day:02d}")
                logger.debug(f"   ✅ {symbol} - {year}-{month:02d}-{day:02d}")
                break  # Si encontramos un día disponible, pasamos al siguiente año
            elif response.status_code == 404:
                logger.debug(f"   ❌ {symbol} - {year}-{month:02d}-{day:02d} (404)")
            else:
                logger.debug(f"   ⚠️ {symbol} - {year}-{month:02d}-{day:02d} ({response.status_code})")
                
        except requests.exceptions.Timeout:
            logger.debug(f"   ⏱️ Timeout: {symbol} - {year}-{month:02d}-{day:02d}")
        except requests.exceptions.ConnectionError:
            logger.debug(f"   🔌 Connection Error: {symbol} - {year}-{month:02d}-{day:02d}")
        except Exception as e:
            logger.debug(f"   ❗ Error: {symbol} - {year}-{month:02d}-{day:02d} - {str(e)[:50]}")
        
        # Pequeña pausa entre requests
        time.sleep(random.uniform(0.5, 1.5))
    
    return {
        "symbol": symbol,
        "year": year,
        "available": len(available_dates) > 0,
        "sample_date": available_dates[0] if available_dates else None,
        "url_checked": url if available_dates else None
    }

def check_symbol_availability(symbol: str, year: int, test_hour: int = 12, retry_count: int = 0) -> Dict:
    """
    Verifica disponibilidad de un símbolo para un año específico con reintentos
    """
    test_dates = get_test_dates(year)
    available_dates = []
    
    for test_year, month, day in test_dates:
        # Delay variable entre intentos de diferentes fechas
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        url = BASE_URL.format(
            symbol=symbol,
            year=test_year,
            month=month,
            day=day,
            hour=test_hour
        )
        
        try:
            response = requests.head(url, timeout=10, allow_redirects=True)
            
            if response.status_code == 200:
                available_dates.append(f"{year}-{month:02d}-{day:02d}")
                break
            elif response.status_code == 429:  # Too Many Requests
                if retry_count < MAX_RETRIES:
                    wait_time = (2 ** retry_count) * 5  # Exponential backoff
                    logger.warning(f"⚠️ Rate limit detectado, esperando {wait_time}s...")
                    time.sleep(wait_time)
                    return check_symbol_availability(symbol, year, test_hour, retry_count + 1)
                    
        except requests.exceptions.RequestException as e:
            logger.debug(f"Error en request: {e}")
            
    return {
        "symbol": symbol,
        "year": year,
        "available": len(available_dates) > 0,
        "sample_date": available_dates[0] if available_dates else None
    }
    
def batch_check_symbols(symbols: List[str], years: List[int], max_workers: int = 5) -> pd.DataFrame:
    """
    Verifica múltiples símbolos en paralelo
    """
    results = []
    total_checks = len(symbols) * len(years)
    completed = 0
    
    logger.info(f"🚀 Iniciando verificación de {len(symbols)} símbolos para {len(years)} años")
    logger.info(f"📊 Total de verificaciones: {total_checks}")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for symbol in symbols:
            for year in years:
                futures.append(executor.submit(check_symbol_availability, symbol, year))
        
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            results.append(result)
            
            if completed % 10 == 0:
                logger.info(f"📈 Progreso: {completed}/{total_checks} verificaciones completadas")

            if completed % 5 == 0:  # Cada 5 verificaciones
                pause = random.uniform(2, 4)
                logger.debug(f"⏸️ Pausa de {pause:.1f}s para evitar rate limit")
                time.sleep(pause)

    # Convertir a DataFrame
    df = pd.DataFrame(results)
    
    # Pivotear para formato año por columna
    pivot_df = df.pivot(index='symbol', columns='year', values='available')
    pivot_df = pivot_df.fillna(False)
    
    # Añadir columna de disponibilidad general
    pivot_df['total_available_years'] = pivot_df.sum(axis=1)
    pivot_df['availability_percentage'] = (pivot_df.sum(axis=1) / len(years)) * 100
    
    # Añadir información de muestra
    sample_dates = df[df['available']].groupby('symbol')['sample_date'].first()
    pivot_df['sample_date'] = sample_dates
    
    # Ordenar por disponibilidad total
    pivot_df = pivot_df.sort_values('total_available_years', ascending=False)
    
    return pivot_df

def generate_report(df: pd.DataFrame, years: List[int], filename: str = "dukascopy_symbols_report.csv"):
    """
    Genera reporte detallado en CSV
    """
    # Crear columnas de años como strings
    year_columns = [str(year) for year in years]
    
    # Preparar DataFrame para exportar
    export_df = df.copy()
    
    # Convertir booleanos a emojis para mejor visualización
    for year in year_columns:
        if year in export_df.columns:
            export_df[year] = export_df[year].map({True: '✅', False: '❌'})
    
    # Guardar CSV
    export_df.to_csv(filename)
    logger.info(f"💾 Reporte guardado: {filename}")
    
    return export_df

def main():
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║     🕵️ DUKASCOPY SEARCH SYMBOLS v1.0                           ║
    ║     Busca símbolos disponibles en Dukascopy (2019-2025)        ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    # Años a verificar
    start_year = 2019
    end_year = 2025
    years = list(range(start_year, end_year + 1))
    
    logger.info(f"📅 Período de verificación: {start_year} - {end_year}")
    logger.info(f"🔍 Símbolos a verificar: {len(ALL_SYMBOLS)}")
    
    # Mostrar primeras categorías
    print("\n📊 Categorías de símbolos:")
    print(f"   Metales: {len(METALS)}")
    print(f"   Forex Majors: {len(FOREX_MAJORS)}")
    print(f"   Forex Crosses: {len(FOREX_CROSSES)}")
    print(f"   Índices: {len(INDICES)}")
    print(f"   Commodities: {len(COMMODITIES)}")
    print(f"   Crypto: {len(CRYPTO)}")
    print(f"   Bonos: {len(BONDS)}")
    print(f"   Energía: {len(ENERGY)}")
    
    # Confirmar
    respuesta = input("\n¿Comenzar búsqueda? (s/n): ").lower()
    if respuesta != 's':
        logger.info("❌ Búsqueda cancelada")
        return
    
    # Realizar verificación
    start_time = datetime.now()
    results_df = batch_check_symbols(ALL_SYMBOLS, years, max_workers=3)
    elapsed = datetime.now() - start_time
    
    # Generar reportes
    logger.info(f"\n⏱️ Tiempo total: {elapsed}")
    
    # Reporte completo
    full_report = generate_report(results_df, years, BASE_DIR / "dukascopy_symbols_complete.csv")
    
    # Reporte solo de símbolos disponibles (al menos 1 año)
    available_symbols = results_df[results_df['total_available_years'] > 0]
    logger.info(f"\n✅ Símbolos con datos disponibles: {len(available_symbols)}/{len(ALL_SYMBOLS)}")
    
    generate_report(available_symbols, years, BASE_DIR / "dukascopy_symbols_available.csv")
    
    # Reporte de metales (especial atención)
    metals_df = available_symbols[available_symbols.index.isin(METALS)]
    if not metals_df.empty:
        logger.info("\n🥇 Metales disponibles:")
        print(metals_df[['total_available_years'] + [str(y) for y in years if str(y) in metals_df.columns]].to_string())
        generate_report(metals_df, years, BASE_DIR / "dukascopy_metals.csv")
    
    # Resumen
    print("\n" + "="*80)
    print("📊 RESUMEN DE DISPONIBILIDAD")
    print("="*80)
    
    for year in years:
        year_str = str(year)
        if year_str in results_df.columns:
            available_count = results_df[results_df[year_str] == True].shape[0]
            print(f"   {year}: {available_count}/{len(ALL_SYMBOLS)} símbolos disponibles ({available_count/len(ALL_SYMBOLS)*100:.1f}%)")
    
    print("\n📁 Reportes generados:")
    print(f"   - {BASE_DIR}/dukascopy_symbols_complete.csv")
    print(f"   - {BASE_DIR}/dukascopy_symbols_available.csv")
    print(f"   - {BASE_DIR}/dukascopy_metals.csv")
    
    # Mostrar los metales específicos que buscamos
    print("\n🎯 Metales específicos buscados:")
    for metal in ["XPTUSD", "XAGUSD", "XAUUSD"]:
        if metal in available_symbols.index:
            years_available = [y for y in years if available_symbols.loc[metal, str(y)] == True]
            print(f"   {metal}: ✅ Disponible en años: {years_available}")
        else:
            print(f"   {metal}: ❌ No disponible")

if __name__ == "__main__":
    main()
