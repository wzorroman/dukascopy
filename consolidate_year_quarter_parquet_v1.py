#!/usr/bin/env python3
import os
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
import argparse
import logging
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

class ParquetConsolidatorV2:
    def __init__(self, input_dir, output_dir):
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_quarter_months(self, quarter):
        """Retorna la lista de meses para un trimestre dado."""
        mapping = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}
        return mapping.get(quarter, [])

    def consolidate_demand(self, symbol, year, quarter=None, compression='snappy'):
        """Consolida archivos por año o por trimestre específico."""
        months = self.get_quarter_months(quarter) if quarter else range(1, 13)
        suffix = f"-Q{quarter}" if quarter else ""
        output_file = self.output_dir / f"{symbol}_{year}{suffix}.parquet"

        logging.info(f"🔍 Buscando archivos para {symbol} {year} {suffix}...")

        # Localizar archivos mensuales
        monthly_files = []
        for month in months:
            # Busca patrones como XAUUSD_2024_01.ticks.parquet o XAUUSD_2024_01.parquet
            pattern = f"{symbol}_{year}_{month:02d}*.parquet"
            found = list(self.input_dir.glob(pattern))
            monthly_files.extend(found)

        if not monthly_files:
            logging.error(f"❌ No se encontraron archivos para el periodo solicitado.")
            return None

        logging.info(f"📦 Consolidando {len(monthly_files)} archivos en {output_file.name}...")

        # Leer y concatenar
        tables = []
        for f in sorted(monthly_files):
            try:
                tables.append(pq.read_table(f))
            except Exception as e:
                logging.error(f"❌ Error leyendo {f.name}: {e}")

        if tables:
            final_table = pa.concat_tables(tables)
            pq.write_table(final_table, output_file, compression=compression)
            logging.info(f"✅ ¡Éxito! Tamaño final: {output_file.stat().st_size / (1024**2):.2f} MB")
            return output_file
        return None

def main():
    parser = argparse.ArgumentParser(description="Consolidador Trimestral v2")
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--quarter', type=int, choices=[1, 2, 3, 4], help="Trimestre (1-4)")
    parser.add_argument('--compression', default='snappy')

    args = parser.parse_args()
    consolidator = ParquetConsolidatorV2(args.input_dir, args.output_dir)
    consolidator.consolidate_demand(args.symbol, args.year, args.quarter, args.compression)

if __name__ == "__main__":
    # python3 consolidate_year_quarter_parquet_v1.py --input-dir "/home/wilson/GOLD/DATOS_ORIGEN" --output-dir "consolidated" --symbol XAUUSD --year 2024 --quarter 1
    # python3 consolidate_year_quarter_parquet_v1.py --input-dir "/home/wilson/GOLD/DATOS_ORIGEN" --output-dir "consolidated" --symbol XAUUSD --year 2024 --quarter 2
    main()