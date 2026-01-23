# 🚀 Cómo Usar el Script:
 1. Consolidación básica para un año:
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011

 2. Consolidación con compresión GZIP:
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011 --compression gzip

 3. Consolidación con particiones diarias:
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011 --partition-daily

 4. Rango de años:
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --start-year 2010 --end-year 2015

 5. Múltiples símbolos:
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbols XAUUSD,EURUSD,GBPUSD --year 2023

 6. Modo verboso con validación:
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011 --verbose

 7. Eliminar archivos originales (¡cuidado!):
    python consolidate_advanced.py --input-dir data --output-dir consolidated --symbol XAUUSD --year 2011 --delete-originals

# 📁 Estructura Resultante:
```text
consolidated/
├── XAUUSD_2011_FULL.parquet          # Archivo consolidado anual
├── XAUUSD_2012_FULL.parquet
├── XAUUSD_2011_FULL_daily/           # Particiones diarias (si se habilita)
│   ├── XAUUSD_20110101.parquet
│   ├── XAUUSD_20110102.parquet
│   └── partition_index.csv
└── consolidation.log                 # Log de operaciones
```

✨ Características Principales:
    ✅ Validación automática: Verifica que los archivos sean Parquet válidos
    ✅ Optimización de tipos: Convierte float64 a float32 donde sea posible
    ✅ Múltiples compresiones: Soporta snappy, gzip, lz4, zstd
    ✅ Particionamiento diario: Opcional para acceso más rápido
    ✅ Logging detallado: Registra todo el proceso en un archivo log
    ✅ Manejo de errores: Continúa procesando incluso si algunos archivos fallan
    ✅ Procesamiento por lotes: Soporta múltiples símbolos y años
    ✅ Estadísticas completas: Muestra ratios de compresión y optimización

