# 🚀 Cómo Usar el Script:
  1. Consolidación básica para un símbolo:
 ```bash
  python consolidate_global.py --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet
 ```
2. Consolidar años específicos:
 ```bash
  python consolidate_global.py --input-dir consolidated --output-file XAUUSD_2010-2015.parquet --years 2010-2015
 ```
3. Consolidar todos los símbolos:
 ```bash
  python consolidate_global.py --input-dir consolidated --output-file ALL_GLOBAL.parquet --symbol ALL
 ```
4. Con compresión GZIP (más compresión):
 ```bash
  python consolidate_global.py --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet --compression gzip
 ```
5. Sin optimización (más rápido):
 ```bash
  python consolidate_global.py --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet --skip-optimization
 ```
6. Modo verboso para debugging:
 ```bash
  python consolidate_global.py --input-dir consolidated --output-file XAUUSD_GLOBAL.parquet --verbose
 ```

# 📊 Características Principales:
  ✅ Consolidación inteligente: Maneja múltiples años y símbolos
  ✅ Validación automática: Verifica consistencia entre archivos
  ✅ Optimización global: Tipos de datos optimizados para todo el dataset
  ✅ Metadatos completos: Guarda información detallada en archivo JSON
  ✅ Manejo de errores: Continúa o pregunta al usuario según severidad
  ✅ Estadísticas detalladas: Muestra rendimiento y ratios de compresión
  ✅ Filtrado flexible: Por años, símbolos o ambos
  ✅ Logging completo: Todo queda registrado para auditoría

# 📁 Estructura Resultante:
text
consolidated/
├── XAUUSD_2010_FULL.parquet
├── XAUUSD_2011_FULL.parquet
└── ...

global/
├── XAUUSD_GLOBAL.parquet          # Archivo global consolidado
├── XAUUSD_GLOBAL.metadata.json    # Metadatos detallados
└── global_consolidation.log       # Log de operaciones

# 🎯 Beneficios de la Consolidación Global:
- Análisis más rápido: Un solo archivo para todo el histórico
- Backtesting completo: Datos de múltiples años en una sola carga
- Menor overhead: Menos archivos para manejar
- Compresión mejorada: Mejor ratio con datos más grandes
- Consultas SQL: Posibilidad de usar herramientas como DuckDB directamente