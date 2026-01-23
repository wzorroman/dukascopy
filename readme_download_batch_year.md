# 📋 README: Dukascopy Batch Downloader v3

## **🚀 Descripción**

Script para descarga masiva de datos históricos de Dukascopy por meses. Descarga ticks y los convierte a OHLCV con bid/ask para un año completo o rango de meses específico.

## **📁 Estructura del Proyecto**

text

```
proyecto/
├── download_batch_year_v3.py   # Script principal (este)
├── download_dukascopy.py       # Script de descarga individual
├── config_logging.py           # Configuración de logging
├── download_progress.json      # Progreso de descargas (auto-generado)
└── data/                       # Directorio de salida por defecto
```

## **📊 Parámetros Completos**

### **🔧 Parámetros Básicos**

| **Parámetro** | **Alias** | **Valor por Defecto** | **Descripción** |
| --- | --- | --- | --- |
| `--symbol` | - | `XAUUSD` | Par de divisas a descargar |
| `--year` | - | `2010` | Año a descargar |
| `--output-dir` | - | `data` | Directorio de salida para archivos |
| `--workers` | - | `6` | Número de workers paralelos |
| `--timeframe` | - | `1min` | Timeframe para OHLCV (1min, 5min, 1H, etc) |

### **📅 Parámetros de Rango**

| **Parámetro** | **Valor por Defecto** | **Descripción** |
| --- | --- | --- |
| `--start-month` | `1` | Mes inicial (1-12) |
| `--end-month` | `12` | Mes final (1-12) |
| `--sleep-min` | `20` | Mínimo segundos de pausa entre meses |
| `--sleep-max` | `60` | Máximo segundos de pausa entre meses |

### **⚡ Parámetros de Control**

| **Parámetro** | **Descripción** |
| --- | --- |
| `--resume` | Reanudar descarga desde el último progreso guardado |
| `--verbose` | Mostrar toda la salida detallada del proceso hijo |
| `-h`, `--help` | Mostrar mensaje de ayuda |

## **🎯 Ejemplos de Uso**

### **1. Descarga Básica - Año Completo**

```bash
# Descargar todo 2010 en carpeta 'data_2010'
python download_batch_year_v3.py --symbol XAUUSD --year 2010 --output-dir data_2010
```

### **2. Rango de Meses Específico**

```bash
# Descargar solo primer trimestre 2011
python download_batch_year_v3.py --symbol XAUUSD --year 2011 --start-month 1 --end-month 3 --output-dir Q1_2011
```

### **3. Con Más Workers (Más Rápido)**

```bash
# Usar 16 workers para descarga más rápida
python download_batch_year_v3.py --symbol XAUUSD --year 2012 --workers 16 --output-dir fast_download
```

### **4. Timeframe Diferente**

```bash
# Descargar datos en timeframe de 5 minutos
python download_batch_year_v3.py --symbol XAUUSD --year 2013 --timeframe 5min --output-dir data_5min
```

### **5. Reanudar Descarga Interrumpida**

```bash
# Reanudar desde donde quedó (útil si se cortó internet)
python download_batch_year_v3.py --symbol XAUUSD --year 2014 --resume --output-dir data_2014
```

### **6. Modo Verboso (Ver Progreso Detallado)**

```bash
# Ver toda la salida incluyendo barra de progreso
python download_batch_year_v3.py --symbol XAUUSD --year 2015 --verbose --output-dir verbose_log
```

### **7. Descarga con Pausas Personalizadas**

```bash
# Pausas más cortas entre meses (10-30 segundos)
python download_batch_year_v3.py --symbol XAUUSD --year 2016 --sleep-min 10 --sleep-max 30 --output-dir quick_pauses
```

### **8. Combinación Completa**

```bash
# Ejemplo completo con todos los parámetros útiles
python download_batch_year_v3.py \
  --symbol XAUUSD \
  --year 2017 \
  --start-month 4 \
  --end-month 9 \
  --workers 12 \
  --timeframe 1min \
  --sleep-min 15 \
  --sleep-max 45 \
  --output-dir mid_2017 \
  --verbose \
  --resume
```

## **🔄 Flujo de Trabajo Recomendado**

### **Paso 1: Descarga por Años Separados**

```bash
# Descargar cada año en su propia carpeta
python download_batch_year_v3.py --symbol XAUUSD --year 2010 --output-dir data_2010
python download_batch_year_v3.py --symbol XAUUSD --year 2011 --output-dir data_2011
python download_batch_year_v3.py --symbol XAUUSD --year 2012 --output-dir data_2012
```

### **Paso 2: Verificar Descargas**

```bash
# Ver archivos descargados
ls -la data_2010/
# Ver tamaño total
du -sh data_2010/
```

### **Paso 3: Consolidar si es necesario**

```bash
# Usar scripts de consolidación si necesitas un archivo único
python consolidate_advanced.py --input-dir data_2010 --output-dir consolidated --symbol XAUUSD --year 2010
```

## **⚠️ Consideraciones Importantes**

### **Límites y Restricciones**

- **Velocidad**: Dukascopy puede limitar descargas muy rápidas
- **Volumen**: Un año completo son ~8,760 horas (aprox. 2.5M ticks)
- **Espacio**: 1 año de datos M1 ~50-100MB CSV, ~20-40MB Parquet
- **Tiempo**: Descarga completa puede tomar 30-60 minutos por año

### **Manejo de Errores**

- **Interrupción**: Usa `-resume` para continuar donde quedó
- **Errores HTTP**: Reintentos automáticos con backoff exponencial
- **Datos faltantes**: Fines de semana/horarios sin datos se saltan automáticamente

### **Archivos Generados**

```
Por cada mes exitoso se crean:
data_2010/
├── XAUUSD_2010_01.csv          # OHLCV M1 con bid/ask (principal)
└── XAUUSD_2010_01.ticks.parquet # Ticks raw (backup)

**Archivo de Progreso**
download_progress.json

Contiene estado de todas las descargas, útil para:
- Reanudar descargas interrumpidas
- Saber qué meses ya están completos
- Debuggear problemas
```

## **🛠️ Troubleshooting**

### **Problema: Descarga muy lenta**

```bash
# Solución: Reducir workers y aumentar pausas
python download_batch_year_v3.py --workers 4 --sleep-min 30 --sleep-max 90
```

### **Problema: Error "Too many requests"**

```bash
# Solución: Aumentar pausas entre meses
python download_batch_year_v3.py --sleep-min 60 --sleep-max 180
```

### **Problema: Progreso no se guarda**

```bash
# Verificar permisos de escritura
ls -la download_progress.json
chmod 644 download_progress.json
```

### **Problema: Modo verbose no muestra barra**

```bash
# Asegurarse que tqdm está instalado
pip install tqdm
# Ejecutar con --verbose
python download_batch_year_v3.py --verbose
```

## **📈 Símbolos Disponibles**

El script soporta los siguientes pares de divisas:

| **Símbolo** | **Decimales** | **Descripción** |
| --- | --- | --- |
| `EURUSD` | 5 | Euro/Dólar americano |
| `GBPUSD` | 5 | Libra/Dólar americano |
| `USDJPY` | 3 | Dólar/Yen japonés |
| `USDCHF` | 5 | Dólar/Franco suizo |
| `AUDUSD` | 5 | Dólar australiano/Dólar |
| `USDCAD` | 5 | Dólar/Dólar canadiense |
| `XAUUSD` | 5 | Oro/Dólar americano |

## **🔍 Monitoreo de Progreso**

### **Durante la Ejecución**

- Barra de progreso anual (modo normal)
- Estadísticas por mes completado
- Tiempo estimado restante
- Velocidad de descarga

### **Después de la Ejecución**

```bash
# Ver resumen de archivos
find data_2010 -name "*.csv" -exec wc -l {} \;
# Ver tamaño total
du -sh data_2010/
# Ver primeros datos
head -n 5 data_2010/XAUUSD_2010_01.csv
```

## **💾 Optimización de Almacenamiento**

### **Para análisis posterior:**

```bash
# Descargar en Parquet directamente (más eficiente)
# Nota: El script actual genera CSV + Parquet
# Para solo Parquet, modificar download_dukascopy.py
```

### **Para uso inmediato:**

```bash
# CSV es más compatible con Excel y otros software
python download_batch_year_v3.py --timeframe 1min --output-dir csv_data
```

## **🤝 Contribución y Soporte**

### **Reportar Problemas**

1. Ejecutar con `-verbose` para más detalles
2. Verificar archivo `download_progress.json`
3. Revisar logs en consola

### **Mejoras Sugeridas**

- Agregar más símbolos
- Soporte para diferentes brokers
- Exportación a más formatos
- Dashboard web de progreso
