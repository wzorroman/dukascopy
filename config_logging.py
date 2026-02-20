# utils/config_loggings.pý
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

# Variables de configuración
VERSION_APP = "1.0.1"
APP_NAME = "dukascopy"

def setup_global_logging():
    """Configura el logging global para toda la aplicación"""
    try:
        project_root = os.path.dirname(os.path.abspath(sys.argv[0]))
        log_dir = os.path.join(project_root, "LOGS")
        os.makedirs(log_dir, exist_ok=True)

        # Configurar el logger principal
        logger = logging.getLogger('app')
        #logger.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)

        # Evitar que los mensajes se propaguen al logger root
        logger.propagate = False
        if logger.handlers:
            logger.handlers.clear()

        formatter = logging.Formatter('%(asctime)s - [%(name)s:%(lineno)s] - %(levelname)s - %(message)s')

        log_filename = f"app_daily_{APP_NAME}.log"
        path_log_filename = os.path.join(log_dir, log_filename)

        # Handler para rotación diaria usando suffix
        daily_handler = TimedRotatingFileHandler(
            path_log_filename,
            when='midnight',      # Rotar a medianoche
            interval=1,           # Cada día
            backupCount=14,       # Conservar 14 días
            encoding='utf-8'
        )

        def history_namer(default_name):
            """
            Renombra los archivos rotados al formato deseado:
            De: app_daily_<APP_NAME>.log.2024-11-06
            A: 2024-11-06_app_history_<APP_NAME>.log
            """
            if default_name.endswith(".log"):
                return default_name

            # Extraer la fecha del nombre por defecto
            base_name = os.path.basename(default_name)
            dir_name = os.path.dirname(default_name)

            # El formato por defecto es: nombre.log.YYYY-MM-DD
            if ".log." in base_name:
                name_part, date_part = base_name.split(".log.", 1)
                # Cambiar el formato a: app_history_..._YYYY-MM-DD.log
                new_name = f"{date_part}_app_history_{APP_NAME}.log"
                return os.path.join(dir_name, new_name)

            return default_name

        daily_handler.namer = history_namer

        daily_handler.setFormatter(formatter)
        daily_handler.setLevel(logging.INFO)

        # Handler para consola (opcional)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        # Agregar handlers al logger
        logger.addHandler(daily_handler)
        logger.addHandler(console_handler)  # Mantener consola para ver logs

        # Logger principal de la aplicación
        logger.info(f"=== INICIO DE APLICACIÓN ({VERSION_APP}) ===")
        logger.info(f"Logging configurado en: {path_log_filename}")

        return logger, path_log_filename

    except Exception as e:
        print(f"Error crítico al configurar logging: {str(e)}")
        raise

# Inicializar logging global al importar el módulo
app_logger, LOG_FILE = setup_global_logging()

# Función para obtener logger en otros módulos
def get_logger(name):
    """Obtiene un logger con el nombre especificado"""
    return logging.getLogger(f'app.{name}')
