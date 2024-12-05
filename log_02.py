# logs_01.py
import logging
from colorlog import ColoredFormatter  # Para salida coloreada en la terminal
import pandas as pd
import numpy as np
import os

class DataProcessor:
    """
    Clase que encapsula métodos para procesar datos utilizando pandas y numpy.
    Cada método realiza operaciones en un DataFrame y utiliza logs para registrar los resultados.
    """
    
    def __init__(self, input_data: pd.DataFrame, log_file: str = "data_processing.log"):
        """
        Inicializa la clase con un DataFrame de entrada y configura los logs.
        
        :param input_data: DataFrame inicial con datos para procesar.
        :param log_file: Nombre del archivo donde se registrarán los logs.
        """
        self.data = input_data
        self.logger = self._setup_logger(log_file)
        self.logger.info("Clase DataProcessor inicializada correctamente.")
    
    def _setup_logger(self, log_file: str) -> logging.Logger:
        """
        Configura el logger para registrar mensajes tanto en la consola como en un archivo.
        
        :param log_file: Archivo donde se guardarán los logs.
        :return: Objeto Logger configurado.
        """
        logger = logging.getLogger("DataProcessorLogger")
        logger.setLevel(logging.DEBUG)
        
        # Formato del log
        console_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red,bg_white",
            },
        )
        
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Handler para la consola con colores
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(console_formatter)
        
        # Handler para archivo
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        
        # Agregar handlers al logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def add_column(self, column_name: str, values: list):
        """
        Agrega una columna al DataFrame.
        
        :param column_name: Nombre de la columna a agregar.
        :param values: Valores de la nueva columna.
        """
        try:
            self.logger.debug(f"Intentando agregar columna '{column_name}'.")
            self.data[column_name] = values
            self.logger.info(f"Columna '{column_name}' agregada correctamente.")
        except Exception as e:
            self.logger.warning(f"No se pudo agregar la columna '{column_name}': {e}")
    
    def apply_numpy_function(self, column_name: str, func: callable):
        """
        Aplica una función de numpy a una columna específica.
        
        :param column_name: Nombre de la columna donde se aplicará la función.
        :param func: Función de numpy a aplicar.
        """
        try:
            self.logger.debug(f"Aplicando la función '{func.__name__}' a la columna '{column_name}'.")
            try:
                self.data[f"{column_name}_transformed"] = self.data[column_name].apply(func)
                self.logger.info(
                    f"Función '{func.__name__}' aplicada correctamente a la columna '{column_name}'."
                )
            except KeyError as e:
                self.logger.warning(f"Columna '{column_name}' no encontrada: {e}")
        except Exception as e:
            self.logger.critical(
                f"Error crítico al aplicar la función '{func.__name__}' a la columna '{column_name}': {e}"
            )
    
    def save_to_csv(self, output_file: str):
        """
        Guarda el DataFrame en un archivo CSV.
        
        :param output_file: Ruta del archivo CSV a generar.
        """
        try:
            self.logger.debug(f"Intentando guardar el archivo en '{output_file}'.")
            self.data.to_csv(output_file, index=False)
            self.logger.info(f"Archivo guardado correctamente en '{output_file}'.")
        except PermissionError as e:
            self.logger.warning(f"No se pudo guardar el archivo (permiso denegado): {e}")
        except Exception as e:
            self.logger.critical(f"Error crítico al guardar el archivo '{output_file}': {e}")
    
    def run(self):
        """
        Método principal que ejecuta una serie de transformaciones en el DataFrame.
        """
        self.logger.info("Iniciando procesamiento de datos.")
        
        # Agregar una columna válida
        self.add_column("NewColumn", [1, 2, 3, 4, 5])
        
        # Intentar aplicar una función válida de numpy
        self.apply_numpy_function("NewColumn", np.sqrt)
        
        # Intentar aplicar una función a una columna inexistente
        self.apply_numpy_function("NonExistentColumn", np.log)
        
        # Provocar un error crítico a propósito
        try:
            self.logger.debug("Forzando un error crítico: división por cero.")
            _ = 1 / 0
        except ZeroDivisionError as e:
            self.logger.critical(f"Error crítico provocado: {e}")
        
        # Guardar el DataFrame en un archivo CSV
        self.save_to_csv("output_data.csv")
        
        self.logger.info("Procesamiento de datos finalizado.")

if __name__ == "__main__":
    # Crear un DataFrame inicial
    data = pd.DataFrame({
        "A": [1, 2, 3, 4, 5],
        "B": [10, 20, 30, 40, 50],
    })
    
    # Crear instancia de la clase y ejecutar el procesamiento
    processor = DataProcessor(data)
    processor.run()
