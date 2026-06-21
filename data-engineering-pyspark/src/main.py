# Imports
from pathlib import Path
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline
import logging

def configurar_logging():
    """Configura o logging para todo o projeto."""
    BASE_DIR = Path(__file__).resolve().parent.parent
    LOG_DIR = BASE_DIR / "logs"
    
    LOG_DIR.mkdir(exist_ok=True)
    
    logging.basicConfig(
        # Nível mínimo de severidade para ser registrado.
        # DEBUG < INFO < WARNING < ERROR < CRITICAL
        level=logging.INFO,
        # Formato de mensagem de log.
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        # Lista de handlers.
        handlers=[
            logging.FileHandler(LOG_DIR / "analise-pedidos-legitimos-recusados.log"),
            logging.StreamHandler()
        ],
        force=True
    )
    logging.info("Logging configurado.")

def main():
    
    # Cria uma instância de logger
    logger = logging.getLogger(__name__)

    # Carregando configurações do arquivo yaml
    config = carregar_config()
    
    # Acessando campos do yaml para definição do nome do projeto spark
    app_name = config['spark']['app_name']
    logging.info(f"Obtido o app name: {app_name}")
    
    # Iniciando a sessão spark
    spark = SparkSessionManager.get_spark_session(app_name=app_name)
            
    # Raiz de composição (Composition Root)
    data_handler = DataHandler(spark)
    transformer = Transformation()
    pipeline = Pipeline(data_handler, transformer)
    pipeline.run(config=config)
    
    spark.stop()
    logger.info("Sessão spark finalizada.")

if __name__ == "__main__":
  configurar_logging()
  main()