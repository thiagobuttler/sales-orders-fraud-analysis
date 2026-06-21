import logging
import yaml

logger = logging.getLogger(__name__)

def carregar_config(path: str = "./trabalho-final-data-eng-prog/data-engineering-pyspark/config/settings.yaml") -> dict:
    """Carrega um arquivo de coniguração YAML."""
    with open(path, 'r') as file:
        logger.info("Carrengando informações do projeto...")
        return yaml.safe_load(file)