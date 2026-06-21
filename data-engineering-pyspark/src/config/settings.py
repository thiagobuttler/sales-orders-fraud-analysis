import logging
import yaml

logger = logging.getLogger(__name__)

def carregar_config(path: str = "./trabalho-final-data-eng-prog/data-engineering-pyspark/config/settings.yaml") -> dict:
    """Carrega um arquivo de coniguração YAML."""
    with open(path, 'r') as file:
        config = yaml.safe_load(file)
        
    logger.info("Informações do projeto carregadas.")    
    return config