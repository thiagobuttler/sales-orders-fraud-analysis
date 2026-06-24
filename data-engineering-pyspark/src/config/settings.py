import logging
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "config" / "settings.yaml"

"""
def carregar_config(
    path: str = "./trabalho-final-data-eng-prog/data-engineering-pyspark/config/settings.yaml",
) -> dict:
    "Carrega um arquivo de configuração YAML"
    with open(path, "r") as file:
        config = yaml.safe_load(file)

    logger.info("Informações do projeto carregadas.")
    return config
"""

def carregar_config() -> dict:
    """Carrega um arquivo de configuração YAML."""
    
    with open(CONFIG_PATH, "r") as file:
        config = yaml.safe_load(file)

    logger.info("Informações do projeto carregadas.")
    return config
