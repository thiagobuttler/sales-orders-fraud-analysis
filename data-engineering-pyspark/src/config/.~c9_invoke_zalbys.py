import logging
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

"""
def carregar_config() -> dict:
   

    path = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"

    with open(path, "r") as file:
        config = yaml.safe_load(file)

    logger.info("Informações do projeto carregadas.")
    return config
"""


def carregar_config(
    path: str = "./trabalho-final-data-engdata-engineering-pyspark/config/settings.yaml",
) -> dict:
    "Carrega um arquivo de configuração YAML"
    with open(path, "r") as file:
        config = yaml.safe_load(file)

    logger.info("Informações do projeto carregadas.")
    return config
