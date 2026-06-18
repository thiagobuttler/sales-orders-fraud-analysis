from pyspark.sql import SparkSession
from config.settings import carregar_config

# Carregando configurações do arquivo yaml
config = carregar_config()

# Acessando o campo de spark name do arquivo yamç
spark_app_name: str = config['spark']['app_name']

class SparkSessionManager:
    """
    Gerencia a criação e o acesso à sessão Spark
    """
    @staticmethod  
    def get_spark_session(app_name: str = spark_app_name) -> SparkSession:
        """
        Cria e retorna uma sessão Spark.Cria
        
        :param app_name: Nome da aplicação Spark.
        :return: Instância da SparkSession.
        """
        return SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .getOrCreate()