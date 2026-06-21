from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType, BooleanType)

class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    # Define e retorna o schema para o dataframe de pagamentos
    def _get_schema_pagamentos(self) -> StructType:
        return StructType([
        StructField("id_pedido", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_pagamento", FloatType(), True),
        StructField("status", BooleanType(), True),
        StructField("data_processamento", TimestampType(), True),
        StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", FloatType(), True)
            ]), True)
    ])
    
    # Define e retorna o schema para o dataframe de pedidos
    def _get_schema_pedidos(self) -> StructType:
        return StructType([
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True)
        ])
    
    def load_pagamentos(self, path: str, compression: str) -> DataFrame:
        "Carrega o dataframe de pagamentos a partir de arquivos JSON"
        schema = self._get_schema_pagamentos()
        return self.spark.read.option("compression", compression).json(path, schema=schema)
    
    def load_pedidos(self, path: str, compression: str, header:bool, sep: str) -> DataFrame:
        "Carrega o dataframe de pedidos a partir de arquivos CSV"
        schema = self._get_schema_pedidos()
        return self.spark.read.option("compression", compression).csv(path, header=header, schema=schema, sep=sep)
    