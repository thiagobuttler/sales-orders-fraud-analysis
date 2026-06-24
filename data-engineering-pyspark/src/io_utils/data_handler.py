import logging
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    FloatType,
    TimestampType,
    BooleanType,
)
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # Define e retorna o schema para o dataframe de pagamentos
    def _get_schema_pagamentos(self) -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", FloatType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", TimestampType(), True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("fraude", BooleanType(), True),
                            StructField("score", FloatType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    # Define e retorna o schema para o dataframe de pedidos
    def _get_schema_pedidos(self) -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

    def load_pagamentos(self, path: str, compression: str) -> DataFrame:
        try:
            "Carrega o dataframe de pagamentos a partir de arquivos JSON"
            schema = self._get_schema_pagamentos()
            df_pagamentos = (
                self.spark.read.option("compression", compression)
                .option("mode", "FAILFAST")
                .json(path, schema=schema)
            )
            logger.info(
                f"Dataframe pagamentos carregado. | Registros: {df_pagamentos.count()}"
            )

            if df_pagamentos.isEmpty():
                logger.warning(
                    f"O arquivo de {path} foi lido, mas não contém registros."
                )

            return df_pagamentos

        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            raise e

        except Py4JJavaError as e:
            logger.critical(
                f"Erro crítico na JVM (possível arquivo corrompido ou erro de memória): {e}"
            )
            raise e

    def load_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        try:
            "Carrega o dataframe de pedidos a partir de arquivos CSV"
            schema = self._get_schema_pedidos()
            df_pedidos = (
                self.spark.read.option("compression", compression)
                .option("mode", "FAILFAST")
                .csv(path, header=header, schema=schema, sep=sep)
            )
            logger.info(
                f"Dataframe pedidos carregado. | Registros: {df_pedidos.count()}"
            )

            if df_pedidos.isEmpty():
                logger.warning(
                    f"O arquivo de {path} foi lido, mas não contém registros."
                )

            return df_pedidos

        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            raise e

        except Py4JJavaError as e:
            logger.critical(
                f"Erro crítico na JVM (possível arquivo corrompito ou erro de memória): {e}"
            )

    def write_parquet(self, df: DataFrame, path: str):
        """Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Dados salvos com sucesso em: {path}")
