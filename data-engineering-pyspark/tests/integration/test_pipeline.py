import gzip
import json
import pytest
from unittest.mock import MagicMock
from pyspark.sql.types import (
    ArrayType, DateType, FloatType, LongType, StringType,
    StructField, StructType, TimestampType, BooleanType,
)

from io_utils.data_handler import DataHandler
from pipeline.pipeline import Pipeline
from processing.transformations import Transformation

SCHEMA_PAGAMENTOS = StructType(
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


SCHEMA_PEDIDOS = StructType(
    [
        StructField("id_pedido", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("uf", StringType(), True),
        StructField("id_cliente", LongType(), True),
    ]
)

@pytest.fixture
def config_teste():
    return {
        "paths": {
            "pagamentos": "/mock/pagamentos/",
            "pedidos": "/mock/pedidos/",
            "output": "/mock/output/",
        },
        "file_options": {
            "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
        },
    }

@pytest.fixture
def dataframes_mock(spark):
    