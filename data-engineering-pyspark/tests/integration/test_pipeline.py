import gzip
import json
import pytest
from datetime import datetime
from unittest.mock import MagicMock
from pyspark.sql.types import (
    ArrayType,
    DateType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
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
        StructField("produto", StringType(), True),
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
            "pagamentos_json" : {"compression": "gzip"},
            "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
        },
    }


@pytest.fixture
def dataframes_mock(spark):
    df_pagamentos = spark.createDataFrame(
        [
            (
                "a1",
                "PIX",
                100.50,
                False,
                datetime(2026, 6, 26, 10, 30, 0),
                (False, 10.5),
            ),
            (
                "b2",
                "CARTÃO",
                200.50,
                False,
                datetime(2026, 6, 25, 10, 30, 0),
                (False, 10.5),
            ),
            (
                "c3",
                "BOLETO",
                300.50,
                False,
                datetime(2026, 6, 24, 10, 30, 0),
                (False, 10.5),
            ),
        ],
        SCHEMA_PAGAMENTOS,
    )

    df_pedidos = spark.createDataFrame(
        [
            (
                "a1",
                "Camiseta",
                100.50,
                1,
                datetime(2026, 6, 26, 10, 30, 0),
                "SP",
                1,
            ),
            (
                "a2",
                "Relógio",
                500.50,
                1,
                datetime(2026, 6, 23, 10, 30, 0),
                "BA",
                2,
            ),
            (
                "c3",
                "Calça",
                300.50,
                1,
                datetime(2026, 6, 24, 10, 30, 0),
                "RS",
                5,
            ),
        ],
        SCHEMA_PEDIDOS,
    )

    return df_pagamentos, df_pedidos


def _handler_mock(df_pagamentos, df_pedidos):
    handler = MagicMock(spec=DataHandler)
    handler.load_pagamentos.return_value = df_pagamentos
    handler.load_pedidos.return_value = df_pedidos
    return handler


class TestPipelineOrquestracao:

    def test_le_pagamentos_com_parametros_da_config(
        self, spark, config_teste, dataframes_mock
    ):
        handler = _handler_mock(*dataframes_mock)
        Pipeline(handler, Transformation()).run(config_teste)
        handler.load_pagamentos.assert_called_once_with(path="/mock/pagamentos/", compression="gzip")

    def test_le_pedidos_com_parametros_da_config(
        self, spark, config_teste, dataframes_mock
    ):
        handler = _handler_mock(*dataframes_mock)
        Pipeline(handler, Transformation()).run(config_teste)
        handler.load_pedidos.assert_called_once_with(
            path="/mock/pedidos/",
            compression="gzip",
            header=True,
            sep=";",
        )

    def test_grava_no_path_de_output(self, spark, config_teste, dataframes_mock):
        handler = _handler_mock(*dataframes_mock)
        Pipeline(handler, Transformation()).run(config_teste)
        handler.write_parquet.assert_called_once()
        assert handler.write_parquet.call_args.kwargs["path"] == "/mock/output/"


class TestPipelineEndToEnd:

    def test_pipeline_completo_gera_parquet_valido(self, spark, tmp_path):
        pagamentos = [
            {
                "id_pedido": "452e8f9f-54d6-48fa-95a1-0d50a9568730",
                "forma_pagamento": "CARTAO_CREDITO",
                "valor_pagamento": 300.0,
                "status": False,
                "data_processamento": "2025-01-23T02:17:40.639413",
                "avaliacao_fraude": {"fraude": False, "score": 0.77},
            },
            {
                "id_pedido": "53d6cc71-961f-446f-b545-d12ac83bf478",
                "forma_pagamento": "PIX",
                "valor_pagamento": 570.0,
                "status": False,
                "data_processamento": "2025-01-19T03:08:14.290672",
                "avaliacao_fraude": {"fraude": False, "score": 0.84},
            },
        ]
        pagamentos_path = tmp_path / "pagamentos.json.gz"
        with gzip.open(pagamentos_path, "wt", encoding="utf-8") as f:
            for p in pagamentos:
                f.write(json.dumps(p) + "\n")

        pedidos_lines = [
            "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
            "452e8f9f-54d6-48fa-95a1-0d50a9568730;TV;1500.0;2;2025-01-01T10:00:00;SP;1",
            "53d6cc71-961f-446f-b545-d12ac83bf478;PC;3000.0;1;2025-01-02T11:00:00;RJ;2",
        ]
        pedidos_path = tmp_path / "pedidos.csv.gz"
        with gzip.open(pedidos_path, "wt", encoding="utf-8") as f:
            f.write("\n".join(pedidos_lines))

        output_path = str(tmp_path / "output")
        config = {
            "paths": {
                "pagamentos": str(pagamentos_path),
                "pedidos": str(pedidos_path),
                "output": output_path,
            },
            "file_options": {
                "pagamentos_json": {
                    "compression": "gzip",
                },
                "pedidos_csv": {
                    "compression": "gzip",
                    "header": True,
                    "sep": ";",
                },
            },
        }

        Pipeline(DataHandler(spark), Transformation()).run(config)

        resultado = spark.read.parquet(output_path)
        assert set(resultado.columns) == {
            "id_pedido",
            "uf",
            "forma_pagamento",
            "valor_pagamento",
            "data_criacao",
        }
        assert resultado.count() == 2
