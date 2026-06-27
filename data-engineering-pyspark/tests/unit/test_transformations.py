import pytest
from datetime import datetime
from pyspark.sql.types import (
    ArrayType,
    DateType,
    FloatType,
    LongType,
    StringType,
    StructType,
    StructField,
    TimestampType,
    BooleanType,
)

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

SCHEMA_PAGAMENTOS_PEDIDOS = StructType(
    [
        StructField("id_pedido", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_pagamento", FloatType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("status", BooleanType(), True),
        StructField("fraude", BooleanType(), True),
    ]
)


class TestJoinPagamentosPedidos:

    def test_inner_join_dfpagamentos_dfpedidos(self, spark):
        "Valida se apenas os id's de pedidos presentes em ambas as tabelas pagamentos e pedidos são retornados"
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

        resultado = Transformation().join_pagamentos_pedidos(df_pagamentos, df_pedidos)
        assert resultado.count() == 2
        ids = {row.id_pedido for row in resultado.collect()}
        assert ids == {"a1", "c3"}


class TestFilterPagamentosRecusados:

    def test_filter_pagamentos_recusados(self, spark):
        "Valida se somente registros com status=False e fraude=False permanecem no dataframe"
        df_pagamentos_pedidos = spark.createDataFrame(
            [
                (
                    "a1",
                    "SP",
                    "PIX",
                    100.05,
                    datetime(2026, 6, 26, 10, 30, 0),
                    False,
                    False,
                ),
                (
                    "a2",
                    "RJ",
                    "BOLETO",
                    200.00,
                    datetime(2026, 6, 25, 10, 30, 0),
                    False,
                    True,
                ),
                (
                    "a3",
                    "MG",
                    "PIX",
                    150.50,
                    datetime(2026, 6, 24, 10, 30, 0),
                    True,
                    False,
                ),
                (
                    "a4",
                    "BA",
                    "CARTÃO",
                    300.75,
                    datetime(2026, 6, 23, 10, 30, 0),
                    True,
                    True,
                ),
                (
                    "a5",
                    "SP",
                    "PIX",
                    90.00,
                    datetime(2026, 6, 22, 10, 30, 0),
                    False,
                    False,
                ),
            ],
            SCHEMA_PAGAMENTOS_PEDIDOS,
        )

        resultado = Transformation().filter_pagamentos_recusados(df_pagamentos_pedidos)
        assert resultado.count() == 2
        ids = {row.id_pedido for row in resultado.collect()}
        assert ids == {"a1", "a5"}


class TestDropColunasStatusFraude:

    def test_drop_colunas_status_fraude(self, spark):
        "Valida a exclusão do dataframe das colunas status e fraude"
        df_pagamentos_pedidos = spark.createDataFrame(
            [
                (
                    "a1",
                    "SP",
                    "PIX",
                    100.05,
                    datetime(2026, 6, 26, 10, 30, 0),
                    False,
                    False,
                ),
                (
                    "a2",
                    "RJ",
                    "BOLETO",
                    200.00,
                    datetime(2026, 6, 25, 10, 30, 0),
                    False,
                    True,
                ),
                (
                    "a3",
                    "MG",
                    "PIX",
                    150.50,
                    datetime(2026, 6, 24, 10, 30, 0),
                    True,
                    False,
                ),
                (
                    "a4",
                    "BA",
                    "CARTÃO",
                    300.75,
                    datetime(2026, 6, 23, 10, 30, 0),
                    True,
                    True,
                ),
                (
                    "a5",
                    "SP",
                    "PIX",
                    90.00,
                    datetime(2026, 6, 22, 10, 30, 0),
                    False,
                    False,
                ),
            ],
            SCHEMA_PAGAMENTOS_PEDIDOS,
        )

        resultado = Transformation().drop_status_fraude(df_pagamentos_pedidos)

        assert "status" not in resultado.columns
        assert "fraude" not in resultado.columns
        assert resultado.columns == [
            "id_pedido",
            "uf",
            "forma_pagamento",
            "valor_pagamento",
            "data_criacao",
        ]


class TestFilterPedidos2025:

    def test_filter_pedidos_2025(self, spark):
        "Valida se os registros filtrados são apenas de 2025"
        df_pagamentos_pedidos = spark.createDataFrame(
            [
                (
                    "a1",
                    "SP",
                    "PIX",
                    100.05,
                    datetime(2025, 6, 26, 10, 30, 0),
                    False,
                    False,
                ),
                (
                    "a2",
                    "RJ",
                    "BOLETO",
                    200.00,
                    datetime(2026, 6, 25, 10, 30, 0),
                    False,
                    True,
                ),
                (
                    "a3",
                    "MG",
                    "PIX",
                    150.50,
                    datetime(2025, 6, 24, 10, 30, 0),
                    True,
                    False,
                ),
                (
                    "a4",
                    "BA",
                    "CARTÃO",
                    300.75,
                    datetime(2024, 6, 23, 10, 30, 0),
                    True,
                    True,
                ),
                (
                    "a5",
                    "SP",
                    "PIX",
                    90.00,
                    datetime(2025, 6, 22, 10, 30, 0),
                    False,
                    False,
                ),
            ],
            SCHEMA_PAGAMENTOS_PEDIDOS,
        )

        resultado = Transformation().filter_pedidos_2025(df_pagamentos_pedidos)

        assert resultado.count() == 3

        anos = {row.data_criacao.year for row in resultado.collect()}

        assert anos == {2025}


class TestOrderByPagamentosPedidos:

    def test_orderBy_pagamentos_pedidos(self, spark):
        "Valida se a ordenação por uf, forma de pagamento e data de criação está sendo realizada"
        df_pagamentos_pedidos = spark.createDataFrame(
            [
                (
                    "a1",
                    "SP",
                    "PIX",
                    100.05,
                    datetime(2025, 6, 26, 10, 30, 0),
                    False,
                    False,
                ),
                (
                    "a2",
                    "RJ",
                    "BOLETO",
                    200.00,
                    datetime(2026, 6, 25, 10, 30, 0),
                    False,
                    True,
                ),
                (
                    "a3",
                    "MG",
                    "PIX",
                    150.50,
                    datetime(2025, 6, 24, 10, 30, 0),
                    True,
                    False,
                ),
                (
                    "a4",
                    "BA",
                    "CARTÃO",
                    300.75,
                    datetime(2024, 6, 23, 10, 30, 0),
                    True,
                    True,
                ),
                (
                    "a5",
                    "SP",
                    "PIX",
                    90.00,
                    datetime(2025, 6, 22, 10, 30, 0),
                    False,
                    False,
                ),
            ],
            SCHEMA_PAGAMENTOS_PEDIDOS,
        )

        resultado = Transformation().orderBy_pagamentos_pedidos(df_pagamentos_pedidos)

        ids = [row.id_pedido for row in resultado.collect()]

        assert ids == ["a4", "a3", "a2", "a5", "a1"]
