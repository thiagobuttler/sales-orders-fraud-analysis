import gzip
import json
import os
import pytest
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    BooleanType,
)

from io_utils.data_handler import DataHandler


@pytest.fixture
def arquivo_pagamentos_gz(tmp_path):
    "Arquivo JSON gzipado com cinco pagamentos de exemplo."
    pagamentos = [
        {
            "id_pedido": "452e8f9f-54d6-48fa-95a1-0d50a9568730",
            "forma_pagamento": "CARTAO_CREDITO",
            "valor_pagamento": 300.0,
            "status": True,
            "data_processamento": "2024-01-23T02:17:40.639413",
            "avaliacao_fraude": {"fraude": False, "score": 0.77},
        },
        {
            "id_pedido": "53d6cc71-961f-446f-b545-d12ac83bf478",
            "forma_pagamento": "PIX",
            "valor_pagamento": 570.0,
            "status": True,
            "data_processamento": "2024-01-19T03:08:14.290672",
            "avaliacao_fraude": {"fraude": False, "score": 0.84},
        },
        {
            "id_pedido": "30dcfd9e-9883-488a-8598-c7d2d918adc4",
            "forma_pagamento": "PIX",
            "valor_pagamento": 1045.0,
            "status": True,
            "data_processamento": "2024-01-16T13:50:06.994955",
            "avaliacao_fraude": {"fraude": False, "score": 0.68},
        },
        {
            "id_pedido": "376fac0a-9f23-4d9a-b6ea-dfa97e655c11",
            "forma_pagamento": "CARTAO_CREDITO",
            "valor_pagamento": 700.0,
            "status": True,
            "data_processamento": "2024-01-24T12:55:26.381288",
            "avaliacao_fraude": {"fraude": False, "score": 0.45},
        },
        {
            "id_pedido": "ba8207a0-549b-4208-8826-c3e71a955c3a",
            "forma_pagamento": "CARTAO_CREDITO",
            "valor_pagamento": 500.0,
            "status": True,
            "data_processamento": "2024-01-18T18:09:56.221746",
            "avaliacao_fraude": {"fraude": False, "score": 0.28},
        },
    ]

    gz_path = tmp_path / "pagamentos.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for p in pagamentos:
            f.write(json.dumps(p) + "\n")
    return str(gz_path)


@pytest.fixture
def arquivo_pedidos_gz(tmp_path):
    "arquivo CSV gzipado com três pedidos de exemplo."
    linhas = [
        "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
        "abc-001;TV;1500.0;2;2024-01-01T10:00:00;SP;1",
        "abc-002;PC;3000.0;1;2024-01-02T11:00:00;RJ;2",
        "abc-003;MONITOR;800.0;3;2024-01-03T12:00:00;MG;1",
    ]
    gz_path = tmp_path / "pedidos.csv.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(linhas))
    return str(gz_path)


class TestLoadPagamentos:

    def test_le_json_gz_e_retorna_dataframe(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(
            arquivo_pagamentos_gz, compression="gzip"
        )
        assert df.count() == 5

    def test_schema_aplica_tipos_corretos(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(
            arquivo_pagamentos_gz, compression="gzip"
        )
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["id_pedido"], StringType)
        assert isinstance(tipos["status"], BooleanType)


class TestLoadPedidos:

    def test_le_csv_com_separador_ponto_e_virgula(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz,
            compression="gzip",
            header=True,
            sep=";",
        )
        assert df.count() == 3

    def test_schema_pedidos_tem_tipos_numericos(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz,
            compression="gzip",
            header=True,
            sep=";",
        )
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["valor_unitario"], FloatType)
        assert isinstance(tipos["quantidade"], LongType)


class TestWriteParquet:

    def test_dados_gravados_podem_ser_relidos(self, spark, tmp_path):
        schema = StructType(
            [
                StructField("id_cliente", LongType(), True),
                StructField("valor_total", FloatType(), True),
            ]
        )
        df = spark.createDataFrame([(1, 3000.0), (2, 300.0)], schema)
        output_path = str(tmp_path / "saida_parquet")

        DataHandler(spark).write_parquet(df, output_path)

        assert os.path.exists(output_path)
        assert spark.read.parquet(output_path).count() == 2
