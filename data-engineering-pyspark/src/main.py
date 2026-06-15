# Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType, BooleanType)

# Iniciando a sessão spark
spark = SparkSession.builder.appName("Analise de Pedidos Legitimos Recusados").getOrCreate()

# (Pagamentos) Definido schema de forma explícita

print("Definindo schema do dataset pagamentos")

schema_pagamentos = StructType([
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

# (Pagamentos) Realizando a leitura dos arquivos json no formado gzip

print("Abrindo o dataframe de pagamentos")

pagamentos = spark.read \
                  .option("compression", "gzip") \
                  .json("./trabalho-final-data-eng-prog/data-engineering-pyspark/data/input/dataset-json-pagamentos/data/pagamentos", schema=schema_pagamentos)

pagamentos.printSchema()

pagamentos.show(10, truncate=False)

# (Pedidos) Definido schema de forma explícita

print("Definindo schema do dataset pedidos")

schema_pedidos = StructType([
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True)
        ])

# (Pedidos) Realizando a leitura dos arquivos json no formado gzip

print("Abrindo o dataframe de pagamentos")

pedidos = spark.read \
               .schema(schema_pedidos) \
               .option("compression", "gzip") \
               .csv("./trabalho-final-data-eng-prog/data-engineering-pyspark/data/input/data-csv-pedidos/data/pedidos", header=True, schema=schema_pedidos, sep=";")
               
pedidos.printSchema()

pedidos.show(10, truncate=False)