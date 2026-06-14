# Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType, BooleanType)

# Iniciando a sessão spark
spark = SparkSession.builder.appName("Analise de Pedidos Legitimos Recusados").getOrCreate()

# Definido schema de forma explícita

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
                  .schema(schema_pagamentos) \
                  .option("compression", "gzip") \
                  .json("./trabalho-final-data-eng-prog/data-engineering-pyspark/data/input/dataset-json-pagamentos/data/pagamentos")

pagamentos.printSchema()

pagamentos.show(5, truncate=False)