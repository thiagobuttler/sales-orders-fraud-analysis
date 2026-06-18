# Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType, BooleanType)
from config.settings import carregar_config
from session.spark_session import SparkSessionManager

# Carregando configurações do arquivo yaml
config = carregar_config()

# Acessando campos do yaml para definição do nome do projeto spark
app_name = config['spark']['app_name']
print(f"Obtido o app name: {app_name}")

# Iniciando a sessão spark
spark = SparkSessionManager.get_spark_session(app_name=app_name)
print(f"""Spark Session iniciada:
- SparkSession: {spark}
- AppName: {app_name}
        """)
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

# Definindo variáveis para capturar as configurações do yaml
path_pagamentos = config['paths']['pagamentos']
compression_pagamentos = config['file_options']['pagamentos_json']['compression']

print(f"""Obtido os seguintes paramentros de pagamentos:
- path: {path_pagamentos}
- compression: {compression_pagamentos}
""")

pagamentos = spark.read \
                  .option("compression", compression_pagamentos) \
                  .json(path_pagamentos, schema=schema_pagamentos)

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

path_pedidos = config['paths']['pedidos']
compression_pedidos = config['file_options']['pedidos_csv']['compression']
header_pedidos = config['file_options']['pedidos_csv']['header']
separator_pedidos = config['file_options']['pedidos_csv']['sep']

print(f"""Obtido os seguintes parametros de pedidos: 
- path: {path_pedidos}
- compression_pedidos = {compression_pedidos}
- header_pedidos = {header_pedidos}
- separator_pedidos = {separator_pedidos}
""")

pedidos = spark.read \
               .schema(schema_pedidos) \
               .option("compression", compression_pedidos) \
               .csv(path_pedidos, header=header_pedidos, schema=schema_pedidos, sep=separator_pedidos)

pagamentos.show(10, truncate=False)
pedidos.show(10, truncate=False)