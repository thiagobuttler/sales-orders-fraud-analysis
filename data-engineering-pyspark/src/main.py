# Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType, BooleanType)
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler

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
        
# Criando uma instância da classe data handler
dh = DataHandler(spark)

# Definindo variáveis para capturar as configurações do yaml
path_pagamentos = config['paths']['pagamentos']
compression_pagamentos = config['file_options']['pagamentos_json']['compression']

print(f"""Obtido os seguintes paramentros de pagamentos:
- path: {path_pagamentos}
- compression: {compression_pagamentos}
""")

print("Abrindo o dataframe de pagamentos")
pagamentos = dh.load_pagamentos(path = path_pagamentos, compression = compression_pagamentos)

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

print("Abrindo o dataframe de pedidos")
pedidos = dh.load_pedidos(path = path_pedidos, compression = compression_pedidos, header = header_pedidos, sep = separator_pedidos)

pagamentos.show(10, truncate=False)
pedidos.show(10, truncate=False)