# Imports
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline

def main():

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
    
    # Criando uma instância da classe transformation
    transformer = Transformation()
    
    # Definindo variáveis para capturar as configurações do yaml
    path_pagamentos = config['paths']['pagamentos']
    compression_pagamentos = config['file_options']['pagamentos_json']['compression']
    
    print(f"""Obtido os seguintes paramentros de pagamentos:
    - path: {path_pagamentos}
    - compression: {compression_pagamentos}
    """)
    
    print("Abrindo o dataframe de pagamentos")
    pagamentos = dh.load_pagamentos(path = path_pagamentos, 
                                    compression = compression_pagamentos)
    
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
    pedidos = dh.load_pedidos(path = path_pedidos, 
                              compression = compression_pedidos, 
                              header = header_pedidos, 
                              sep = separator_pedidos)
    
    # Join entre os dataframes de pagamentos e pedidos
    pagamentos_pedidos_df = transformer.join_pagamentos_pedidos(pagamentos, pedidos)
    print("Join entre pagamentos e pedidos realizado")
    
    # Filtra apenas os pagamentos recusados, identificados como legítimo
    pagamentos_pedidos_df = transformer.filter_pagamentos_recusados(pagamentos_pedidos_df)
    print("Filtro de pagamentos = false e fraude = false realizado")
    
    # Apaga as colunas de status e fraude, desnecessárias para o df final
    pagamentos_pedidos_df = transformer.drop_status_fraude(pagamentos_pedidos_df)
    print("Colunas status e fraude removidas")
    
    # Filtra apenas os pedidos de 2025
    pagamentos_pedidos_df = transformer.filter_pedidos_2025(pagamentos_pedidos_df)
    print("Filtrado apenas os pedidos de 2025")
    
    # Ordena o df final pelas colunas de UF, forma de pagamento e data de criação do pedido
    pagamentos_pedidos_df = transformer.orderBy_pagamentos_pedidos(pagamentos_pedidos_df)
    print("Dataframe ordenado por UF, forma de pagamento e data de criação do pedido")
    
    # Salvando o df final em parquet
    path_output = config['paths']['output']
    print(f"Obtido o path de saída: {path_output}")
    
    dh.write_parquet(df = pagamentos_pedidos_df, path = path_output)
    print(f"Arquivo parquet salvo em: {path_output}")
    
    # Finalizando a sessão spark
    spark.stop()

if __name__ == "__main__":
  main()