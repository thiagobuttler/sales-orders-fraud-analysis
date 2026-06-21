import logging
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """
    def __init__(self, data_handler: DataHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer
        
    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação e salvamento.
        """
        logger.info("Pipeline iniciado...")
        
        # Definindo variáveis para capturar as configurações do yaml (pagamentos)
        path_pagamentos = config['paths']['pagamentos']
        compression_pagamentos = config['file_options']['pagamentos_json']['compression']
        
        pagamentos_df = self.data_handler.load_pagamentos(path = path_pagamentos, 
                                        compression = compression_pagamentos)
        
        # Definindo variáveis para capturar as configurações do yaml (pedidos)
        path_pedidos = config['paths']['pedidos']
        compression_pedidos = config['file_options']['pedidos_csv']['compression']
        header_pedidos = config['file_options']['pedidos_csv']['header']
        separator_pedidos = config['file_options']['pedidos_csv']['sep']
        
        pedidos_df = self.data_handler.load_pedidos(path = path_pedidos, 
                                  compression = compression_pedidos, 
                                  header = header_pedidos, 
                                  sep = separator_pedidos)
        
        # Join entre os dataframes de pagamentos e pedidos
        pagamentos_pedidos_df = self.transformer.join_pagamentos_pedidos(pagamentos_df, pedidos_df)
        logger.info(f"Join entre pagamentos e pedidos realizado com sucesso! | Registros: {pagamentos_pedidos_df.count()}")
        
        # Filtra apenas os pagamentos recusados, identificados como legítimo
        pagamentos_pedidos_df = self.transformer.filter_pagamentos_recusados(pagamentos_pedidos_df)
        logger.info(f"Filtro de pagamentos = false e fraude = false realizado com sucesso! | Registros: {pagamentos_pedidos_df.count()}")
        
        # Apaga as colunas de status e fraude, desnecessárias para o df final
        pagamentos_pedidos_df = self.transformer.drop_status_fraude(pagamentos_pedidos_df)
        logger.info(f"Colunas status e fraude removidas com sucesso! | Registros: {pagamentos_pedidos_df.count()}")
        
        # Filtra apenas os pedidos de 2025
        pagamentos_pedidos_df = self.transformer.filter_pedidos_2025(pagamentos_pedidos_df)
        logger.info(f"Filtro de pedidos de 2025 realizado com sucesso! | Registros: {pagamentos_pedidos_df.count()}")
        
        # Ordena o df final pelas colunas de UF, forma de pagamento e data de criação do pedido
        pagamentos_pedidos_df = self.transformer.orderBy_pagamentos_pedidos(pagamentos_pedidos_df)
        logger.info(f"Dataframe ordenado por UF, forma de pagamento e data de criação do pedido com sucesso!")
        
        # Salvando o df final em parquet
        path_output = config['paths']['output']
        logger.info(f"Obtido o path de saída: {path_output}")
        
        self.data_handler.write_parquet(df = pagamentos_pedidos_df, path = path_output)
        
        logger.info("Pipeline concluído com sucesso!")