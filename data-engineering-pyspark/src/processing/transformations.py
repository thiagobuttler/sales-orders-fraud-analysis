from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """
    
    # Cria o dataframe para as análises posteriores
    def join_pagamentos_pedidos(self, pagamentos_df: DataFrame, pedidos_df: DataFrame) -> DataFrame:
        "Faz a junção de pagamentos e pedidos"
        return pedidos_df.join(pagamentos_df, pagamentos_df.id_pedido == pedidos_df.id_pedido, "inner") \
            .select(
                pedidos_df.id_pedido,
                pedidos_df.uf,
                pagamentos_df.forma_pagamento,
                pagamentos_df.valor_pagamento,
                pedidos_df.data_criacao,
                pagamentos_df.status,
                pagamentos_df["avaliacao_fraude.fraude"].alias("fraude")
            )
    
    def filter_pagamentos_recusados(self, pagamentos_pedidos_df: DataFrame) -> DataFrame:
        "Filtra pagamentos recusados e fraudes não identificadas (pagamentos = false E fraude = false)"
        return pagamentos_pedidos_df.filter((pagamentos_pedidos_df.status == False) 
                                          & (pagamentos_pedidos_df.fraude == False))
                                          
    def drop_status_fraude(self, pagamentos_pedidos_df: DataFrame) -> DataFrame:
        "Apaga as colunas de status e fraude, pois não são necessárias para a análise final"
        
        colunas_para_remover = ['status', 'fraude']
        
        return pagamentos_pedidos_df.drop(*colunas_para_remover)
    
    def filter_pedidos_2025(self, pagamentos_pedidos_df: DataFrame) -> DataFrame:
        "Filtra apenas os pedidos realizados no ano de 2025"
        return pagamentos_pedidos_df.filter(F.year(F.col("data_criacao")) == 2025)
    
    def orderBy_pagamentos_pedidos(self, pagamentos_pedidos_df: DataFrame) -> DataFrame:
        "Ordenação por UF, forma de pagamento e data de criação"
        return pagamentos_pedidos_df.orderBy(
                F.asc("uf"),
                F.asc("forma_pagamento"),
                F.asc("data_criacao")
            )