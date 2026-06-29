# 📦 Sales Orders & Fraud Analysis Pipeline

Pipeline de dados desenvolvido com **PySpark** para identificar pedidos de compra **recusados** que **não foram classificados como fraude** — ou seja, recusas legítimas que representam perda real de receita.

O projeto aplica princípios de **engenharia de software** sobre um pipeline de engenharia de dados: injeção de dependência, separação de responsabilidades, tipagem explícita de schemas, testes unitários e de integração, e empacotamento para deploy distribuído via `spark-submit`.

---

## 🎯 Objetivo

Cruzar dados de **pedidos de compra** (CSV) com **registros de pagamento** (JSON), aplicar regras de negócio e entregar o resultado em **Parquet particionado** — pronto para análise em ferramentas como Power BI.

**Regras de negócio aplicadas:**

- Apenas pedidos onde `status == False` (pagamento recusado) **e** `fraude == False` (não classificado como fraude)
- Apenas pedidos criados no ano de **2025**
- Resultado ordenado por `uf → forma_pagamento → data_criacao`

---

## 🏗️ Arquitetura do Pipeline

```
CSV (pedidos) ──────┐
                    ├──► Inner Join ──► Filtros de Negócio ──► Ordenação ──► Parquet (Snappy)
JSON (pagamentos) ──┘
```

### Fluxo detalhado

| Etapa | Responsável | Descrição |
|---|---|---|
| Leitura CSV | `DataHandler.load_pedidos` | Schema explícito, compressão gzip, separador `;` |
| Leitura JSON | `DataHandler.load_pagamentos` | Schema com struct aninhado (`avaliacao_fraude`) |
| Join | `Transformation.join_pagamentos_pedidos` | Inner join por `id_pedido` |
| Filtro recusados | `Transformation.filter_pagamentos_recusados` | `status=False AND fraude=False` |
| Drop colunas | `Transformation.drop_status_fraude` | Remove `status` e `fraude` do output |
| Filtro 2025 | `Transformation.filter_pedidos_2025` | `year(data_criacao) == 2025` |
| Ordenação | `Transformation.orderBy_pagamentos_pedidos` | `uf ASC, forma_pagamento ASC, data_criacao ASC` |
| Escrita | `DataHandler.write_parquet` | Parquet com Snappy, modo overwrite |

---

## 🧱 Estrutura do Projeto

```
sales-orders-fraud-analysis/
└── data-engineering-pyspark/
    ├── config/
    │   └── settings.yaml          # Configurações de paths e opções de leitura
    ├── data/
    │   ├── input/
    │   │   ├── data-csv-pedidos/
    │   │   └── dataset-json-pagamentos/
    │   └── output/                # Parquet gerado pelo pipeline
    ├── dist/
    │   └── analise_pedidos_legitimos_recusados-0.1.0-py3-none-any.whl
    ├── logs/
    │   └── analise-pedidos-legitimos-recusados.log
    ├── src/
    │   ├── config/
    │   │   └── settings.py        # Carregamento do YAML
    │   ├── io_utils/
    │   │   └── data_handler.py    # Leitura (CSV/JSON) e escrita (Parquet)
    │   ├── pipeline/
    │   │   └── pipeline.py        # Orquestração do fluxo
    │   ├── processing/
    │   │   └── transformations.py # Regras de negócio e transformações
    │   ├── session/
    │   │   └── spark_session.py   # Gerenciamento da SparkSession
    │   └── main.py                # Entrypoint
    ├── tests/
    │   ├── conftest.py            # Fixture da SparkSession de teste
    │   ├── unit/                  # Testes unitários por módulo
    │   └── integration/           # Testes de orquestração e end-to-end
    ├── pyproject.toml
    └── requirements.txt
```

---

## ⚙️ Decisões de Engenharia de Software

### Injeção de Dependência (Composition Root)

O `Pipeline` não instancia suas dependências — ele as **recebe**. Isso permite substituir `DataHandler` e `Transformation` por mocks nos testes sem alterar nenhuma lógica de produção.

```python
# main.py — Composition Root
data_handler = DataHandler(spark)
transformer  = Transformation()
pipeline     = Pipeline(data_handler, transformer)
pipeline.run(config=config)
```

### Schema explícito com StructType

Nenhum schema é inferido em runtime. Todos os tipos são declarados explicitamente, incluindo o struct aninhado de `avaliacao_fraude`:

```python
StructField("avaliacao_fraude", StructType([
    StructField("fraude", BooleanType(), True),
    StructField("score",  FloatType(),   True),
]), True)
```

Isso garante falha rápida (`FAILFAST`) em caso de dados malformados, em vez de silenciosamente inferir tipos errados.

### Configuração externalizada via YAML

Todos os paths e opções de leitura vivem no `settings.yaml`. O código não tem nenhum caminho de arquivo hardcoded — qualquer ambiente (local, Cloud9, EMR, Databricks) é configurado apenas alterando o YAML.

```yaml
paths:
  pagamentos: "./data/input/dataset-json-pagamentos/data/pagamentos/"
  pedidos:    "./data/input/data-csv-pedidos/data/pedidos/"
  output:     "./data/output"
```

### Agnóstico de plataforma

O pipeline foi projetado para rodar em qualquer ambiente com Spark disponível:

- **Local / Cloud9 (AWS):** `SparkSession.builder.master("local[*]")`
- **EMR / Dataproc / Databricks:** basta remover `.master()` — o cluster assume o controle
- **spark-submit:** empacotado como `.whl`, todas as dependências são distribuídas automaticamente aos workers

---

## 🧪 Testes

O projeto possui cobertura em dois níveis:

### Unitários (`tests/unit/`)

Testam cada transformação de forma isolada, sem I/O real:

| Teste | O que valida |
|---|---|
| `test_inner_join_dfpagamentos_dfpedidos` | Apenas `id_pedido` presentes em ambas as tabelas são retornados |
| `test_filter_pagamentos_recusados` | Somente `status=False AND fraude=False` permanecem |
| `test_drop_colunas_status_fraude` | Colunas `status` e `fraude` removidas do schema |
| `test_filter_pedidos_2025` | Apenas registros de 2025 no resultado |
| `test_orderBy_pagamentos_pedidos` | Ordenação correta por UF, forma de pagamento e data |

### Integração (`tests/integration/`)

Testam a orquestração completa com mocks e end-to-end com dados reais:

| Teste | O que valida |
|---|---|
| `test_le_pagamentos_com_parametros_da_config` | Pipeline passa os parâmetros corretos do YAML ao `DataHandler` |
| `test_le_pedidos_com_parametros_da_config` | Idem para pedidos |
| `test_grava_no_path_de_output` | Output gravado no path definido na config |
| `test_pipeline_completo_gera_parquet_valido` | Pipeline end-to-end gera Parquet com schema e contagem corretos |

### Executar os testes

```bash
cd data-engineering-pyspark

# Todos os testes
pytest

# Apenas unitários
pytest -m unit

# Apenas integração
pytest -m integration

# Com relatório de cobertura
pytest --cov=src --cov-report=term-missing
```

---

## 🚀 Como Executar

### Pré-requisitos

- Python 3.8+
- Java 8 ou 11 (necessário para o PySpark)
- PySpark 4.1.1

```bash
pip install -r requirements.txt
```

### Utilizando o main.py (desenvolvimento)

O `PYTHONPATH` precisa apontar para o diretório `src` para que o Python localize os módulos do projeto.

1. Navegar até o diretório `data-engineering-pyspark`:

```bash
cd data-engineering-pyspark
```

2. Adicionar o caminho `src` à variável de ambiente `PYTHONPATH`:

```bash
export PYTHONPATH=$(pwd)/src
```

3. Executar o pipeline:

```bash
python src/main.py
```

> O `PYTHONPATH` permite que o Python localize os módulos do projeto sem necessidade de instalação do pacote.

### Utilizando o comando spark-submit

O arquivo `.whl` distribui os módulos do projeto aos workers do Spark, substituindo a necessidade de configurar o `PYTHONPATH` manualmente.

1. Navegar até o diretório `data-engineering-pyspark`:

```bash
cd data-engineering-pyspark
```

2. Executar o pipeline via `spark-submit`:

```bash
spark-submit \
  --master "local[*]" \
  --py-files ./dist/analise_pedidos_legitimos_recusados-0.1.0-py3-none-any.whl \
  ./src/main.py
```

> O arquivo `.whl` permite que o PySpark localize os módulos do projeto descritos no `requirements.txt`, sendo a forma recomendada para ambientes de produção e clusters distribuídos.

---

## 📦 Build do pacote

```bash
cd data-engineering-pyspark
python -m build
```

Gera o `.whl` em `dist/`, que distribui os módulos do projeto para todos os workers do Spark.

---

## 🛠️ Ambiente de Desenvolvimento

O projeto foi desenvolvido no **AWS Cloud9**, ambiente de desenvolvimento baseado em nuvem que oferece um terminal Linux com acesso direto a serviços AWS. O Cloud9 permite executar o PySpark em modo local sem necessidade de configuração de infraestrutura adicional, facilitando o ciclo de desenvolvimento e teste antes do deploy em clusters gerenciados como o EMR.

---

## 📐 Schema de Saída

O Parquet final contém as seguintes colunas:

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_pedido` | String | Identificador único do pedido |
| `uf` | String | Estado (Unidade Federativa) |
| `forma_pagamento` | String | BOLETO, CARTAO_CREDITO ou PIX |
| `valor_pagamento` | Float | Valor da transação recusada |
| `data_criacao` | Timestamp | Data e hora de criação do pedido |

---

## 🧰 Stack

| Categoria | Tecnologia |
|---|---|
| Processamento | PySpark 4.1.1 |
| Linguagem | Python 3.8+ |
| Configuração | PyYAML 6.0.3 |
| Testes | pytest 8.4.1 + pytest-cov |
| Linting | Ruff 0.12.9 |
| Formatação | Black 25.1.0 |
| Build | setuptools + build |
| Ambiente | AWS Cloud9 |
| Output | Parquet (Snappy) |

---

## 👤 Autor

**thiagobuttler**

---

## 📄 Licença

MIT