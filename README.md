## Como executar o pipeline

### Utilizando o main.py (desenvolvimento)
Adicionar ao PYTHONPATH o diretorio root do projeto, no caso ./data-engineering-pyspark/...

Para isso: 

1. Navegar até o diretório data-engineering-pyspark 
2. Utilizar o comando *export PYTHONPATH=$(pwd)/src* para adicionar o caminho à variável de ambiente pythonpath.

O PYTONPATH permite que o python localize os módulos do projeto.

### Utilizando o comando spark-submit

1. Navegar até o diretório data-engineering-pyspark 
2. Executar o comando spark-submit --master "local[*]" --py-files ./dist/analise_pedidos_legitimos_recusados-0.1.0-py3-none-any.whl ./src/main.py

O arquivo *.whl permite que o pyspark localize os módulos do projeto, descritos no arquivo ./data-engineering-pyspark/requirements.txt