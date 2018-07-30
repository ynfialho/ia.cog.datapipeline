# Desafio Cognitivo

Nesse desafio foi utilizado um AWS RDS SQL Server 2017 para armazenar os dados, DBSchema para modelagem do banco e serviços AWS que inicialmente compunham uma arquitetura rodando 100% na AWS, nesse momento fica como uma melhoria em virtude de alguns problemas no decorrer do desafio.

**Proposta de arquitetura**

![alt](https://github.com/ynfialho/ia.cog.datapipeline/blob/develop/proposta_futura.JPG) 

----------
## Como executar
Para executar o programa é necessario ter o python 3.6 instalado(assumindo que sua python environ seja python3.6 também) e editar o arquivo conf.json com os seguintes dados:

* `server`: endereço do servidor do banco (aqui foi utilizado um AWS RDS).
* `db`: nome da base de dados.
* `user`: nome do usuário no banco de dados.
* `pw`: senha do usuário

Após preenchido o arquivo de configuração, validar se os arquivos .csv estão na pasta *data* , se sim, executar o arquivo *orquestrador.py* da seguinte forma:
```python
python3.6 orquestrador.py 
```

----------
## Sobre os dados
* códigos *9999* foram importados como NULL.
* códigos de componentes da base bill_of_materials que não existem na comp_boss foram ignorados.
* dados como NA foram importados como NULL.

----------

## Modelagem


![alt](https://github.com/ynfialho/ia.cog.datapipeline/blob/develop/mer.JPG)
