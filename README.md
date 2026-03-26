## 1. Arquitetura do Projeto

O fluxo de dados segue uma arquitetura "Medallion" simplificada:

1. **Fonte - Bronze:** Dados brutos em CSV (~60k linhas x 80 colunas - 4M dados)
2. **Processamento - Silver - (Python):** Limpeza, tipagem e transformação utilizando `Pandas`.
3. **Storage Intermediário - Silver - (Parquet):** Escrita em formato colunar para eficiência de I/O.
4. **Destino - Silver - (Postgres):** Carga final no PostgreSQL estruturada em Star Schema.
5. **Tratamento do dado transformado - GOLD:** Aplicação das regras de negócio aos dados transformados.
6. **Query dos dados - GOLD - (jupyter):** Análise dos dados via queries.

## 2. Documentação do Projeto 

O desenvolvimento foi dividido em etapas modulares para garantir a escalabilidade

### ETAPA A: Ingestão (Python)
Os dados brutos foram extraídos do site: _https://github.com/owid/co2-data_.
E foram consequentemente armazenados no diretório `data/raw`.
> **Print 1: extraction.py script**
![Pipeline Extraction](images/extraction.png)

### ETAPA B: Transformação (Python)
Os dados são carregados a partir do diretório `data/raw`.
Uma série de transformações é realizada nesses dados, garantindo que a camada Silver seja imutável e performática.
Os dados são então salvos em formato `.parquet` no diretório `data/silver`.
> **Print 2: transformation.py script - pt1**
![Pipeline Transformation](images/transformation_1.png)

> **Print 3: transformation.py script - pt2**
![Pipeline Transformation](images/transformation_2.png)

> **Print 4: transformation.py script - pt3**
![Pipeline Transformation](images/transformation_3.png)

> **Print 5: Estrutura das pastas**
![Pipeline Transformation](images/folder_structure.png)

### ETAPA C: Carga no PostgreSQL (SQLAlchemy)
Aplica-se, inicialmente, as regras de negócio às tabelas geradas na camada silver.
Utilizando o `schema.sql` existente no diretório `database_configuration`,
as tabelas foram criadas, no PostgreSQL, com as devidas restrições (Primary Keys) e comentários.
> **Print 6: load.py script - pt1**
![Pipeline Load](images/load_1.png)
 
> **Print 7: load.py script - pt2**
![Pipeline Load](images/load_2.png)

> **Print 8: load.py script - pt3** 
![Pipeline Load](images/load_3.png)

> **Print 9: Data Dictionary - fact_emissions**
![Pipeline Load](images/data_dict_fact_emissions.png)

> **Print 10: Data Dictionary - fact_consumption** 
![Pipeline Load](images/data_dict_fact_consumption.png)

> **Print 11: Data Dictionary - fact_emission_sources**
![Pipeline Load](images/data_dict_fact_emission_sources.png)

> **Print 12: Data Dictionary - fact_non_co2_ghg** 
![Pipeline Load](images/data_dict_fact_non_co2_ghg.png)

> **Print 13: Data Dictionary - fact_climate_impact** 
![Pipeline Load](images/data_dict_fact_climate_impact.png)

### ETAPA D: Análise e Visualização (Jupyter)
A partir das tabelas do PostgreSQL, 7 queries de negócio são executadas no Jupyter Notebook.
> **Print 14: main.py script - pt.1** 
![Pipeline Load](images/main_1.png)

> **Print 15: main.py script - pt.2** 
![Pipeline Load](images/main_2.png)

> **Print 16: main.py script - pt.3** 
![Pipeline Load](images/main_3.png)

> **Print 17: main.py run - pt.3** 
![Pipeline Load](images/main_run.png)

> **Print 18: jupyter config**
![Pipeline GOLD](images/Jupyter_config.png)

> **Print 19: jupyter Query 1**
![Pipeline GOLD](images/jupyter_q1.png)

> **Print 20: jupyter Graphic 1**
![Pipeline GOLD](images/jupyter_q1_graphic.png)

> **Print 21: jupyter Query 2**
![Pipeline GOLD](images/jupyter_q2.png)

> **Print 22: jupyter Graphic 2**
![Pipeline GOLD](images/jupyter_q2_graphic.png)

> **Print 23: jupyter Query 3**
![Pipeline GOLD](images/jupyter_q3.png)

> **Print 24: jupyter Graphic 3**
![Pipeline GOLD](images/jupyter_q3_graphic.png)

> **Print 25: jupyter Query 4**
![Pipeline GOLD](images/jupyter_q4.png)

> **Print 26: jupyter Graphic 4**
![Pipeline GOLD](images/jupyter_q4_graphic.png)

> **Print 23: jupyter Query 5**
![Pipeline GOLD](images/jupyter_q5.png)

> **Print 24: jupyter Graphic 5**
![Pipeline GOLD](images/jupyter_q5_graphic.png)

> **Print 24: jupyter Query 6**
![Pipeline GOLD](images/jupyter_q6.png)

> **Print 25: jupyter Graphic 6**
![Pipeline GOLD](images/jupyter_q6_graphic.png)

> **Print 26: jupyter Query 7**
![Pipeline GOLD](images/jupyter_q7.png)

> **Print 27: jupyter Graphic 7**
![Pipeline GOLD](images/jupyter_q7_graphic.png)

## EXTRA: Relatório de Caracterização (Jupyter)
> **Print 28: Caracterização 1**
![Pipeline report](images/carac_1.png)

> **Print 29: Caracterização 2**
![Pipeline report](images/carac_2.png)

> **Print 30: Caracterização - Graphic report 1**
![Pipeline report](images/carac_graphic_1.png)

> **Print 30: Caracterização - Graphic report 2**
![Pipeline report](images/carac_graphic_2.png)

> **Print 30: Caracterização - Graphic report 3**
![Pipeline report](images/carac_graphic_3.png)

> **Print 30: Caracterização - Graphic report 4**
![Pipeline report](images/carac_graphic_4.png)

> **Print 30: Caracterização - Graphic report 5**
![Pipeline report](images/carac_graphic_5.png)

## 3. Instruções de Execução
Siga esta ordem exata para replicar o ambiente:
1. **Instale as dependências:** `pip install -r requirements.txt`.
2. **Configure as credenciais:** Crie o arquivo `.env` baseado no `.env.example`.
3. **Prepare o Banco de Dados:** Execute o script `schema.sql` no seu Postgres para criar a estrutura.
4. **Execute o Pipeline:** `python main.py` (Isso irá ler o CSV, gerar o Parquet e carregar o Postgres).
5. **Abra o Relatório:** Execute as células do `analysis_report.ipynb` para ver os gráficos interativos.