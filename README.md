# ETL Integrado de Clientes e Vendas com PySpark e Dashboard

Este repositório apresenta um pipeline completo de ETL (Extract, Transform, Load) para análise de dados de clientes e vendas, utilizando **PySpark** para processamento e **Streamlit** para visualização interativa dos resultados em dashboard.

## Funcionalidades

- **Ingestão de Dados**: Leitura de arquivos de clientes (CSV) e vendas (posicional TXT).
- **Validação e Limpeza**: Identificação de vendas inválidas (valores nulos, zero ou negativos) e vendas sem cliente correspondente.
- **Transformação e Agregação**: Geração de relatórios consolidados por cliente e por produto.
- **Exportação de Resultados**: Escrita dos resultados em CSV ou Parquet, prontos para análise.
- **Dashboard Interativo**: Visualização dos principais indicadores e métricas de negócio com filtros dinâmicos.

---

## Estrutura do Projeto

```
.
├── main.py                # Script principal do ETL em PySpark
├── dashboard.py           # Dashboard interativo em Streamlit
├── run_all.py             # Script para executar ETL e dashboard em sequência
├── data/
│   └── input/
│       ├── clientes.csv   # Exemplo de arquivo de clientes
│       └── vendas.txt     # Exemplo de arquivo posicional de vendas
│   └── output/            # Resultados gerados pelo ETL
├── tests/
│   └── test_etl.py        # Testes automatizados do pipeline
├── requirements.txt       # Dependências do projeto
└── README.md
```

---

## Preparando o Ambiente

1. **Crie um ambiente virtual (recomendado):**
   - **Windows:**
     ```powershell
     python -m venv venv
     venv\Scripts\activate
     ```
   - **Linux/MacOS:**
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```

2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

   > Certifique-se de ter o Java instalado e configurado no PATH para rodar o PySpark.

---

## Executando o Pipeline

### 1. Rodando o ETL

Execute o script principal para processar os dados e gerar os relatórios:

```bash
python main.py --clientes data/input/clientes.csv --vendas data/input/vendas.txt --output data/output --formato csv
```

- `--clientes`: Caminho para o arquivo CSV de clientes.
- `--vendas`: Caminho para o arquivo TXT posicional de vendas.
- `--output`: Diretório onde os resultados serão salvos.
- `--formato`: Formato de saída (`csv` ou `parquet`).

Os resultados serão salvos em:
- `data/output/resumo_clientes/`
- `data/output/balanco_produtos/`
- Vendas inválidas e sem cliente serão salvas em subpastas específicas para análise posterior.

### 2. Executando o Dashboard

Após rodar o ETL, visualize os resultados com o dashboard interativo:

```bash
streamlit run dashboard.py
```

O dashboard permite:
- Filtrar clientes por nome.
- Filtrar produtos por ID.
- Visualizar gráficos e métricas gerais.

### 3. Execução Automática (ETL + Dashboard)

Para rodar todo o fluxo de uma vez:

```bash
python run_all.py
```

---

## Exemplos de Arquivos de Entrada

### Exemplo de `clientes.csv`

```csv
cliente_id,nome,data_nascimento,email
1,João Silva,1980-05-12,joao.silva@email.com
2,Maria Souza,1995-07-30,maria.souza@email.com
...
```

### Exemplo de `vendas.txt`

Cada linha possui campos fixos (posicionais):

```
00001000011000102450.0020230401
00002000021000201200.5020230403
...
```

---

## Testes

O projeto inclui testes automatizados para garantir a qualidade das principais funções do ETL. Para rodar os testes:

```bash
pytest tests/
```

---

## Observações

- O pipeline foi desenvolvido para ser facilmente adaptável a diferentes formatos de entrada.
- Os arquivos de saída são gerados em múltiplos arquivos (`part-*.csv`) por padrão, conforme o padrão do Spark.
- Para gerar um único arquivo CSV, utilize `.coalesce(1)` antes do `.write` no script, lembrando que isso pode impactar a performance em grandes volumes de dados.
- O dashboard pode ser customizado conforme as necessidades do negócio.

---

## Licença

Este projeto é distribuído sob a licença MIT.

---