import pytest
from pyspark.sql import SparkSession
from main import read_clients, read_vendas_positional, transform_and_aggregate
from pyspark.sql.functions import col

# -------------------------
# Fixture do SparkSession
# -------------------------
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("ETL Test") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

# -------------------------
# Teste leitura clientes
# -------------------------
def test_read_clients(spark):
    df = read_clients(spark, "data/input/clientes.csv")
    # Verifica se dataframe não está vazio
    assert df.count() > 0
    # Verifica colunas
    assert "cliente_id" in df.columns
    assert "nome" in df.columns
    assert "data_nascimento" in df.columns
    # Verifica tipo da coluna cliente_id
    assert dict(df.dtypes)["cliente_id"] == "int"

# -------------------------
# Teste leitura vendas
# -------------------------
def test_read_vendas(spark):
    df = read_vendas_positional(spark, "data/input/vendas.txt")
    # Verifica se dataframe não está vazio
    assert df.count() > 0
    # Verifica colunas
    for col_name in ["venda_id", "cliente_id", "produto_id", "valor", "data_venda"]:
        assert col_name in df.columns
    # Verifica tipo da coluna valor
    assert dict(df.dtypes)["valor"] in ["double", "float"]

# -------------------------
# Teste agregações
# -------------------------
def test_transform_and_aggregate(spark):
    clientes_df = read_clients(spark, "data/input/clientes.csv")
    vendas_df = read_vendas_positional(spark, "data/input/vendas.txt")
    resumo_clientes, balanco_produtos = transform_and_aggregate(vendas_df, clientes_df)

    # Verifica colunas resumo_clientes
    for col_name in ["cliente_id", "nome", "total_vendas", "quantidade_vendas", "ticket_medio"]:
        assert col_name in resumo_clientes.columns
    # Verifica valores não nulos
    resumo_clientes_na = resumo_clientes.filter(
        col("total_vendas").isNull() |
        col("quantidade_vendas").isNull() |
        col("ticket_medio").isNull()
    ).count()
    assert resumo_clientes_na == 0

    # Verifica colunas balanco_produtos
    for col_name in ["produto_id", "total_vendas_produto", "quantidade_vendas_produto", "ticket_medio_produto"]:
        assert col_name in balanco_produtos.columns
    # Valores não nulos
    balanco_produtos_na = balanco_produtos.filter(
        col("total_vendas_produto").isNull() |
        col("quantidade_vendas_produto").isNull() |
        col("ticket_medio_produto").isNull()
    ).count()
    assert balanco_produtos_na == 0

# -------------------------
# Teste valores válidos
# -------------------------
def test_valores_validos(spark):
    vendas_df = read_vendas_positional(spark, "data/input/vendas.txt")
    # Só considera valores positivos e não nulos como válidos
    assert vendas_df.filter(col("valor").isNull()).count() == 0 or vendas_df.filter(col("valor") <= 0).count() > 0
