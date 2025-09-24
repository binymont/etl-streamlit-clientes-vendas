import os
import sys
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    substring, col, trim, to_date, when, regexp_replace,
    sum as _sum, count as _count, round as _round
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# -------------------------
# Criar SparkSession
# -------------------------
def create_spark(app_name: str = "PySpark ETL - Resumo Clientes") -> SparkSession:
    """Cria e retorna uma SparkSession."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# -------------------------
# Ler clientes
# -------------------------
def read_clients(spark: SparkSession, clientes_path: str):
    """Lê o arquivo de clientes e retorna um DataFrame."""
    df = spark.read.option("header", True).option("inferSchema", True).csv(clientes_path)
    df = df.withColumn("cliente_id", col("cliente_id").cast("int"))
    return df

# -------------------------
# Ler vendas posicionais
# -------------------------
def read_vendas_positional(spark: SparkSession, vendas_path: str):
    """Lê o arquivo de vendas posicional e retorna um DataFrame estruturado."""
    df_raw = spark.read.text(vendas_path)
    df = df_raw.select(
        substring(col("value"), 1, 5).alias("venda_id"),
        substring(col("value"), 6, 5).alias("cliente_id"),
        substring(col("value"), 11, 5).alias("produto_id"),
        substring(col("value"), 16, 8).alias("valor_raw"),
        substring(col("value"), 24, 8).alias("data_venda_str"),
    )
    df = df.withColumn("valor_clean", trim(regexp_replace(col("valor_raw"), ",", ".")))
    df = df.withColumn(
        "valor",
        when(col("valor_clean").rlike(r"^\s*-?\d+(\.\d{2})?\s*$"), col("valor_clean").cast("double"))
        .otherwise(None)
    )
    df = df.withColumn("venda_id", col("venda_id").cast("int"))
    df = df.withColumn("cliente_id", col("cliente_id").cast("int"))
    df = df.withColumn("produto_id", col("produto_id").cast("int"))
    df = df.withColumn("data_venda", to_date(col("data_venda_str"), "yyyyMMdd"))
    df = df.select("venda_id", "cliente_id", "produto_id", "valor", "data_venda")
    return df

# -------------------------
# Transformação e agregação com validação
# -------------------------
def transform_and_aggregate(vendas_df, clientes_df):
    """
    Realiza validações, agrega dados e retorna dois DataFrames:
    resumo_clientes e balanco_produtos.
    """
    # Validação: vendas sem cliente correspondente
    vendas_sem_cliente = vendas_df.join(clientes_df, on="cliente_id", how="left_anti")
    count_sem_cliente = vendas_sem_cliente.count()
    if count_sem_cliente > 0:
        logging.warning(f"{count_sem_cliente} vendas sem cliente correspondente!")
        vendas_sem_cliente.write.mode("overwrite").option("header", "true").csv("data/output/vendas_sem_cliente")

    # Validação: valores nulos ou negativos
    vendas_invalidas = vendas_df.filter((col("valor").isNull()) | (col("valor") <= 0))
    count_invalidas = vendas_invalidas.count()
    if count_invalidas > 0:
        logging.warning(f"{count_invalidas} vendas com valor nulo, zero ou negativo!")
        vendas_invalidas.write.mode("overwrite").option("header", "true").csv("data/output/vendas_invalidas")

    # Filtra vendas válidas
    vendas_validas = vendas_df.filter((col("valor").isNotNull()) & (col("valor") > 0))

    joined = vendas_validas.join(clientes_df, on="cliente_id", how="left")

    resumo_clientes = joined.groupBy("cliente_id", "nome").agg(
        _round(_sum("valor"), 2).alias("total_vendas"),
        _count("venda_id").alias("quantidade_vendas"),
        _round(_sum("valor") / _count("venda_id"), 2).alias("ticket_medio")
    ).orderBy(col("total_vendas").desc())

    balanco_produtos = vendas_validas.groupBy("produto_id").agg(
        _round(_sum("valor"), 2).alias("total_vendas_produto"),
        _count("venda_id").alias("quantidade_vendas_produto"),
        _round(_sum("valor") / _count("venda_id"), 2).alias("ticket_medio_produto")
    ).orderBy(col("total_vendas_produto").desc())

    return resumo_clientes, balanco_produtos

# -------------------------
# Escrita de saída com particionamento opcional
# -------------------------
def write_output(df, output_path: str, formato: str = "csv", mode: str = "overwrite"):
    """Escreve o DataFrame no formato desejado."""
    os.makedirs(output_path, exist_ok=True)
    if formato.lower() == "csv":
        df.write.mode(mode).option("header", "true").csv(output_path)
    else:
        df.write.mode(mode).parquet(output_path)

# -------------------------
# Argument Parser
# -------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="ETL PySpark - Resumo Clientes e Produtos")
    parser.add_argument("--clientes", type=str, default="data/input/clientes.csv", help="Caminho do arquivo de clientes")
    parser.add_argument("--vendas", type=str, default="data/input/vendas.txt", help="Caminho do arquivo de vendas")
    parser.add_argument("--output", type=str, default="data/output", help="Diretório de saída")
    parser.add_argument("--formato", type=str, default="csv", choices=["csv", "parquet"], help="Formato de saída")
    return parser.parse_args()

# -------------------------
# Função principal
# -------------------------
def main():
    args = parse_args()
    clientes_path = args.clientes
    vendas_path = args.vendas
    output_path = args.output
    formato = args.formato

    spark = None
    try:
        spark = create_spark()
        clientes_df = read_clients(spark, clientes_path)
        vendas_df = read_vendas_positional(spark, vendas_path)
        resumo_clientes, balanco_produtos = transform_and_aggregate(vendas_df, clientes_df)

        resumo_dir = os.path.join(output_path, "resumo_clientes")
        produtos_dir = os.path.join(output_path, "balanco_produtos")

        write_output(resumo_clientes, resumo_dir, formato=formato)
        write_output(balanco_produtos, produtos_dir, formato=formato)

        logging.info(f"ETL finalizado com sucesso! Arquivos escritos em: {output_path}")

    except Exception as e:
        logging.error(f"Erro durante execução do ETL: {e}", exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()
