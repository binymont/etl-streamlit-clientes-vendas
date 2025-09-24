import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Dashboard de Vendas", layout="wide")
st.title("📊 Dashboard de Vendas - Clientes e Produtos")

OUTPUT_DIR = "data/output"
RESUMO_CLIENTES_PATH = os.path.join(OUTPUT_DIR, "resumo_clientes")
BALANCO_PRODUTOS_PATH = os.path.join(OUTPUT_DIR, "balanco_produtos")

def read_spark_csv(path):
    if not os.path.exists(path):
        st.error(f"Pasta não encontrada: {path}")
        return pd.DataFrame()
    all_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(".csv")]
    if not all_files:
        st.error(f"Nenhum arquivo CSV encontrado em {path}")
        return pd.DataFrame()
    df_list = [pd.read_csv(f) for f in all_files]
    df = pd.concat(df_list, ignore_index=True)
    return df

resumo_clientes = read_spark_csv(RESUMO_CLIENTES_PATH)
balanco_produtos = read_spark_csv(BALANCO_PRODUTOS_PATH)

st.sidebar.header("Filtros")
cliente_filter = st.sidebar.text_input("Filtrar por nome de cliente:")
produto_filter = st.sidebar.text_input("Filtrar por ID de produto:")

st.header("🏆 Clientes")

if not resumo_clientes.empty:
    df_clientes = resumo_clientes.copy()
    if cliente_filter:
        df_clientes = df_clientes[df_clientes["nome"].str.contains(cliente_filter, case=False, na=False)]

    st.subheader("Top 10 Clientes por Total de Vendas")
    top_clientes = df_clientes.sort_values("total_vendas", ascending=False).head(10)
    st.bar_chart(top_clientes.set_index("nome")["total_vendas"])

    st.subheader("📋 Resumo Completo de Clientes")
    st.dataframe(df_clientes)
else:
    st.warning("Nenhum dado de clientes encontrado")

st.header("🛒 Produtos")

if not balanco_produtos.empty:
    df_produtos = balanco_produtos.copy()
    if produto_filter:
        df_produtos = df_produtos[df_produtos["produto_id"].astype(str).str.contains(produto_filter)]

    st.subheader("Top 10 Produtos por Total de Vendas")
    top_produtos = df_produtos.sort_values("total_vendas_produto", ascending=False).head(10)
    st.bar_chart(top_produtos.set_index("produto_id")["total_vendas_produto"])

    st.subheader("📋 Resumo Completo de Produtos")
    st.dataframe(df_produtos)

    if "total_vendas_produto" in df_produtos.columns and not df_produtos.empty:
        st.subheader("Distribuição de Vendas por Produto")
        st.pyplot(
            df_produtos.plot.pie(
                y="total_vendas_produto",
                labels=df_produtos["produto_id"],
                autopct="%1.1f%%",
                legend=False,
                figsize=(6,6)
            ).figure
        )
else:
    st.warning("Nenhum dado de produtos encontrado")

st.header("📈 Métricas Gerais")

if not resumo_clientes.empty and not balanco_produtos.empty:
    total_vendas = resumo_clientes["total_vendas"].sum()
    total_quantidade_vendas = resumo_clientes["quantidade_vendas"].sum()
    ticket_medio_geral = round(total_vendas / total_quantidade_vendas, 2) if total_quantidade_vendas > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("💰 Total Vendas", f"R$ {total_vendas:,.2f}")
    col2.metric("🛍️ Total Vendas (Qtd)", f"{total_quantidade_vendas}")
    col3.metric("🎯 Ticket Médio Geral", f"R$ {ticket_medio_geral:,.2f}")
