# Template PySpark ETL (Desafio Técnico)

Este repositório contém um template em **PySpark** para resolver o desafio ETL descrito.
Inclui:
- `main.py` : script PySpark com comentários linha-a-linha em Português.
- `clientes.csv` : arquivo de exemplo com clientes.
- `vendas.txt` : arquivo posicional de exemplo (fixo, 31 caracteres por linha).
- `requirements.txt` : dependências sugeridas.
- `tests/test_etl.py` : esqueleto de teste (opcional).

---

## Como preparar o ambiente (local)

1. Crie um ambiente virtual (recomendado):
```bash
python -m venv venv
source venv/bin/activate   # Linux / MacOS
venv\\Scripts\\activate    # Windows (PowerShell)
```

2. Instale o PySpark (versão compatível com seu Python):
```bash
pip install pyspark
```

> Nota: versões de PySpark podem variar. Se estiver usando Databricks ou outra plataforma, adapte as instruções.

---

## Como rodar o script de exemplo

```bash
python main.py --clientes clientes.csv --vendas vendas.txt --output output_dir --formato csv
```

- `--clientes` : caminho para o CSV de clientes (com cabeçalho).
- `--vendas` : caminho para o TXT posicional (cada linha com 31 caracteres).
- `--output` : pasta onde serão criados `resumo_clientes/` e `balanco_produtos/`.
- `--formato` : `csv` (padrão) ou `parquet`.

**Observação:** o Spark salva CSV em **part-files** (várias partes). Se quiser um único CSV, edite `main.py` aplicando `.coalesce(1)` antes do `.write` (atenção: isso força 1 part e pode não ser eficiente para grandes volumes).

---

## Como criar manualmente os arquivos de entrada (exemplo rápido)

No Linux / MacOS (via terminal):
```bash
cat > clientes.csv <<EOF
cliente_id,nome,data_nascimento
1,João Silva,1980-05-12
2,Maria Souza,1995-07-30
3,Pedro Santos,1990-01-15
EOF

cat > vendas.txt <<EOF
00001000011000102450.0020230401
00002000021000201200.5020230403
00003000011000100100.0020230405
00004000031000100350.0020230406
00005000011000300020.0020230407
EOF
```

No Windows (PowerShell):
```powershell
@'
cliente_id,nome,data_nascimento
1,João Silva,1980-05-12
2,Maria Souza,1995-07-30
3,Pedro Santos,1990-01-15
'@ | Out-File clientes.csv -Encoding utf8

@'
00001000011000102450.0020230401
00002000021000201200.5020230403
00003000011000100100.0020230405
00004000031000100350.0020230406
00005000011000300020.0020230407
'@ | Out-File vendas.txt -Encoding utf8
```

---

## Observações finais

- O script está preparado para execução local e foi comentado linha a linha para facilitar entendimento.
- Você pode adaptar a lógica de parsing do `valor` caso o formato do arquivo posicional seja ligeiramente diferente (por exemplo: sem ponto decimal).
- Particionamento por data de venda pode ser adicionado no `write_output` usando `partitionBy("data_venda")` se desejar salvar por pastas por data.
