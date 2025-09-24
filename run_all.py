import subprocess
import os
import sys
import time

CLIENTES_PATH = "data/input/clientes.csv"
VENDAS_PATH = "data/input/vendas.txt"
OUTPUT_DIR = "data/output"
FORMATO = "csv"

def run_etl():
    print("🚀 Iniciando ETL...")
    cmd = [
        sys.executable,
        "main.py",
        "--clientes", CLIENTES_PATH,
        "--vendas", VENDAS_PATH,
        "--output", OUTPUT_DIR,
        "--formato", FORMATO
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print("❌ ETL falhou!")
            print(result.stderr)
            sys.exit(1)
        print("✅ ETL finalizado com sucesso!")
    except Exception as e:
        print("❌ Erro ao rodar ETL:", e)
        sys.exit(1)

def run_dashboard():
    print("📊 Abrindo dashboard...")
    try:
        subprocess.Popen([sys.executable, "-m", "streamlit", "run", "dashboard.py"])
    except Exception as e:
        print("❌ Erro ao abrir dashboard:", e)

if __name__ == "__main__":
    if not os.path.exists(CLIENTES_PATH) or not os.path.exists(VENDAS_PATH):
        print("❌ Arquivos de entrada não encontrados!")
        sys.exit(1)

    run_etl()
    time.sleep(2)
    run_dashboard()
