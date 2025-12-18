# ==============================================================================
# SCRIPT DE INVESTIGAÇÃO FORENSE (AUTÔNOMO)
# Descobre por que 2019 está gerando Nulos em Valor e Objeto
# ==============================================================================
import os
import sys
from pyspark.sql import SparkSession

# 1. Configuração Rápida (Recriando variáveis perdidas)
BASE_DIR = os.path.join(os.getcwd(), "dados")
input_base_path = os.path.join(BASE_DIR, "input")
path_2019 = os.path.join(input_base_path, "2019")

print(f"--- Iniciando Investigação em: {path_2019} ---")

# 2. Iniciar Spark (Simples)
spark = SparkSession.builder \
    .appName("Debug_2019") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.0_0.20.3") \
    .getOrCreate()

if not os.path.exists(path_2019):
    print(f"❌ Pasta 2019 não encontrada: {path_2019}")
    sys.exit()

# 3. Pegar um arquivo de exemplo
arquivos = [f for f in os.listdir(path_2019) if f.endswith(('.xlsx', '.xls'))]
if not arquivos:
    print("❌ Nenhum arquivo Excel encontrado em 2019.")
    sys.exit()

arquivo_alvo = arquivos[0] # Pega o primeiro
caminho_completo = os.path.join(path_2019, arquivo_alvo)

print(f"\n📂 Analisando arquivo: {arquivo_alvo}")

try:
    # 4. Leitura "Crua" (Raw)
    # Lemos sem forçar schema para ver exatamente o que o Spark enxerga
    df_raw = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load(caminho_completo)
    
    print("\n--- 1. Análise de Colunas (Nomes Exatos) ---")
    print(f"Colunas encontradas: {df_raw.columns}")
    
    # Verificação de caracteres ocultos nos nomes das colunas
    print("\nVerificando caracteres ocultos nos nomes:")
    for col in df_raw.columns:
        # Imprime o nome e sua representação 'repr' para ver espaços/tabs
        print(f"  > '{col}' (Len: {len(col)}) -> Slug esperado: '{col.lower().replace(' ', '').replace('ã','a').replace('ç','c')}'")

    print("\n--- 2. Análise de Conteúdo (Amostra Bruta) ---")
    
    # Tenta identificar a coluna de Valor
    cols_valor = [c for c in df_raw.columns if "valor" in c.lower()]
    cols_objeto = [c for c in df_raw.columns if "objeto" in c.lower() or "motivo" in c.lower()]
    
    if cols_valor:
        v_col = cols_valor[0]
        print(f"\nColuna de Valor identificada: '{v_col}'")
        print("Amostra dos primeiros 10 valores DISTINTOS (para ver formato):")
        df_raw.select(v_col).distinct().show(10, truncate=False)
    else:
        print("❌ Nenhuma coluna com nome 'valor' encontrada!")

    if cols_objeto:
        o_col = cols_objeto[0]
        print(f"\nColuna de Objeto identificada: '{o_col}'")
        df_raw.select(o_col).show(5, truncate=False)
    else:
        print("❌ Nenhuma coluna com nome 'objeto/motivo' encontrada!")

except Exception as e:
    print(f"❌ Erro fatal: {e}")

print("\n--- Fim da Investigação ---")

