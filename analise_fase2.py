# ==============================================================================
# FASE 2: Análise Exploratória de Dados (EDA) e Mineração
# Arquivo: analise_fase2.py
# ==============================================================================

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- 1. Configuração de Ambiente (Windows) ---
# Garante que o PySpark use o Python correto do ambiente virtual
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("--- Iniciando Fase 2: Análise Exploratória ---")

# --- 2. Inicializando Sessão Spark (Leve) ---
# Nota: Não precisamos mais do JAR de Excel, pois leremos Parquet nativo.
spark = SparkSession.builder \
    .appName("Analise_Gastos_Fase2") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .master("local[*]") \
    .getOrCreate()

# Otimização: Habilita Apache Arrow para converter Spark -> Pandas mais rápido (útil para gráficos)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

spark.sparkContext.setLogLevel("WARN")
print(f"✅ Sessão Spark iniciada (Versão {spark.version})")

# --- 3. Carregamento dos Dados ---
BASE_DIR = os.path.join(os.getcwd(), "dados")
input_path = os.path.join(BASE_DIR, "Consolidado_Final")

print(f"📂 Buscando base consolidada em: {input_path}")

if os.path.exists(input_path):
    try:
        # Leitura do Parquet (O Spark já entende o schema automaticamente)
        df = spark.read.parquet(input_path)
        
        # Cache: Como vamos usar esse DataFrame repetidas vezes para várias análises,
        # colocamos ele na memória para não ler do disco toda hora.
        df.cache()
        
        count = df.count()
        print(f"✅ Sucesso! Base carregada com {count} registros.")
        
        print("\n--- Estrutura dos Dados (Schema) ---")
        df.printSchema()
        
        print("\n--- Amostra Inicial (Top 5) ---")
        df.show(5, truncate=False)
        
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo Parquet: {e}")
else:
    print(f"❌ ARQUIVO NÃO ENCONTRADO. Verifique se a Fase 1 gerou a pasta: {input_path}")

# --- Fim do Script Inicial ---