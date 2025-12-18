# ==============================================================================
# SCRIPT DE ETL COMPLETO: Extração, Transformação e Carga (Fase 1)
# ==============================================================================

# --- 1. Configuração Inicial e Importações ---
import os
import sys
import shutil
import pandas as pd
import unicodedata
import re
import traceback
from datetime import datetime
from functools import reduce

# Importações do PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, trim, lower, lit, when
from pyspark.sql.types import (
    DecimalType, StringType, DateType, IntegerType, DoubleType,
    StructType, StructField
)

# Configuração para Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("--- Configuração Inicial ---")
print(f"Versão do Python: {sys.version.split()[0]}")

# --- 2. Inicializando Spark ---
print("--- Iniciando Sessão Spark ---")
excel_maven_package = "com.crealytics:spark-excel_2.12:3.5.0_0.20.3"

spark = SparkSession.builder \
    .appName("AnaliseA3_Local") \
    .config("spark.jars.packages", excel_maven_package) \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 3. Diretórios ---
BASE_DIR = os.path.join(os.getcwd(), "dados")
input_base_path = os.path.join(BASE_DIR, "input")
output_base_path = os.path.join(BASE_DIR, "Parquet")

os.makedirs(input_base_path, exist_ok=True)
os.makedirs(output_base_path, exist_ok=True)

anos_a_processar = [str(ano) for ano in range(2016, 2022)]

print(f"Diretórios configurados. Processando anos: {anos_a_processar}")

# ==============================================================================
# CÉLULA 2: Mapeamento de Colunas (Slugify)
# ==============================================================================

SCHEMA_COLUMNS_MAP = {
    # --- Chaves Temporais e Organizacionais ---
    "ano": "ano",
    "unidadegestora": "unidade_gestora",
    "periododeaplicacao": "periodo_aplicacao",
    
    # --- Identificação ---
    "suprido": "nome_suprido",
    "nomedosuprido": "nome_suprido",
    "cpfdosuprido": "cpf_suprido",
    "cpfportador": "cpf_suprido",
    "aprovado": "aprovado",
    
    # --- Favorecido ---
    "nomedofavorecido": "nome_favorecido",
    "nomefavorecido": "nome_favorecido",
    "favorecido": "nome_favorecido",
    
    "cpfcnpjfavorecido": "cpf_cnpj_favorecido",
    "cpfcnpjdofavorecido": "cpf_cnpj_favorecido",
    "cnpjoucpffavorecido": "cpf_cnpj_favorecido",
    
    # --- Detalhes ---
    "datadaaquisicao": "data_aquisicao",
    "data": "data_aquisicao",
    
    # Variações críticas
    "objetodaaquisicao": "objeto_aquisicao", 
    "motivo": "objeto_aquisicao",
    
    # --- Valores ---
    "valor": "valor_transacao",
    "valortotal": "valor_transacao"
}

COLUNAS_FINAIS_ORDENADAS = [
    "ano",
    "unidade_gestora",
    "nome_suprido",
    "cpf_suprido",
    "periodo_aplicacao",
    "aprovado",
    "data_aquisicao",
    "nome_favorecido",
    "cpf_cnpj_favorecido",
    "objeto_aquisicao",
    "valor_transacao"
]

print("✅ Schema definido.")

# ==============================================================================
# CÉLULA 3: Funções de Limpeza (Blindadas)
# ==============================================================================
print("\n--- Definindo Funções de Limpeza ---")

def clean_column_names(df):
    """Padroniza colunas para slug (sem acento, minúsculo, sem espaço)"""
    new_columns = []
    accents_src = 'áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ'
    accents_tgt = 'aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN'
    
    for col_name in df.columns:
        clean = col_name
        trans_table = str.maketrans(accents_src, accents_tgt)
        clean = clean.translate(trans_table).lower()
        clean_slug = re.sub(r'[^a-z0-9]', '', clean)
        
        final_name = SCHEMA_COLUMNS_MAP.get(clean_slug)
        if not final_name: final_name = clean_slug 
            
        new_columns.append(F.col(f"`{col_name}`").alias(final_name))
    
    return df.select(*new_columns)

def process_dataframe(df):
    """Aplica limpeza e tipagem forte"""
    df = clean_column_names(df)
    
    # Helpers de Regex
    src_chars = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
    tgt_chars = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"
    
    def clean_text_expr(col_name):
        return F.trim(F.regexp_replace(F.translate(F.lower(F.col(col_name)), src_chars, tgt_chars), r"[^a-z0-9\s]", ""))

    # Limpeza de Valor (Allowlist: só números, vírgula e traço)
    val_clean_expr = F.regexp_replace(
        F.regexp_replace(F.col("valor_transacao").cast("string"), r"[^0-9,-]", ""), 
        ",", "."
    ).cast(DoubleType())

    doc_clean_expr = lambda c: F.regexp_replace(F.col(c).cast("string"), r"[^0-9]", "")

    # Montagem do Select Final
    final_cols = []
    col_defs = {
        "ano": (IntegerType(), F.col("ano") if "ano" in df.columns else F.lit(None)),
        "unidade_gestora": (StringType(), clean_text_expr("unidade_gestora") if "unidade_gestora" in df.columns else F.lit(None)),
        "periodo_aplicacao": (StringType(), F.col("periodo_aplicacao").cast(StringType()) if "periodo_aplicacao" in df.columns else F.lit(None)),
        "nome_suprido": (StringType(), clean_text_expr("nome_suprido") if "nome_suprido" in df.columns else F.lit(None)),
        "cpf_suprido": (StringType(), doc_clean_expr("cpf_suprido") if "cpf_suprido" in df.columns else F.lit(None)),
        "aprovado": (StringType(), clean_text_expr("aprovado") if "aprovado" in df.columns else F.lit(None)),
        "data_aquisicao": (StringType(), F.trim(F.col("data_aquisicao").cast("string")) if "data_aquisicao" in df.columns else F.lit(None)),
        "nome_favorecido": (StringType(), clean_text_expr("nome_favorecido") if "nome_favorecido" in df.columns else F.lit(None)),
        "cpf_cnpj_favorecido": (StringType(), doc_clean_expr("cpf_cnpj_favorecido") if "cpf_cnpj_favorecido" in df.columns else F.lit(None)),
        "objeto_aquisicao": (StringType(), clean_text_expr("objeto_aquisicao") if "objeto_aquisicao" in df.columns else F.lit(None)),
        "valor_transacao": (DoubleType(), val_clean_expr if "valor_transacao" in df.columns else F.lit(None))
    }

    for name in COLUNAS_FINAIS_ORDENADAS:
        dtype, expr = col_defs[name]
        final_cols.append(expr.cast(dtype).alias(name))

    return df.select(*final_cols)

print("✅ Funções definidas.")

# ==============================================================================
# CÉLULA 4: Execução do Pipeline (Leitura -> Particionamento)
# ==============================================================================
print("\n--- Executando Processamento ---")

output_path_final = os.path.join(output_base_path, "final")

for ano in anos_a_processar:
    caminho_ano = os.path.join(input_base_path, ano)
    
    if not os.path.exists(caminho_ano):
        continue

    arquivos = [f for f in os.listdir(caminho_ano) if f.endswith(('.xlsx', '.xls'))]
    if not arquivos: continue
        
    print(f"\n>>> Processando {len(arquivos)} arquivos de {ano}...")
    
    dfs_ano = []
    
    for arquivo in arquivos:
        path_file = os.path.join(caminho_ano, arquivo)
        try:
            df_raw = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .load(path_file)
            
            df_clean = process_dataframe(df_raw)
            
            # Garante coluna ano se vier nula
            df_clean = df_clean.withColumn("ano", F.when(F.col("ano").isNull(), F.lit(int(ano))).otherwise(F.col("ano")))
                
            dfs_ano.append(df_clean)
            
        except Exception as e:
            print(f"❌ Erro em {arquivo}: {e}")

    if dfs_ano:
        try:
            df_ano_final = reduce(lambda df1, df2: df1.unionByName(df2), dfs_ano)
            output_dir = os.path.join(output_path_final, f"ano_partition={ano}")
            df_ano_final.write.mode("overwrite").parquet(output_dir)
            print(f"   💾 {ano}: Salvo ({df_ano_final.count()} linhas)")
        except Exception as e:
            print(f"❌ Erro consolidando {ano}: {e}")

print("\n✅ Reprocessamento concluído.")

# ==============================================================================
# CÉLULA 5: Consolidação Final (Gold) e Visualização
# ==============================================================================
print("\n--- Executando Consolidação ---")

input_parquet_path = os.path.join(output_base_path, "final")
output_consolidado = os.path.join(BASE_DIR, "Consolidado_Final")

try:
    df_full = spark.read.option("basePath", input_parquet_path).parquet(input_parquet_path)
    
    total_bruto = df_full.count()
    print(f"✅ Total Bruto Carregado: {total_bruto}")
    
    # 1. TRATAMENTO DE CAMPOS VAZIOS
    df_treated = df_full.withColumn(
        "objeto_aquisicao",
        F.when(
            F.col("objeto_aquisicao").isNull() | 
            (F.trim(F.col("objeto_aquisicao")) == "") | 
            (F.col("objeto_aquisicao") == "na"), 
            F.lit("NAO INFORMADO")
        ).otherwise(F.col("objeto_aquisicao"))
    )

    # 2. FILTRO FINANCEIRO
    df_gold = df_treated.filter(
        F.col("valor_transacao").isNotNull() & 
        (F.col("valor_transacao") > 0)
    )
    
    total_liquido = df_gold.count()
    descartados = total_bruto - total_liquido
    
    print(f"✅ Total Válido Final: {total_liquido}")
    print(f"🚮 Descartados (Lixo/Sem Valor): {descartados}")
    
    # 3. SALVAMENTO
    print(f"💾 Salvando Dataset Consolidado em: {output_consolidado}")
    
    df_gold.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_consolidado)
        
    print("✅ Consolidação concluída!")
    
    # 4. VISUALIZAÇÃO FINAL (Ajustada)
    print("\n--- Amostra Final dos Dados (Todas as Colunas - Top 5) ---")
    df_gold.show(5, truncate=True)
    
    print("\n--- Schema Final do Arquivo Consolidado ---")
    df_gold.printSchema()

except Exception as e:
    print(f"❌ Erro na consolidação: {e}")