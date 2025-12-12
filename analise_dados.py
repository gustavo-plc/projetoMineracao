# Configuração Inicial e Importações

import os
import sys
import shutil
import pandas as pd
import unicodedata
import re
import traceback
from datetime import datetime

# Importações do PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, trim, lower, lit, when
from pyspark.sql.types import (
    DecimalType, StringType, DateType, IntegerType,
    StructType, StructField
)

# --- 1. Configuração Crítica para Windows ---
# Força o PySpark a usar o mesmo Python do ambiente virtual atual
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("--- Configuração Inicial e Importações ---")
print("Ambiente: Local (Windows/VS Code) adaptado.")
print(f"Versão do Python: {sys.version.split()[0]}")
print(f"Versão do Pandas: {pd.__version__}")
print("Diagnóstico da Célula 1 concluído.\n---")

# --- 2. Inicializando a Sessão Spark Manualmente (Versão Estável) ---
print("--- Iniciando Sessão Spark ---")
print("Nota: Na primeira execução, pode demorar para baixar o pacote do Excel...")

# Definição da biblioteca de Excel correta para Spark 3.x
# Versão antiga: "com.crealytics:spark-excel_2.12:0.14.0" (Causava erro)
# Versão nova: "com.crealytics:spark-excel_2.12:3.5.0_0.20.3" (Estável)
excel_maven_package = "com.crealytics:spark-excel_2.12:3.5.0_0.20.3"

spark = SparkSession.builder \
    .appName("ProjetoMineracao_Mestrado") \
    .config("spark.jars.packages", excel_maven_package) \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
    .config("spark.sql.shuffle.partitions", "32") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .master("local[*]") \
    .getOrCreate()

# Ajuste do nível de log para reduzir poluição no terminal
spark.sparkContext.setLogLevel("WARN")

# --- 3. Configuração de Diretórios Locais ---
print("--- Configurando parâmetros ---")

# DEFINA AQUI SEU CAMINHO LOCAL BASE
# Sugestão: Usar caminho relativo ou absoluto da sua pasta de projeto
BASE_DIR = os.path.join(os.getcwd(), "dados")

input_base_path = os.path.join(BASE_DIR, "input")  # Onde você colocará os .xlsx
output_base_path = os.path.join(BASE_DIR, "Parquet") # Onde serão salvos os resultados

# Garante que as pastas existam
os.makedirs(input_base_path, exist_ok=True)
os.makedirs(output_base_path, exist_ok=True)

anos_a_processar = [str(ano) for ano in range(2016, 2022)]

print(f"Caminho de entrada base configurado: {input_base_path}")
print(f"Caminho de saída base para Parquet configurado: {output_base_path}")
print(f"Anos a processar: {anos_a_processar}")

# --- 4. Verificação de Arquivos (Substituto do DBUtils) ---
# Como não temos dbutils.fs.ls, usamos os.listdir
try:
    arquivos = os.listdir(input_base_path)
    print(f"Diagnóstico: Caminho de entrada base '{input_base_path}' EXISTE e contém {len(arquivos)} itens.")
    
    if len(arquivos) == 0:
        print("⚠️ AVISO: A pasta de entrada está vazia. Coloque seus arquivos Excel em subpastas por ano (ex: dados/input/2016/)")
except Exception as e:
    print(f"ERRO DE DIAGNÓSTICO: Erro ao acessar '{input_base_path}'. Erro: {e}")

print("Diagnóstico concluído.\n---")

# --- 5. Verificação da Sessão Spark ---
if 'spark' in locals() and spark:
    print(f"✅ Sessão Spark Local está ativa. Versão: {spark.version}")
    
    # Verificando configurações definidas
    configs_to_check = [
        "spark.sql.parquet.datetimeRebaseModeInWrite",
        "spark.sql.parquet.int96RebaseModeInWrite",
        "spark.sql.shuffle.partitions"
    ]
    
    for conf in configs_to_check:
        try:
            val = spark.conf.get(conf)
            print(f"   Config '{conf}': {val}")
        except:
            print(f"   Config '{conf}': Não definida.")

    # Informação sobre paralelismo
    try:
        print(f"   Info 'default.parallelism': {spark.sparkContext.defaultParallelism}")
    except Exception:
         print("   Info 'default.parallelism': Erro ao obter.")
else:
    print("❌ Sessão Spark ('spark') não encontrada.")

# DBUtils não existe localmente, então removemos ou criamos um mock se necessário.
# Para este script, substituímos o uso dele por 'os', então não precisamos emular agora.
print("   Nota: Utilitário 'dbutils' foi substituído por funções nativas 'os' do Python.")


# ==============================================================================
# CÉLULA 2: Mapeamento de colunas e definição de Schema
# ==============================================================================

print("\n--- Executando Célula 2: Definição de Schemas ---")

output_folder_name = "final"
output_path_final = os.path.join(output_base_path, output_folder_name)

# 1. Mapeamento: Traduz nomes de colunas bagunçados para um padrão único
column_name_mapping = {
    "ano": "ano",
    "cpf do suprido": "cpf_suprido",
    "cpf/cnpj favorecido": "cpf_cnpj_favorecido",
    "cpf/cnpj do favorecido": "cpf_cnpj_favorecido",
    "objeto da aquisição": "objeto_aquisicao",
    "motivo": "objeto_aquisicao",  # Em 2016 usavam "motivo" em vez de objeto
    " objeto da aquisição ": "objeto_aquisicao",
    "objeto da aquisição ": "objeto_aquisicao",
    "valor": "valor",
    " valor ": "valor",
    " valor": "valor",
    "objeto da aquisicao": "objeto_aquisicao"
}

# 2. Schema: Define o tipo de dado de cada coluna (Texto, Inteiro, Decimal)
schema_base = StructType([
    StructField("ano", IntegerType(), True),
    StructField("cpf_suprido", StringType(), True),
    StructField("cpf_cnpj_favorecido", StringType(), True),
    StructField("objeto_aquisicao", StringType(), True),
    StructField("valor", DecimalType(12, 2), True)
])

# Lista das colunas finais que queremos manter
desired_final_columns = [field.name for field in schema_base.fields]

print(f"✅ Schema definido.")
print(f"   Colunas alvo: {desired_final_columns}")

# Verificação simples se a pasta de entrada tem conteúdo
try:
    if os.path.exists(input_base_path) and os.listdir(input_base_path):
        print(f"   Verificação: Pasta de entrada encontrada e não vazia.")
    else:
        print(f"   ⚠️ AVISO: A pasta de entrada '{input_base_path}' parece vazia.")
except Exception as e:
    print(f"   ⚠️ AVISO: Erro ao verificar entrada: {e}")

print("--- Fim da Célula 2 ---")



# ==============================================================================
# CÉLULAS 3, 4 e 5 (CONSOLIDADAS): Processamento Otimizado (Native Spark)
# ==============================================================================

print("\n--- Executando Processamento Otimizado (Native Spark) ---")

# Importação necessária para a manipulação de strings no driver (nomes de colunas)
import re

def clean_column_names(df):
    """
    Renomeia todas as colunas de uma vez só usando Select + Alias.
    """
    new_columns = []
    existing_names = set()
    
    for col_name in df.columns:
        # 1. Limpeza básica da string do nome
        clean = col_name.strip().lower()
        # Remove acentos de forma simples no nome da coluna
        for a, b in zip('áéíóúâêôãõç', 'aeiouaeoaoc'):
            clean = clean.replace(a, b)
        
        # 2. Verifica mapeamento oficial (definido na Célula 2)
        final_name = column_name_mapping.get(clean)
        
        # 3. Se não achar, padroniza (snake_case simplificado)
        if not final_name:
            final_name = re.sub(r'[^a-z0-9]+', '_', clean).strip('_')
            if not final_name: final_name = f"col_{df.columns.index(col_name)}"
        
        # 4. Resolve colisões (sufixo _1, _2)
        base_name = final_name
        count = 1
        while final_name in existing_names:
            final_name = f"{base_name}_{count}"
            count += 1
            
        existing_names.add(final_name)
        
        # Adiciona à lista de seleção
        new_columns.append(col(f"`{col_name}`").alias(final_name))
    
    return df.select(*new_columns)

def process_dataframe(df):
    """
    Aplica transformações usando APENAS funções nativas do Spark.
    """
    # 1. Padroniza nomes das colunas primeiro
    df = clean_column_names(df)
    
    # 2. Definição das Expressões de Limpeza (Lazy Evaluation)
    
    # Texto: Remove símbolos e espaços extras
    txt_clean_expr = F.trim(F.regexp_replace(F.lower(F.col("objeto_aquisicao")), r"[^a-z0-9\s]", ""))
    
    # Valor: Remove tudo que não é dígito, converte e divide por 100
    val_clean_expr = (
        F.regexp_replace(F.col("valor").cast("string"), r"[^0-9]", "").cast(DecimalType(20,0)) / 100.0
    ).cast(DecimalType(12,2))

    # CPF/CNPJ: Remove pontuação
    doc_clean_expr = lambda c: F.regexp_replace(F.col(c).cast("string"), r"[^0-9]", "")

    # 3. Montagem do Select Final
    final_cols = []
    
    # Ano
    if "ano" in df.columns:
        final_cols.append(F.col("ano").cast(IntegerType()).alias("ano"))
    else:
        final_cols.append(F.lit(None).cast(IntegerType()).alias("ano"))

    # CPFs
    for c in ["cpf_suprido", "cpf_cnpj_favorecido"]:
        if c in df.columns:
            final_cols.append(doc_clean_expr(c).alias(c))
        else:
            final_cols.append(F.lit(None).cast(StringType()).alias(c))

    # Objeto
    if "objeto_aquisicao" in df.columns:
        final_cols.append(txt_clean_expr.alias("objeto_aquisicao"))
    else:
        final_cols.append(F.lit(None).cast(StringType()).alias("objeto_aquisicao"))

    # Valor
    if "valor" in df.columns:
        final_cols.append(val_clean_expr.alias("valor"))
    else:
        final_cols.append(F.lit(None).cast(DecimalType(12,2)).alias("valor"))

    return df.select(*final_cols)

print("✅ Funções otimizadas definidas.")