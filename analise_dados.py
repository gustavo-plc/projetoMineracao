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

# --- 2. Inicializando a Sessão Spark Manualmente ---
print("--- Iniciando Sessão Spark ---")
print("Nota: Na primeira execução, pode demorar para baixar o pacote do Excel...")

spark = SparkSession.builder \
    .appName("ProjetoMineracao_Mestrado") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.14.0") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
    .config("spark.sql.shuffle.partitions", "32") \
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
# CÉLULA 3: Funções de Limpeza e Padronização (UDFs)
# ==============================================================================

print("\n--- Executando Célula 3: Funções de Limpeza ---")

# Garante que as bibliotecas necessárias para texto estejam importadas
import unicodedata
import re
from pyspark.sql.types import StringType

def standardize_text(text):
    """
    Limpa e padroniza texto:
    - Remove acentos (ex: 'avô' -> 'avo')
    - Remove caracteres especiais (mantém apenas letras, números e espaços)
    - Converte para minúsculas
    - Remove espaços duplicados
    """
    if text is None:
        return None

    text_str = str(text).strip()

    if not text_str:
        return None  # Evita processar string vazia

    # Normalização Unicode (Separa o acento da letra e remove)
    text_str = unicodedata.normalize('NFKD', text_str) \
        .encode('ASCII', 'ignore') \
        .decode('ASCII')

    # Remove tudo que não é letra (a-z), número (0-9) ou espaço (\s)
    text_str = re.sub(r'[^a-zA-Z0-9\s]', ' ', text_str).lower()

    # Remove espaços extras (ex: "  texto   livre " -> "texto livre")
    text_str = ' '.join(text_str.split())

    return text_str if text_str else None

def standardize_column_name(col_name):
    """
    Padroniza nomes de colunas para snake_case.
    Ex: 'Valor (R$)' -> 'valor'
    """
    if not col_name:
        return "col_desconhecida"
        
    col_name = str(col_name).lower()
    
    # Substituições de preposições comuns para encurtar o nome
    for prep in [' do ', ' de ', ' da ', ' dos ', ' das ']:
        col_name = col_name.replace(prep, '_')
        
    # Remove caracteres não alfanuméricos e substitui por underscore
    col_name = re.sub(r'[^\w]+', '_', col_name)
    
    # Remove múltiplos underscores e underscores no início/fim
    col_name = re.sub(r'_+', '_', col_name).strip('_')
    
    return col_name

print("✅ Funções 'standardize_text' e 'standardize_column_name' definidas.")

# Registrar UDF (User Defined Function) no Spark
# Isso permite usar a função standardize_text dentro de comandos Spark SQL
standardize_text_udf = spark.udf.register("standardize_text_udf", standardize_text, StringType())

# Verificar se a UDF foi registrada corretamente
registered_udfs = [f.name for f in spark.catalog.listFunctions() if f.name == 'standardize_text_udf']

if "standardize_text_udf" in registered_udfs:
    print("✅ UDF 'standardize_text_udf' registrada com sucesso no Spark.")
else:
    print("❌ Falha ao registrar a UDF 'standardize_text_udf'.")

print("--- Fim da Célula 3 ---")



