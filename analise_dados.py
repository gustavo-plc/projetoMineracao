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


# ==============================================================================
# CÉLULA 4: Padronização de Nomes de Colunas (DataFrame)
# ==============================================================================

def standardize_dataframe_columns(df):
    """
    Aplica padronização nos nomes das colunas do DataFrame.
    Resolve conflitos de nomes duplicados.
    """
    original_columns = df.columns
    proposed_renames = {}

    # Passo 1: Propor novos nomes
    for orig_col_name in original_columns:
        # Limpa o nome para facilitar o match
        clean_name = str(orig_col_name).strip().lower()
        clean_name = ''.join(c for c in unicodedata.normalize('NFKD', clean_name) if not unicodedata.combining(c))

        target_name = None
        
        # Tenta achar no mapeamento oficial
        if clean_name in column_name_mapping:
            target_name = column_name_mapping[clean_name]
        else:
            # Se não achar, padroniza automaticamente
            target_name = standardize_column_name(orig_col_name)
            if not target_name:
                target_name = f"col_fallback_{original_columns.index(orig_col_name)}"

        proposed_renames[orig_col_name] = target_name

    # Passo 2: Resolver colisões (nomes iguais)
    final_renames_map = {}
    name_counts = {}

    for orig_col_name in original_columns:
        target = proposed_renames[orig_col_name]
        
        # Se o nome já existe, adiciona um sufixo numérico (ex: valor_1)
        count = name_counts.get(target, 0)
        if count > 0:
            resolved_name = f"{target}_{count}"
        else:
            resolved_name = target
            
        # Verifica se o nome resolvido também colide (raro, mas possível)
        while resolved_name in final_renames_map.values():
            count += 1
            resolved_name = f"{target}_{count}"

        name_counts[target] = count + 1
        final_renames_map[orig_col_name] = resolved_name

    # Passo 3: Aplicar renomeação no DataFrame
    df_final = df
    for orig, new in final_renames_map.items():
        if orig != new:
            df_final = df_final.withColumnRenamed(orig, new)

    return df_final

print("\n✅ Função 'standardize_dataframe_columns' definida.")


# ==============================================================================
# CÉLULA 5: Processamento de Dados (Limpeza de Valores e Texto)
# ==============================================================================

def process_dataframe(df):
    """
    Limpa e converte os dados das colunas principais.
    """
    df_processed = df

    # 1. Limpeza de Texto (Objeto da Aquisição)
    if "objeto_aquisicao" in df_processed.columns:
        df_processed = df_processed.withColumn(
            "objeto_aquisicao",
            when(
                col("objeto_aquisicao").isNotNull() & (trim(col("objeto_aquisicao")) != ""),
                standardize_text_udf(col("objeto_aquisicao"))
            ).otherwise(lit(None).cast(StringType()))
        )

    # 2. Limpeza de Valor (R$ 1.000,00 -> 1000.00)
    if "valor" in df_processed.columns:
        # Remove tudo que não é número (mantém apenas dígitos)
        # Assume que o valor original no Excel pode vir como texto sujo
        df_processed = df_processed.withColumn(
            "valor_cleaned_str",
            when(
                col("valor").isNotNull(),
                regexp_replace(col("valor").cast(StringType()), r"[^0-9]", "")
            ).otherwise(lit(None))
        )

        # Converte para Decimal (divide por 100 para ajustar centavos, se vier sem vírgula)
        # Nota: Essa lógica assume que "1000" virou "1000" (R$ 10,00).
        # Se o excel lê direto como float, isso pode precisar de ajuste.
        # Para garantir, vamos confiar na conversão direta se já for numérico, ou tratar se for string.
        
        df_processed = df_processed.withColumn(
            "valor",
            when(
                col("valor_cleaned_str").isNotNull() & (col("valor_cleaned_str") != ""),
                (col("valor_cleaned_str").cast(DecimalType(20, 0)) / 100.0).cast(DecimalType(12, 2))
            ).otherwise(lit(None).cast(DecimalType(12, 2)))
        ).drop("valor_cleaned_str")

    # 3. Conversão de Ano
    if "ano" in df_processed.columns:
        df_processed = df_processed.withColumn("ano", col("ano").cast(IntegerType()))

    # 4. Limpeza de CPF/CNPJ (Apenas números)
    for doc_col in ["cpf_suprido", "cpf_cnpj_favorecido"]:
        if doc_col in df_processed.columns:
            df_processed = df_processed.withColumn(
                doc_col,
                when(
                    col(doc_col).isNotNull(),
                    regexp_replace(col(doc_col).cast(StringType()), r"[^0-9]", "")
                ).otherwise(lit(None).cast(StringType()))
            )

    # 5. Seleção Final (Apenas colunas do Schema)
    final_cols = []
    for field in schema_base.fields:
        colname = field.name
        if colname in df_processed.columns:
            final_cols.append(col(colname).cast(field.dataType).alias(colname))
        else:
            # Se a coluna faltar, cria como NULL
            final_cols.append(lit(None).cast(field.dataType).alias(colname))

    return df_processed.select(final_cols)

print("✅ Função 'process_dataframe' definida.")
print("--- Fim das Células 4 e 5 ---")
