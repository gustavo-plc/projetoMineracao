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

# --- Célula 1: Ajuste de Memória e Paralelismo ---
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
# CÉLULA 2: Mapeamento de Colunas (Estratégia Slugify - Sem acentos/espaços)
# ==============================================================================

# A chave (esquerda) deve ser o nome da coluna:
# 1. Tudo minúsculo
# 2. Sem acentos
# 3. Sem espaços
# Exemplo: "Objeto da Aquisição" vira "objetodaaquisicao"

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
    
    # AQUI ESTAVA O ERRO PRINCIPAL:
    "objetodaaquisicao": "objeto_aquisicao", 
    "motivo": "objeto_aquisicao",
    
    # --- Valores ---
    "valor": "valor_transacao",
    "valortotal": "valor_transacao"
}

# Lista final de colunas desejadas (Ordem do Parquet)
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

print("✅ Dicionário 'Slug' atualizado. Pronto para mapear qualquer variação.")

# ==============================================================================
# CÉLULA 3: Funções de Limpeza (Correção Moeda e N/A)
# ==============================================================================
import re
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pyspark.sql.functions as F

print("\n--- Executando Célula 3: Funções de Limpeza Blindadas ---")

def clean_column_names(df):
    """
    Renomeia colunas para o padrão slug (sem acento, minúsculo, sem espaço).
    """
    new_columns = []
    # Remove acentos
    accents_src = 'áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ'
    accents_tgt = 'aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN'
    
    for col_name in df.columns:
        clean = col_name
        # Transliteração manual simples para Spark/Python misto
        trans_table = str.maketrans(accents_src, accents_tgt)
        clean = clean.translate(trans_table).lower()
        
        # Remove tudo que não é letra ou número (slug)
        clean_slug = re.sub(r'[^a-z0-9]', '', clean)
        
        # Busca no dicionário
        final_name = SCHEMA_COLUMNS_MAP.get(clean_slug)
        if not final_name: final_name = clean_slug 
            
        new_columns.append(F.col(f"`{col_name}`").alias(final_name))
    
    return df.select(*new_columns)

def process_dataframe(df):
    """
    Aplica limpeza nos dados e garante o Schema final.
    """
    df = clean_column_names(df)
    
    # 1. Limpeza de Texto (Objeto, Nomes)
    src_chars = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
    tgt_chars = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"
    
    def clean_text_expr(col_name):
        # Remove acentos e caracteres especiais, mas mantém letras, números e espaços
        # Transforma "N/A" em "na"
        return F.trim(F.regexp_replace(F.translate(F.lower(F.col(col_name)), src_chars, tgt_chars), r"[^a-z0-9\s]", ""))

    # 2. VALOR (A GRANDE CORREÇÃO)
    # Lógica:
    # Passo A: Remove TUDO que não for dígito (0-9), vírgula (,) ou sinal de menos (-).
    #          Isso elimina "R$", ".", espaços, caracteres invisíveis.
    #          Ex: "R$ 1.200,50" -> "1200,50"
    #          Ex: "R$    12,60" -> "12,60"
    # Passo B: Troca a vírgula por ponto ("1200.50")
    # Passo C: Converte para Double
    val_clean_expr = F.regexp_replace(
        F.regexp_replace(F.col("valor_transacao").cast("string"), r"[^0-9,-]", ""), 
        ",", "."
    ).cast(DoubleType())

    # 3. CPF/CNPJ
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

print("✅ Funções de limpeza corrigidas (Regex de Valor por Allowlist).")


# ==============================================================================
# CÉLULA 4: Execução do Pipeline (Leitura -> Correção -> Gravação)
# ==============================================================================
from functools import reduce

print("\n--- Executando Célula 4: Reprocessamento ---")

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
            # Forçamos inferSchema=False e lemos tudo como String primeiro para evitar erro de tipo
            # Isso é mais seguro para a limpeza manual que fazemos na Célula 3
            df_raw = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .load(path_file)
            
            df_clean = process_dataframe(df_raw)
            
            # Garante coluna ano
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


from pyspark.sql.functions import col, when, lit, trim

print("\n--- Executando Célula 5: Consolidação Final (Gold) ---")

input_parquet_path = os.path.join(output_base_path, "final")
output_consolidado = os.path.join(BASE_DIR, "Consolidado_Final")

try:
    df_full = spark.read.option("basePath", input_parquet_path).parquet(input_parquet_path)
    
    total_bruto = df_full.count()
    print(f"✅ Total Bruto Carregado: {total_bruto}")
    
    # 1. TRATAMENTO DE CAMPOS VAZIOS (Para não perder dinheiro real)
    # Se o Objeto for nulo, vazio ou "na", vira "NAO INFORMADO"
    # Assim salvamos os registros da Leroy Merlin/Drogaria SP que estavam sem descrição
    df_treated = df_full.withColumn(
        "objeto_aquisicao",
        when(
            col("objeto_aquisicao").isNull() | 
            (trim(col("objeto_aquisicao")) == "") | 
            (col("objeto_aquisicao") == "na"), 
            lit("NAO INFORMADO")
        ).otherwise(col("objeto_aquisicao"))
    )

    # 2. FILTRO FINANCEIRO (Obrigatório ter Valor)
    # Agora só descartamos se não tiver VALOR. Se tiver valor, a gente guarda.
    df_gold = df_treated.filter(
        col("valor_transacao").isNotNull() & 
        (col("valor_transacao") > 0)
    )
    
    total_liquido = df_gold.count()
    descartados = total_bruto - total_liquido
    
    print(f"✅ Total Válido Final: {total_liquido}")
    print(f"🚮 Descartados (Sem Valor / Lixo Excel): {descartados}")
    
    # 3. SALVAMENTO
    print(f"💾 Salvando Dataset Consolidado em: {output_consolidado}")
    
    df_gold.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_consolidado)
        
    print("✅ Consolidação concluída!")
    
    # 4. PROVA DOS 9
    print("\n--- Verificando registros recuperados (Ex: Leroy Merlin/Drogaria) ---")
    df_gold.filter(col("objeto_aquisicao") == "NAO INFORMADO").select("ano", "valor_transacao", "nome_favorecido").show(5, truncate=False)

except Exception as e:
    print(f"❌ Erro na consolidação: {e}")