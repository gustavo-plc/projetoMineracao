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
# CÉLULAS 3, 4 e 5 (CORRIGIDAS E FINALIZADAS): Processamento Nativo Seguro
# ==============================================================================

print("\n--- Executando Processamento Otimizado (Native Spark) ---")

import re

def clean_column_names(df):
    """
    Renomeia colunas removendo acentos e padronizando (Python-side).
    Garante que nomes de colunas como 'Descrição' virem 'descricao'.
    """
    new_columns = []
    existing_names = set()
    
    # Mapeamento completo de acentos para nomes de colunas
    accents_src = 'áàâãäéèêëíìîïóòôõöúùûüçñ'
    accents_tgt = 'aaaaaeeeeiiiiooooouuuucn'
    
    for col_name in df.columns:
        clean = col_name.strip().lower()
        
        # Remove acentos do nome da coluna
        for src, tgt in zip(accents_src, accents_tgt):
            clean = clean.replace(src, tgt)
        
        # Verifica mapeamento oficial (definido na Célula 2)
        final_name = column_name_mapping.get(clean)
        
        if not final_name:
            # Remove qualquer coisa que não seja letra ou número (snake_case)
            final_name = re.sub(r'[^a-z0-9]+', '_', clean).strip('_')
            if not final_name: final_name = f"col_{df.columns.index(col_name)}"
        
        # Resolve conflitos de nomes iguais
        base_name = final_name
        count = 1
        while final_name in existing_names:
            final_name = f"{base_name}_{count}"
            count += 1
            
        existing_names.add(final_name)
        new_columns.append(col(f"`{col_name}`").alias(final_name))
    
    return df.select(*new_columns)

def process_dataframe(df):
    """
    Aplica limpeza nos dados usando funções nativas do Spark.
    CORREÇÃO FINAL: Garante substituição de todos os acentos antes da limpeza de símbolos.
    """
    # 1. Padroniza nomes das colunas
    df = clean_column_names(df)
    
    # 2. Definição da limpeza de Texto (Objeto da Aquisição)
    
    # Lista completa de caracteres acentuados do Português
    # O Spark vai procurar qualquer caractere da primeira string e trocar pelo correspondente na segunda.
    src_chars = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
    tgt_chars = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"
    
    # Passo A: Converte para minúsculo
    txt_lower = F.lower(F.col("objeto_aquisicao"))
    
    # Passo B: Troca acentos por letras normais (ç->c, ã->a, é->e...)
    txt_translated = F.translate(txt_lower, src_chars, tgt_chars)
    
    # Passo C: Remove caracteres que não são letras(a-z), números(0-9) ou espaço
    # Isso elimina traços, pontos, parênteses, etc.
    txt_clean_expr = F.trim(F.regexp_replace(txt_translated, r"[^a-z0-9\s]", ""))
    
    # Limpeza de Valor (R$ 1.000,00 -> 1000.00)
    val_clean_expr = (
        F.regexp_replace(F.col("valor").cast("string"), r"[^0-9]", "").cast(DecimalType(20,0)) / 100.0
    ).cast(DecimalType(12,2))

    # Limpeza de CPF/CNPJ (Remove pontuação)
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

print("✅ Funções otimizadas (Correção total de acentuação: ã, ç, é -> a, c, e) definidas.")
print("--- Fim das Células 4 e 5 ---")

# ==============================================================================
# CÉLULA 6 (CORRIGIDA): Leitura Inteligente de Abas + Conversão
# ==============================================================================

print("\n--- Executando Célula 6: Conversão XLS -> Parquet ---")

# Importação necessária para ler nomes de abas
import pandas as pd

total_arquivos = 0
sucessos = 0
erros = {}

if 'anos_a_processar' not in locals():
    anos_a_processar = [str(ano) for ano in range(2016, 2026)]

print(f"Processando período: {min(anos_a_processar)} a {max(anos_a_processar)}")

for ano in sorted(anos_a_processar):
    caminho_origem_ano = os.path.join(input_base_path, ano)
    caminho_destino_ano = os.path.join(output_base_path, ano)
    
    print(f"\n📂 Processando ano: {ano}")
    
    if not os.path.exists(caminho_origem_ano):
        print(f"   ⚠️ Pasta não encontrada: {caminho_origem_ano}")
        continue
        
    arquivos_ano = [
        f for f in os.listdir(caminho_origem_ano) 
        if f.lower().endswith(('.xlsx', '.xls')) and not f.startswith('~$')
    ]
    
    if not arquivos_ano:
        print(f"   ℹ️ Nenhum arquivo Excel na pasta {ano}.")
        continue
        
    os.makedirs(caminho_destino_ano, exist_ok=True)
    
    for arquivo in arquivos_ano:
        total_arquivos += 1
        nome_sem_extensao = os.path.splitext(arquivo)[0]
        path_origem = os.path.join(caminho_origem_ano, arquivo)
        path_destino = os.path.join(caminho_destino_ano, nome_sem_extensao)
        
        print(f"   🔄 {arquivo} ... ", end="")
        
        try:
            # ESTRATÉGIA NOVA: Descobrir o nome da aba com Pandas (rápido e seguro)
            # O Pandas lê apenas os metadados, não o arquivo todo, então é rápido.
            xl = pd.ExcelFile(path_origem)
            nome_primeira_aba = xl.sheet_names[0]
            
            # Agora mandamos o Spark ler exatamente essa aba
            df_raw = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("treatEmptyValuesAsNulls", "true") \
                .option("dataAddress", f"'{nome_primeira_aba}'!A1") \
                .load(path_origem)

            if len(df_raw.columns) == 0 or df_raw.rdd.isEmpty():
                print("⚠️ VAZIO")
                erros[arquivo] = "Arquivo vazio ou sem colunas"
                continue

            # Processamento
            df_final = process_dataframe(df_raw)
            
            # Gravação
            df_final.write.mode("overwrite").option("compression", "snappy").parquet(path_destino)
            
            print("✅ OK")
            sucessos += 1
            
        except Exception as e:
            msg_erro = str(e).split('\n')[0][:100]
            print(f"❌ FALHA ({msg_erro}...)")
            erros[arquivo] = str(e)

print("\n" + "="*40)
print(f"RELATÓRIO FINAL: {sucessos}/{total_arquivos} arquivos.")
if erros:
    print(f"Falhas: {len(erros)}")
    # Salva log de erros em arquivo para facilitar debug
    with open("erros_conversao.log", "w") as f:
        for arq, msg in erros.items():
            f.write(f"{arq}: {msg}\n")
    print("Detalhes salvos em 'erros_conversao.log'")
print("="*40)


# ==============================================================================
# CÉLULA 7 (FINAL BLINDADA): Consolidação com Schema Explícito
# ==============================================================================

print("\n--- Executando Célula 7: Consolidação dos Dados ---")

import glob

try:
    # 1. Encontrar todas as subpastas de dados (Ano -> Arquivo)
    # Padrão: dados/Parquet/20*/despesas_*
    padrao_busca = os.path.join(output_base_path, "20*", "*")
    candidatos = glob.glob(padrao_busca)
    
    # 2. Filtrar apenas pastas que contêm arquivos .parquet válidos
    pastas_validas = []
    print("Verificando integridade das pastas...")
    
    for pasta in candidatos:
        # Verifica se tem algum arquivo terminando em .parquet dentro
        tem_parquet = any(f.endswith('.parquet') for f in os.listdir(pasta))
        if tem_parquet:
            pastas_validas.append(pasta)
    
    if not pastas_validas:
        raise Exception(f"Nenhuma pasta válida com arquivos Parquet encontrada em: {output_base_path}")
        
    print(f"Pastas válidas encontradas: {len(pastas_validas)}")
    
    # 3. Leitura com Schema FORÇADO
    # Ao passar 'schema=schema_base', o Spark não tenta adivinhar nada, ele apenas lê.
    # Isso resolve o erro UNABLE_TO_INFER_SCHEMA e é muito mais rápido.
    df_consolidado = spark.read \
        .schema(schema_base) \
        .option("mergeSchema", "false") \
        .parquet(*pastas_validas)
    
    total_registros = df_consolidado.count()
    print(f"✅ Leitura concluída. Total de registros: {total_registros}")

    # 4. Filtragem de Qualidade
    print("Aplicando filtros de qualidade...")
    
    df_filtered = df_consolidado.filter(
        F.col("valor").isNotNull() & 
        (F.col("valor") > 0) & 
        F.col("objeto_aquisicao").isNotNull() & 
        (F.trim(F.col("objeto_aquisicao")) != "")
    )
    
    total_filtrado = df_filtered.count()
    print(f"Registros válidos: {total_filtrado}")
    print(f"Descartados: {total_registros - total_filtrado}")

    # 5. Salvamento Final
    path_consolidado = os.path.join(BASE_DIR, "Consolidado")
    print(f"Salvando consolidado em: {path_consolidado}")
    
    # Removemos .coalesce(1) se o arquivo for muito grande, mas para 100k linhas é seguro
    df_filtered.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(path_consolidado)
        
    print("✅ Consolidação concluída com sucesso!")
    
    # 6. Amostra
    print("\n--- Amostra dos Dados Finais ---")
    df_filtered.select("ano", "valor", "objeto_aquisicao").show(5, truncate=80)

except Exception as e:
    print(f"❌ Erro na consolidação: {e}")
    import traceback
    traceback.print_exc()

print("--- Fim da Célula 7 ---")