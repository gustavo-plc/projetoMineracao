# ==============================================================================
# FASE 2: Análise Exploratória de Dados (EDA) e Mineração (Completo)
# Arquivo: analise_fase2.py
# ==============================================================================

import os
import sys
import matplotlib.pyplot as plt
import numpy as np

# Importações Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, size, lower, avg, stddev, abs as _abs, round as _round, max as _max, min as _min, count
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import KMeans

# --- 1. Configuração de Ambiente (Windows) ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print("--- Iniciando Fase 2: Análise Exploratória ---")

# --- 2. Inicializando Sessão Spark ---
spark = SparkSession.builder \
    .appName("Analise_Gastos_Fase2") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .master("local[*]") \
    .getOrCreate()

# Otimização Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.sparkContext.setLogLevel("WARN")

# --- 3. Carregamento dos Dados ---
BASE_DIR = os.path.join(os.getcwd(), "dados")
input_path = os.path.join(BASE_DIR, "Consolidado_Final")

print(f"📂 Buscando base consolidada em: {input_path}")

if not os.path.exists(input_path):
    print(f"❌ ARQUIVO NÃO ENCONTRADO: {input_path}")
    sys.exit() # Encerra se não achar o arquivo

try:
    df = spark.read.parquet(input_path)
    df.cache() # Cache do dataset bruto
    print(f"✅ Base carregada: {df.count()} registros.")
except Exception as e:
    print(f"❌ Erro leitura: {e}")
    sys.exit()


# ==============================================================================
# CORREÇÃO CRÍTICA: Remoção de Duplicatas
# ==============================================================================
print(f"\n--- Saneamento da Base ---")
print(f"Total Bruto: {df.count()}")

# Remove linhas onde Objeto, Valor e Favorecido são idênticos
# Isso elimina as repetições causadas pela fusão de células no Excel
df = df.dropDuplicates(['objeto_aquisicao', 'valor_transacao', 'nome_favorecido'])

# Força o recálculo e cache na memória
df.cache()
count_real = df.count()

print(f"✅ Total Real (Únicos): {count_real}")
print(f"🗑️ Lixo Removido: {54196 - count_real}")


# ==============================================================================
# CÉLULA 6 (V13): NLP - Remoção de Conectivos e Termos de Ação
# ==============================================================================
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, size, regexp_replace, expr

print("--- Iniciando NLP V13 (Removendo 'Devido', 'Realização' e cia) ---")

# 1. Limpeza de Caracteres
# Como deve ficar (Seguro):
df_clean_chars = df.withColumn("objeto_limpo", regexp_replace(lower(col("objeto_aquisicao")), r"[^a-z]", " "))

# 2. Stopwords
stopwords_pt_custom = [
    # Artigos/Preposições/Pronomes
    "de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "com", "nao", "uma", "os", "no", 
    "se", "na", "por", "mais", "as", "dos", "como", "mas", "ao", "ele", "das", "seu", "sua", "ou", 
    "quando", "muito", "nos", "ja", "eu", "tambem", "so", "pelo", "pela", "ate", "isso", "ela", 
    "entre", "depois", "sem", "mesmo", "aos", "seus", "quem", "nas", "me", "esse", "eles", "você", 
    "foi", "desta", "deste", "pelas", "pelos", "nesta", "neste", 
    
    # NOVOS VILÕES (Detectados no Raio-X do Cluster 5)
    "devido", "ser", "realizacao", "fixacao", "reposicao", "dois", "reparos", "equipamentos", 
    "prr", "covid", "bateria", # Bateria é genérico (carro? pilha?), melhor remover se for vago
    
    # Termos Genéricos / Burocracia
    "aquisicao", "referente", "pagamento", "despesa", "servico", "servicos", "material", 
    "fornecimento", "nf", "nfs", "nota", "fiscal", "cupom", "valor", "pgto", "compra", 
    "consumo", "suprimento", "fundo", "recurso", "objeto", "item", "itens", "unidade", "unid", 
    "cx", "pct", "pc", "kg", "litro", "litros", "qtd", "quantidade", "nao", "informado",
    "prestacao", "manutencao", "uso", "aplicacao", "total", "unitario", "valor",
    "atender", "solicitacao", "pregao", "ata", "registro", "preco", "conforme", "atendimento",
    "razao", "urgente", "disponivel", "utilizado", "pequeno", "grande", "novo", "velho", "aparelho", 
    "oficial", "produtos", 
    
    # Justificativas
    "acabar", "prestes", "falta", "rotina", "emergencia", "urgencia",
    
    # Instituições, Locais e CARGOS
    "pr", "rs", "prm", "dr", "dra", "sr", "sra", "ltda", "me", "epp", "sa", "co", "s/a",
    "prmcruz", "altars", "sede", "prrs", "cnpj", "cpf", "procuradoria", "empresa", "orgao",
    "procurador", "republica", "servidores", "servidor",
    "blumenau", 
    
    # Ações e Adjetivos
    "instalacao", "substituicao", "conserto", "reparo", "troca", "confeccao", "locacao",
    "gabinete", "sala", "almoxarifado", "estoque", "deposito", "setor", "unidades", "andar",
    "materiais", "diversos", "pedagio", "utilizacao", "emergencial", "seguranca", "sistema"
]

try:
    tokenizer = Tokenizer(inputCol="objeto_limpo", outputCol="words_raw")
    df_tokenized = tokenizer.transform(df_clean_chars)

    remover = StopWordsRemover(inputCol="words_raw", outputCol="words_temp")
    remover.setStopWords(stopwords_pt_custom)
    df_clean_temp = remover.transform(df_tokenized)

    # FILTRO SQL (Mantido igual)
    filter_expression = """
        filter(words_temp, x -> 
            x != '' AND 
            length(x) > 2 AND 
            NOT (length(x) == 4 AND substring(x, 1, 2) == 'pr') AND
            substring(x, 1, 6) != 'necess' AND
            substring(x, 1, 6) != 'demand'
        )
    """
    
    df_clean_nlp = df_clean_temp.withColumn("words_filtered", expr(filter_expression))

    df_final_nlp = df_clean_nlp.filter(size(col("words_filtered")) > 0)

    print("✅ NLP V13 concluído.")
    df_final_nlp.select("objeto_aquisicao", "words_filtered").show(5, truncate=False)

except Exception as e:
    print(f"❌ Erro NLP: {e}")

# ==============================================================================
# CÉLULA 7 (V6): Vetorização + Normalização L2
# ==============================================================================
from pyspark.ml.feature import Normalizer

print("\n--- Vetorização V6 (Com Normalização) ---")
try:
    # 1. CountVectorizer (Vocabulário Rico)
    cv = CountVectorizer(inputCol="words_filtered", outputCol="raw_features", 
                         vocabSize=20000, minDF=1.0, maxDF=0.1) 
    cv_model = cv.fit(df_final_nlp)
    df_tf = cv_model.transform(df_final_nlp)
    
    # 2. IDF
    idf = IDF(inputCol="raw_features", outputCol="idf_features") # Mudei nome output
    idf_model = idf.fit(df_tf)
    df_idf = idf_model.transform(df_tf)

    # 3. NORMALIZAÇÃO (O Pulo do Gato para Texto)
    # Transforma os vetores para que todos tenham a mesma "norma" (magnitude).
    # Isso faz o K-Means funcionar baseando-se no ângulo (similaridade de cosseno)
    normalizer = Normalizer(inputCol="idf_features", outputCol="features", p=2.0)
    df_tfidf = normalizer.transform(df_idf)
    
    df_tfidf.cache()
    print("✅ Vetorização e Normalização concluídas.")

except Exception as e:
    print(f"❌ Erro Vetorização: {e}")



# ==============================================================================
# CÉLULA 9 (V2): Clusterização Hierárquica (Bisecting K-Means)
# ==============================================================================
from pyspark.ml.clustering import BisectingKMeans

# Aumentamos K para 20 para garantir que "Copa", "TI" e "Obra" tenham quartos separados
K_FINAL = 20 

print(f"\n--- Aplicando Bisecting K-Means (k={K_FINAL}) ---")
print("Este algoritmo força a divisão do cluster gigante em sub-temas.")

try:
    # minDivisibleClusterSize=100: Garante que ele continue dividindo até chegar no detalhe
    bkmeans = BisectingKMeans(featuresCol="features", k=K_FINAL, seed=1, 
                              predictionCol="prediction", minDivisibleClusterSize=100)
    
    model_final = bkmeans.fit(df_tfidf)
    df_clustered = model_final.transform(df_tfidf)
    
    print(f"✅ Clusterização Hierárquica concluída.")
    
    print("\n--- Nova Distribuição (Agora deve estar equilibrada) ---")
    df_clustered.groupBy("prediction").count().orderBy("prediction").show(25)

except Exception as e:
    print(f"❌ Erro: {e}")

# ==============================================================================
# CÉLULA 10: Detecção de Anomalias (Z-Score) - A PARTE QUE FALTAVA
# ==============================================================================
print("\n--- Detecção de Anomalias (Z-Score) ---")

try:
    w = Window.partitionBy("prediction")

    # Calcula estatísticas do grupo
    df_analise = df_clustered.withColumn("media_grupo", avg("valor_transacao").over(w)) \
                             .withColumn("stddev_grupo", stddev("valor_transacao").over(w))

    # Calcula Z-Score (Quão longe da média o valor está?)
    df_zscore = df_analise.withColumn("z_score", 
                                      (col("valor_transacao") - col("media_grupo")) / col("stddev_grupo"))

    # Filtra Outliers (Z-Score > 3)
    df_outliers = df_zscore.filter(col("z_score") > 3)
    
    qtd_outliers = df_outliers.count()
    print(f"🚩 ALERTA: {qtd_outliers} anomalias detectadas (Z-Score > 3).")
    
    # Formatação para visualização
    df_show = df_outliers.select(
        "prediction", 
        _round("z_score", 2).alias("z_score"), 
        "valor_transacao", 
        _round("media_grupo", 2).alias("media_ref"), 
        "objeto_aquisicao", 
        "nome_favorecido"
    ).orderBy(col("z_score").desc())

    print("\n--- Top 10 Anomalias Críticas ---")
    df_show.show(10, truncate=False)

    # Opcional: Salvar resultado
    # df_show.write.csv("Relatorio_Anomalias.csv", header=True)

except Exception as e:
    print(f"❌ Erro Z-Score: {e}")

print("\n✅ Script Finalizado.")

from pyspark.sql.functions import col, explode, desc


