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
        df.show(5, truncate=True)
        
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo Parquet: {e}")
else:
    print(f"❌ ARQUIVO NÃO ENCONTRADO. Verifique se a Fase 1 gerou a pasta: {input_path}")

# --- Fim do Script Inicial ---

# ==============================================================================
# CÉLULA 6: NLP - Tokenização e Remoção de Stopwords
# ==============================================================================
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, size, lower

print("--- Iniciando Processamento de Linguagem Natural (NLP) ---")

# 1. Definição de Stopwords (Palavras para remover)
# O Spark tem stopwords padrão em inglês. Para PT-BR + Termos Públicos, precisamos de uma lista customizada.
stopwords_pt_custom = [
    # --- Artigos e Preposições Comuns (PT-BR) ---
    "de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "com", "nao", "uma", "os", "no", 
    "se", "na", "por", "mais", "as", "dos", "como", "mas", "ao", "ele", "das", "seu", "sua", "ou", 
    "quando", "muito", "nos", "ja", "eu", "tambem", "so", "pelo", "pela", "ate", "isso", "ela", 
    "entre", "depois", "sem", "mesmo", "aos", "seus", "quem", "nas", "me", "esse", "eles", "você", 
    "essa", "num", "nem", "suas", "meu", "as", "minha", "numa", "pelos", "elas", "qual", "nos", 
    "lhe", "deles", "essas", "esses", "pelas", "este", "dele", 
    
    # --- Termos Genéricos de Compras Públicas (Ruído para Clusterização) ---
    # Queremos agrupar "Caneta", não agrupar todo mundo que escreveu "Aquisição"
    "aquisicao", "referente", "pagamento", "despesa", "servico", "servicos", "material", 
    "fornecimento", "nf", "nfs", "nota", "fiscal", "cupom", "valor", "pgto", "compra", 
    "consumo", "suprimento", "fundo", "recurso", "objeto", "item", "unidade", "unid", 
    "cx", "pct", "pc", "kg", "litro", "litros", "qtd", "quantidade", "nao", "informado",
    "prestacao", "manutencao", "uso", "aplicacao"
]

print(f"Lista de Stopwords carregada com {len(stopwords_pt_custom)} termos.")

try:
    # 2. Tokenização (Transforma frase "compra de caneta" em vetor ["compra", "de", "caneta"])
    # Nota: O inputCol deve ser o texto limpo da Fase 1 ("objeto_aquisicao")
    tokenizer = Tokenizer(inputCol="objeto_aquisicao", outputCol="words_raw")
    df_tokenized = tokenizer.transform(df)

    # 3. Remoção de Stopwords
    remover = StopWordsRemover(inputCol="words_raw", outputCol="words_filtered")
    # Carrega nossa lista customizada
    remover.setStopWords(stopwords_pt_custom)
    
    df_clean_nlp = remover.transform(df_tokenized)

    # 4. Limpeza Final (Opcional, mas boa para performance)
    # Removemos palavras vazias que podem ter sobrado e linhas que ficaram vazias após limpar
    # Ex: Se a descrição era "Pagamento de Despesa", sobrou [], isso não serve para clusterizar.
    df_final_nlp = df_clean_nlp.filter(size(col("words_filtered")) > 0)

    print("✅ Tokenização e Limpeza concluídas.")
    print(f"Total de registros prontos para vetorização: {df_final_nlp.count()}")

    print("\n--- Comparativo: Antes vs Depois (Top 10) ---")
    df_final_nlp.select("objeto_aquisicao", "words_filtered").show(10, truncate=False)

except Exception as e:
    print(f"❌ Erro no NLP: {e}")

# ==============================================================================
# CÉLULA 7: Vetorização (CountVectorizer + IDF)
# ==============================================================================
from pyspark.ml.feature import CountVectorizer, IDF

print("--- Iniciando Vetorização (TF-IDF) ---")

try:
    # 1. CountVectorizer (Calcula a Frequência do Termo - TF)
    # vocabSize: Limita o vocabulário às 10.000 palavras mais comuns (evita ruído excessivo)
    # minDF: Ignora palavras que aparecem em menos de 2 documentos (erros de digitação únicos)
    cv = CountVectorizer(inputCol="words_filtered", outputCol="raw_features", vocabSize=10000, minDF=2.0)
    
    cv_model = cv.fit(df_final_nlp)
    df_tf = cv_model.transform(df_final_nlp)
    
    print(f"Vocabulário aprendido: {len(cv_model.vocabulary)} palavras únicas.")

    # 2. IDF (Inverse Document Frequency - Dá peso à raridade)
    idf = IDF(inputCol="raw_features", outputCol="features")
    idf_model = idf.fit(df_tf)
    df_tfidf = idf_model.transform(df_tf)

    print("✅ Vetorização concluída.")
    
    # Mostra um exemplo do vetor gerado
    # O vetor é "esparso": (Tamanho, [Indices das palavras], [Pesos TF-IDF])
    print("\n--- Amostra do Vetor Matemático (Features) ---")
    df_tfidf.select("words_filtered", "features").show(5, truncate=80)

except Exception as e:
    print(f"❌ Erro na Vetorização: {e}")


# ==============================================================================
# CÉLULA 8: Definição do K Ideal (Método do Cotovelo Otimizado)
# ==============================================================================
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
import numpy as np

print("--- Iniciando Método do Cotovelo (Otimizado) ---")

# 1. Performance: Coloca os dados vetorizados na memória
# Se não fizer isso, o Spark reprocessa o TF-IDF a cada loop (Lento!)
df_tfidf.cache()
print(f"Dados em cache. Total de registros: {df_tfidf.count()}")

costs = []
ks = range(2, 13)  # Testa de 2 a 12 grupos

try:
    print("Calculando custos (Inércia)...")
    
    for k in ks:
        # Treina K-Means
        kmeans = KMeans(featuresCol="features", k=k, seed=42)
        model = kmeans.fit(df_tfidf)
        
        # Obtém o custo (Soma dos erros quadráticos)
        cost = model.summary.trainingCost
        costs.append(cost)
        print(f"   > k={k} : Custo={cost:,.0f}")

    # 2. Inteligência: Tenta achar o 'Cotovelo' matematicamente
    # A ideia é achar o ponto onde a curva 'dobra' mais forte (maior distância da linha reta)
    # ou onde a redução do erro desacelera.
    
    # Cálculo simples da derivada (redução percentual)
    diffs = [costs[i] - costs[i+1] for i in range(len(costs)-1)]
    
    # Sugere o K onde a redução começa a ficar marginal (menos de 10% do drop inicial)
    drop_inicial = diffs[0]
    k_sugerido = 0
    
    print("\n--- Análise de Redução de Erro ---")
    for i, drop in enumerate(diffs):
        k_atual = ks[i+1] # O drop é calculado indo para esse K
        ratio = drop / drop_inicial
        print(f"   Mudar para k={k_atual}: Reduz o erro em {drop:,.0f} ({ratio:.1%} do inicial)")
        
        # Regra de ouro: Se o ganho for menor que 20% do ganho inicial, paramos.
        if ratio < 0.20 and k_sugerido == 0:
            k_sugerido = k_atual

    if k_sugerido == 0: k_sugerido = 7 # Fallback se a regra falhar
    
    print(f"\n💡 K Sugerido Matematicamente: {k_sugerido}")

    # 3. Visualização
    plt.figure(figsize=(10, 6))
    plt.plot(ks, costs, 'bo-', label='Custo (Inércia)')
    
    # Marca o ponto sugerido
    idx_sug = ks.index(k_sugerido)
    plt.plot(k_sugerido, costs[idx_sug], 'ro', markersize=12, label=f'Cotovelo Sugerido (k={k_sugerido})')
    
    plt.title(f'Método do Cotovelo (Melhor k ~ {k_sugerido})')
    plt.xlabel('Número de Clusters (k)')
    plt.ylabel('Custo (Soma dos Erros Quadráticos)')
    plt.grid(True)
    plt.legend()
    
    plt.savefig("grafico_cotovelo_otimizado.png")
    print("📊 Gráfico salvo como 'grafico_cotovelo_otimizado.png'")
    plt.show()

except Exception as e:
    print(f"❌ Erro no cálculo: {e}")

    # ==============================================================================
# CÉLULA 9: Aplicação do K-Means (k=6)
# ==============================================================================
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col

# Configuração Definida
K_FINAL = 6

print(f"--- Aplicando K-Means Definitivo (k={K_FINAL}) ---")

try:
    # 1. Treinamento
    kmeans = KMeans(featuresCol="features", k=K_FINAL, seed=1, predictionCol="prediction")
    model = kmeans.fit(df_tfidf)
    
    # 2. Predição (Adiciona a coluna 'prediction' com o nº do grupo)
    df_clustered = model.transform(df_tfidf)
    
    print("✅ Clusterização concluída.")
    print(f"Total de registros classificados: {df_clustered.count()}")

except Exception as e:
    print(f"❌ Erro no K-Means: {e}")