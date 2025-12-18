# ==============================================================================
# DIAGNÓSTICO FINAL (AUTÔNOMO): As linhas perdidas são Lixo ou Ouro?
# ==============================================================================
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

print("--- Iniciando Diagnóstico de Perdas (Foco em 2019) ---")

# 1. Configuração de Caminhos (Recriando contexto)
BASE_DIR = os.path.join(os.getcwd(), "dados")
output_base_path = os.path.join(BASE_DIR, "Parquet")
input_parquet_path = os.path.join(output_base_path, "final")
path_2019 = os.path.join(input_parquet_path, "ano_partition=2019")

# 2. Iniciar Spark
spark = SparkSession.builder \
    .appName("Diagnostico_Final_2019") \
    .config("spark.sql.caseSensitive", "false") \
    .getOrCreate()

if os.path.exists(path_2019):
    try:
        df_2019 = spark.read.parquet(path_2019)
        total = df_2019.count()
        
        # 1. Caso "Linha Fantasma" (LIXO): Valor E Objeto são nulos/vazios/na
        # Se isso for alto, é apenas sujeira do Excel (linhas em branco formatadas).
        df_lixo = df_2019.filter(
            (col("valor_transacao").isNull() | (col("valor_transacao") == 0)) &
            (col("objeto_aquisicao").isNull() | (col("objeto_aquisicao") == "") | (col("objeto_aquisicao") == "na"))
        )
        qtd_lixo = df_lixo.count()
        
        # 2. Caso "Perda Real" (ERRO): Tem Valor MAS não tem Objeto (ou vice-versa)
        # Esses são os preocupantes: perdemos a descrição de um gasto real ou o valor de uma descrição real.
        df_erro_real = df_2019.filter(
            # Cenário A: Tem Dinheiro, mas Objeto é ruim
            (col("valor_transacao").isNotNull() & (col("valor_transacao") > 0) & 
             (col("objeto_aquisicao").isNull() | (col("objeto_aquisicao") == "") | (col("objeto_aquisicao") == "na")))
            |
            # Cenário B: Tem Objeto, mas Dinheiro é ruim
            ((col("objeto_aquisicao").isNotNull() & (col("objeto_aquisicao") != "") & (col("objeto_aquisicao") != "na")) &
             (col("valor_transacao").isNull() | (col("valor_transacao") == 0)))
        )
        qtd_erro_real = df_erro_real.count()

        print(f"\nANÁLISE PROFUNDA DE 2019 (Total Bruto: {total})")
        print(f"--------------------------------------------------")
        print(f"🗑️  Linhas vazias/inúteis (Lixo Excel): {qtd_lixo} ({(qtd_lixo/total)*100:.1f}%)")
        print(f"⚠️  Erros Reais (Dados parciais):       {qtd_erro_real} ({(qtd_erro_real/total)*100:.1f}%)")
        print(f"✅  Dados Válidos (Sobra Líquida):      {total - qtd_lixo - qtd_erro_real}")
        print(f"--------------------------------------------------")

        if qtd_erro_real > 0:
            print("\n--- Amostra dos ERROS REAIS (Investigar Padrão) ---")
            # Mostra o que tem erro parcial para vermos se dá para salvar
            df_erro_real.select("ano", "valor_transacao", "objeto_aquisicao", "nome_favorecido").show(10, truncate=False)
        else:
            print("\n🎉 EXCELENTE! Nenhuma perda real detectada! Tudo que foi descartado era lixo do Excel.")

    except Exception as e:
        print(f"❌ Erro ao ler Parquet: {e}")
else:
    print(f"❌ Pasta de 2019 não encontrada em: {path_2019}")
    print("   Verifique se a Célula 4 rodou e gerou os arquivos.")

print("\n--- Fim do Diagnóstico ---")