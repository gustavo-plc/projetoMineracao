import os
from pyspark.sql import SparkSession

# --- 3. Configuração de Diretórios Locais (TRECHO SOLICITADO) ---
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
print("-" * 50)

def ler_schemas_pyspark():
    # 1. Configurar a Sessão Spark com suporte a Excel
    # O pacote com.crealytics:spark-excel é necessário.
    print("Iniciando Sessão Spark (isso pode demorar na primeira vez para baixar o JAR)...")
    
    spark = SparkSession.builder \
        .appName("VerificadorDeSchemaExcel") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.5.0_0.20.3") \
        .getOrCreate()
    
    # 2. Iterar sobre os anos configurados
    for ano in anos_a_processar:
        caminho_ano = os.path.join(input_base_path, ano)
        
        # Verificar se a pasta do ano existe
        if not os.path.exists(caminho_ano):
            print(f"[AVISO] Pasta do ano {ano} não encontrada em: {caminho_ano}")
            continue
            
        print(f"\n>>> Processando arquivos do ano: {ano}")
        
        arquivos = [f for f in os.listdir(caminho_ano) if f.endswith(('.xlsx', '.xls'))]
        
        if not arquivos:
            print(f"   Nenhum arquivo Excel encontrado em {caminho_ano}")
            continue
            
        for arquivo in arquivos:
            caminho_completo = os.path.join(caminho_ano, arquivo)
            print(f"   Arquivo: {arquivo}")
            
            try:
                # 3. Ler o arquivo Excel
                # Removemos o 'dataAddress' fixo para ele pegar a primeira aba ativa automaticamente
                df = spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load(caminho_completo)

                # 4. Imprimir o Schema
                print("   Schema detectado:")
                df.printSchema()
                print("." * 30)

            except Exception as e:
                print(f"   [ERRO] Falha ao ler {arquivo}: {e}")

    spark.stop()
    print("\nProcesso finalizado.")

if __name__ == "__main__":
    ler_schemas_pyspark()