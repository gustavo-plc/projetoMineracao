import os
import pandas as pd
import sys

# Configuração
BASE_DIR = os.path.join(os.getcwd(), "dados", "input")

print(f"--- Iniciando Conversor de ODS/ODT para XLSX ---")
print(f"Procurando na pasta: {BASE_DIR}")

arquivos_convertidos = 0
erros = 0

# Varre todas as pastas e subpastas
for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if file.lower().endswith(('.ods', '.odt')):
            caminho_origem = os.path.join(root, file)
            caminho_destino = os.path.splitext(caminho_origem)[0] + ".xlsx"
            
            # Se o xlsx já existe, pula (para não perder tempo)
            if os.path.exists(caminho_destino):
                print(f"Ignorando (já existe): {file}")
                continue

            print(f"Convertendo: {file} ...", end="")
            
            try:
                # Tenta ler como planilha (funciona para .ods)
                # Se for .odt, o pandas geralmente não lê direto, precisa de tratamento especial
                if file.lower().endswith('.odt'):
                     print(" [AVISO: .odt detectado. Se falhar, verifique se é uma tabela mesmo] ", end="")
                
                # Lê o arquivo usando a engine 'odf'
                df = pd.read_excel(caminho_origem, engine="odf")
                
                # Salva como Excel padrão
                df.to_excel(caminho_destino, index=False)
                print(" OK! ✅")
                arquivos_convertidos += 1
                
                # Opcional: Remover o arquivo original para não duplicar no Spark depois
                # os.remove(caminho_origem) 
                
            except Exception as e:
                print(f" ❌ ERRO: {e}")
                erros += 1

print("------------------------------------------------")
print(f"Processo finalizado.")
print(f"Convertidos com sucesso: {arquivos_convertidos}")
print(f"Falhas: {erros}")