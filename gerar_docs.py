import os

# Conteúdo da documentação
conteudo_docs = """# 📄 Documentação Técnica: Pipeline de ETL Local (PySpark)

**Projeto:** Mineração de Dados de Gastos Públicos (Cartão Corporativo)
**Ambiente:** Local (Windows 11 / VS Code)
**Tecnologia:** Python 3.11 + PySpark 3.5.3 (Single Node)
**Escopo:** Configuração até Célula 5 (Pré-processamento)

## Visão Geral
Este script realiza a Extração, Transformação e Carga (ETL) de arquivos de despesas públicas (originalmente em `.ods`/`.xlsx`). O objetivo é normalizar dados heterogêneos provenientes de diferentes anos, limpar inconsistências de texto e valores, e preparar o dataset para análise em formato colunar (Parquet).

---

## 🛠️ Detalhamento por Célula

### Célula 1: Configuração de Ambiente e Inicialização
**Objetivo:** Preparar o sistema operacional e instanciar o motor de processamento distribuído (Spark) em modo local.

* **Correção de Ambiente Windows:**
    * Define variáveis `PYSPARK_PYTHON` e `PYSPARK_DRIVER_PYTHON` apontando para o executável do `venv`. Isso previne o erro onde o Spark tenta invocar o Python global ou da Microsoft Store.
* **Gestão de Diretórios:**
    * Utiliza `os.getcwd()` e `os.path.join` para definir caminhos relativos (`dados/input`, `dados/Parquet`). Isso torna o projeto portável.
    * Cria diretórios automaticamente com `os.makedirs`.
* **Instanciação da `SparkSession`:**
    * **Versão:** Configurada para PySpark 3.5.3 (LTS) para estabilidade no Windows.
    * **Dependência Excel:** Carrega o pacote `com.crealytics:spark-excel_2.12:3.5.0_0.20.3` via Maven para leitura nativa de planilhas.
    * **Configurações de Rede:** Define `spark.driver.bindAddress` como `127.0.0.1` para evitar erros de *heartbeat* e queda de conexão (WinError 10054).
    * **Compatibilidade:** Define modos `LEGACY` para escrita de datas em Parquet.

### Célula 2: Definição de Metadados (Schema e Mapeamento)
**Objetivo:** Estabelecer as regras de negócio para a estrutura dos dados.

* **Dicionário de Mapeamento (`column_name_mapping`):**
    * Atua como um "tradutor" (De -> Para). Resolve o problema de variação de nomenclatura nos arquivos governamentais ao longo dos anos (ex: "Motivo" -> "Objeto da Aquisição").
* **Tipagem Forte (`schema_base`):**
    * Define explicitamente o tipo de dado esperado (`StructType`).
    * `valor`: Decimal(12,2) para precisão financeira.
    * `ano`: IntegerType.
    * `texto`: StringType.

### Célula 3: Funções de Limpeza (UDFs)
**Objetivo:** Criar funções puras de Python para higienização de strings e registrá-las no motor do Spark.

* **Normalização de Texto (`standardize_text`):**
    * Remove acentos (Normalização Unicode NFKD).
    * Remove caracteres especiais (mantém apenas alfanuméricos).
    * Converte tudo para minúsculas (`lower()`).
* **Padronização de Colunas (`standardize_column_name`):**
    * Converte nomes de colunas para o padrão *snake_case* (ex: `Valor Total` -> `valor_total`).
* **Registro de UDF:**
    * A função `standardize_text` é registrada como uma **UDF (User Defined Function)**, permitindo aplicação distribuída em DataFrames.

### Célula 4: Padronização Dinâmica de Colunas
**Objetivo:** Garantir que o DataFrame tenha os nomes de colunas corretos antes de tentar processar os dados.

* **Lógica de Renomeação:**
    * Itera sobre as colunas do arquivo bruto.
    * Verifica se o nome existe no dicionário de mapeamento (Célula 2).
    * Se não existir, aplica uma padronização genérica (*snake_case*).
* **Tratamento de Colisão:**
    * Implementa um algoritmo para detectar nomes duplicados e adiciona sufixos numéricos automaticamente (`valor_1`, `valor_2`) para evitar conflitos no Spark.

### Célula 5: Processamento e Transformação (ETL)
**Objetivo:** Aplicar as regras de limpeza nos dados brutos e garantir a conformidade com o schema final.

* **Limpeza de Texto:** Aplica a UDF `standardize_text` na coluna `objeto_aquisicao`.
* **Limpeza Financeira (Coluna `valor`):** Remove caracteres não numéricos ("R$", pontos) e converte para `Decimal`.
* **Limpeza de Documentos (CPF/CNPJ):** Remove pontuação (pontos, traços), mantendo apenas dígitos.
* **Seleção Final:** Reordena as colunas de acordo com o `schema_base` e cria colunas `NULL` caso alguma coluna esperada não exista no arquivo original.

---

## 🏗️ Decisões de Arquitetura (Ambiente Windows)

1.  **Downgrade para Spark 3.5.3:** A versão 4.0.1 mostrou-se instável no Windows. A versão 3.5.3 (LTS) garantiu estabilidade.
2.  **Conversão Prévia (ODS -> XLSX):** Utilizou-se `odfpy` + `pandas` para converter arquivos OpenDocument, pois a leitura nativa do Spark para ODS é limitada.
3.  **Winutils:** Configuração do binário `winutils.exe` (Hadoop 3.3.5) para emular o sistema de arquivos HDFS no Windows.
"""

nome_arquivo = "documentacao_tecnica_ate_celula_5.md"
caminho_completo = os.path.join(os.getcwd(), nome_arquivo)

try:
    with open(caminho_completo, "w", encoding="utf-8") as f:
        f.write(conteudo_docs)
    print(f"✅ Arquivo gerado com sucesso!")
    print(f"📂 Local: {caminho_completo}")
    print("💡 Dica: Abra este arquivo no VS Code e pressione 'Ctrl + Shift + V' para visualizar formatado.")
except Exception as e:
    print(f"❌ Erro ao gerar arquivo: {e}")