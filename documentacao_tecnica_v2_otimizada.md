# 📄 Documentação Técnica: Pipeline de ETL Local (PySpark) - Versão Otimizada

**Projeto:** Mineração de Dados de Gastos Públicos (Cartão Corporativo)
**Ambiente:** Local (Windows 11 / VS Code)
**Tecnologia:** Python 3.11 + PySpark 3.5.3 (Single Node)
**Status:** Configuração e Pré-processamento Otimizado (Native Spark)

## Visão Geral
Este script realiza a Extração, Transformação e Carga (ETL) de arquivos de despesas públicas. O fluxo foi projetado para **alta performance local**, substituindo funções Python puras por expressões nativas do Spark (Catalyst Optimizer), eliminando gargalos de serialização.

---

## 🛠️ Detalhamento por Célula

### Célula 1: Configuração de Ambiente e Inicialização
**Objetivo:** Preparar o sistema operacional e instanciar o motor Spark com correções para Windows.

* **Configuração de Ambiente:**
    * Define `PYSPARK_PYTHON` apontando para o `venv` atual, prevenindo conflitos com o Python da Microsoft Store.
* **Gestão de Diretórios:**
    * Utiliza caminhos relativos (`dados/input`) para portabilidade do projeto.
* **Instanciação da `SparkSession`:**
    * **Versão:** PySpark 3.5.3 (LTS) para estabilidade.
    * **Dependência:** Carrega `com.crealytics:spark-excel...` para leitura de planilhas.
    * **Rede:** Fixa `bindAddress` em `127.0.0.1` para evitar erros de conexão (WinError 10054).

### Célula 2: Definição de Metadados (Schema)
**Objetivo:** Estabelecer a tipagem forte dos dados para evitar inferência lenta.

* **Mapeamento:** Dicionário para normalizar nomes de colunas que mudaram ao longo dos anos.
* **Schema:** Define explicitamente `Decimal(12,2)` para valores e `Integer` para anos.

### Células 3, 4 e 5 (Consolidadas): Processamento Otimizado (Native Spark)
**Objetivo:** Limpeza, padronização e transformação dos dados em uma única passagem, utilizando apenas a JVM do Spark.

> **Mudança Arquitetural:** Abandonou-se o uso de **UDFs (User Defined Functions)** em Python.
> **Motivo:** UDFs exigem que o Spark serialize os dados da JVM para o Python e vice-versa para cada linha processada, causando lentidão severa. A abordagem nativa roda inteiramente em memória otimizada (C++/Java).

#### 1. Padronização de Colunas (`clean_column_names`)
* **Técnica:** Em vez de usar um loop com `withColumnRenamed` (que gera múltiplos planos de execução), utiliza-se uma única projeção `select` com `alias`.
* **Lógica:**
    * Remove acentos e caracteres especiais dos nomes das colunas.
    * Resolve colisões de nomes (ex: `valor`, `valor_1`) automaticamente.

#### 2. Limpeza de Dados (`process_dataframe`)
Utiliza expressões *Lazy Evaluation* do Spark SQL (`pyspark.sql.functions`):

* **Texto (`objeto_aquisicao`):**
    * `regexp_replace`: Remove caracteres não alfanuméricos via Regex nativo.
    * `trim` e `lower`: Normalização de strings.
* **Valores Monetários (`valor`):**
    * Remove símbolos (R$, pontos) e converte para `Decimal(20,0)`.
    * Aplica divisão aritmética por 100.0 para ajuste de centavos.
* **Documentos (CPF/CNPJ):**
    * Regex para manter apenas dígitos (`[^0-9]`).

---

## 🏗️ Decisões Técnicas e Motivações

| Decisão | Motivo Técnico |
| :--- | :--- |
| **Remoção de UDFs Python** | **Performance:** Elimina o overhead de serialização (Pickle) entre o executor Java e o processo Python. Permite que o *Catalyst Optimizer* do Spark planeje a query de forma eficiente. |
| **Select vs withColumnRenamed** | **Otimização de Plano:** `withColumnRenamed` em loop cria um plano lógico novo a cada iteração (DAG complexo). Um único `select` com aliases resolve tudo em uma operação. |
| **Spark 3.5.3 (Downgrade)** | **Estabilidade:** A versão 4.0.1 apresentou instabilidade no Windows (erros de Netty/BlockManager). A versão 3.5.3 é estável e compatível com `winutils`. |
| **Conversão ODS -> XLSX** | **Compatibilidade:** O Spark não possui leitura nativa robusta para OpenDocument (`.ods`). A conversão prévia via Pandas garante a integridade da ingestão. |

