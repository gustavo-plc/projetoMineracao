# 📘 Manual Técnico Unificado: Pipeline ETL de Gastos Públicos

**Projeto:** Mineração e Auditoria de Cartão Corporativo (CPGF)
**Status:** Fase 1 Concluída (ETL, Limpeza Forense e Consolidação "Gold")
**Tecnologia:** Python 3.x + PySpark 3.x (Local/Windows)
**Versão do Pipeline:** 2.0 (Refinada com Slugify e Regex Allowlist)

---

## 1. Visão Geral e Arquitetura

O pipeline foi desenhado para processar arquivos Excel governamentais com baixa padronização, transformando dados "sujos" em uma camada analítica confiável ("Gold").

### Fluxo de Dados (Data Lineage)
1.  **Camada Bronze (Input):** Arquivos `.xls` e `.xlsx` originais, organizados por ano.
2.  **Processamento (Silver):**
    * Leitura bruta (Raw) tratando tudo como texto inicialmente.
    * Normalização de colunas via estratégia *Slugify*.
    * Limpeza agressiva de valores via *Regex Allowlist*.
    * Gravação em formato **Parquet** particionado por ano.
3.  **Camada Ouro (Gold/Consolidado):**
    * Leitura unificada das partições.
    * Aplicação de Regras de Negócio (Tratamento de Nulos).
    * Filtragem de "Linhas Fantasmas" (Lixo do Excel).
    * Geração do arquivo único `Consolidado_Final.parquet`.

---

## 2. Configuração do Ambiente Spark (Windows)

Para garantir a execução local no Windows, o script realiza configurações automáticas de ambiente na **Célula 1**.

* **Engine:** Spark Session com `spark-excel` (com.crealytics versão `3.5.0_0.20.3`).
* **Hadoop/Winutils:** Configuração necessária para simular HDFS no Windows.
* **Memória:** `spark.driver.memory = "4g"` (Otimizado para volume médio de dados).
* **Fix de Rede:** `spark.driver.bindAddress = "127.0.0.1"` (Evita erros de VPN/Wi-Fi).

---

## 3. Detalhamento Técnico das Etapas (ETL)

### 3.1. Estratégia de Mapeamento "Slugify" (Célula 2)
**Problema:** Os arquivos governamentais variam os cabeçalhos entre anos (ex: "Objeto da Aquisição", "Objeto da Aquisicao", "MOTIVO").
**Solução:** Implementação de um dicionário baseado em *slugs*.
1.  O script remove acentos, espaços e converte o nome da coluna para minúsculo (ex: "Objeto da Aquisição" -> `objetodaaquisicao`).
2.  O dicionário mapeia o *slug* para o nome final (`objetodaaquisicao` -> `objeto_aquisicao`).
3.  **Benefício:** Imunidade a erros de digitação, acentuação ou *Case Sensitivity* nos arquivos originais.

### 3.2. Limpeza "Blindada" (Célula 3)
Funções nativas do Spark (`pyspark.sql.functions`) substituíram loops Python para performance.

#### A. Limpeza de Valor (Regex Allowlist)
O maior desafio foi limpar campos monetários sujos (ex: `R$    1.200,50` com caracteres ocultos).
* **Lógica Antiga:** Tentar remover o que *não* queremos (R$, espaços). Falhava com caracteres invisíveis.
* **Lógica Nova (Blindada):** Manter apenas o que *queremos*.
    * Regex: `[^0-9,-]` (Apaga tudo que não for número, vírgula ou traço).
    * Resultado: `R$ .. 1.200,50` vira `1200,50`.
    * Conversão: Troca `,` por `.` e converte para `DoubleType`.

#### B. Limpeza de Texto
* Uso de `translate` para converter caracteres acentuados (á -> a, ç -> c).
* Uso de `regexp_replace` para manter apenas letras e números (`[^a-z0-9\s]`).

### 3.3. Processamento e Particionamento (Célula 4)
* **Leitura Segura:** `inferSchema="false"`. O Spark lê tudo como String. A tipagem forte (Integer, Double) é aplicada apenas *após* a limpeza, evitando erros de conversão prematura.
* **Particionamento:** Os dados são salvos em `dados/Parquet/final/ano_partition=YYYY`. Isso permite leitura otimizada (Pruning) no futuro.

---

## 4. Regras de Negócio e Consolidação (Célula 5)

Esta etapa transforma os dados técnicos em dados de negócio, separando o "Lixo" do "Ouro".

### 4.1. O Problema das "Linhas Fantasmas"
* **Diagnóstico:** Arquivos Excel frequentemente possuem milhares de linhas em branco formatadas, que o Spark lê como linhas nulas. Em 2019, 47% das linhas eram lixo.
* **Solução:** Filtro `valor_transacao > 0`. Se não há saída de dinheiro, o registro é descartado.

### 4.2. Recuperação de Dados Parciais ("Salvar a Leroy Merlin")
* **Diagnóstico:** Alguns registros válidos (com valor monetário e favorecido, ex: Leroy Merlin) não possuíam descrição (`objeto_aquisicao` vazio ou "N/A").
* **Regra de Negócio:** Não descartar dinheiro real por falta de texto.
* **Ação:** Se `objeto_aquisicao` for nulo/vazio, o sistema preenche com **"NAO INFORMADO"** e mantém o registro na base.

---

## 5. Dicionário de Dados Final (Schema)

O arquivo `Consolidado_Final` possui a seguinte estrutura garantida:

| Coluna | Tipo (Spark) | Descrição | Regra de Limpeza |
| :--- | :--- | :--- | :--- |
| `ano` | Integer | Ano do exercício | Preenchido via nome da pasta se nulo |
| `unidade_gestora` | String | Órgão responsável | Slugify + Clean Text |
| `nome_suprido` | String | Servidor portador | Slugify + Clean Text |
| `cpf_suprido` | String | CPF do servidor | Apenas números |
| `nome_favorecido` | String | Empresa/Pessoa que recebeu | Slugify + Clean Text |
| `cpf_cnpj_favorecido`| String | Documento do recebedor | Apenas números |
| `objeto_aquisicao` | String | Descrição da compra | "NAO INFORMADO" se vazio |
| `data_aquisicao` | String | Data da compra | Mantido original (limpo) |
| `valor_transacao` | Double | Valor gasto (R$) | Regex Allowlist + Cast Double |

---

## 6. Como Utilizar

1.  **Entrada:** Coloque os arquivos `.xlsx` em `dados/input/{ANO}/`.
2.  **Execução:** Rode o script completo (Células 1 a 5).
3.  **Saída:** O arquivo final estará em `dados/Consolidado_Final`.
4.  **Análise:** Carregue este Parquet no Power BI, Tableau ou Pandas. Ele já está limpo, tipado e sem lixo.

