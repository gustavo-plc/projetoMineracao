```python
# Read the content of Phase 1 documentation
with open('Documentacao_Tecnica_ETL_Fase1_v2.md', 'r', encoding='utf-8') as f:
    doc_phase1 = f.read()

# Read the content of Phase 2 documentation
with open('docs_fase_2.md', 'r', encoding='utf-8') as f:
    doc_phase2 = f.read()

print("Phase 1 Content Length:", len(doc_phase1))
print("Phase 2 Content Length:", len(doc_phase2))


```

```text
Phase 1 Content Length: 5614
Phase 2 Content Length: 7269


```

Com base na documentação anexada, preparei uma versão unificada e bem estruturada para o `README.md` do seu projeto no GitHub.

O documento abaixo integra o fluxo completo, desde a limpeza bruta (Fase 1) até a inteligência artificial preditiva (Fase 2), mantendo o nível técnico detalhado que você solicitou.

---

# 📘 Manual Técnico Unificado: Pipeline de Auditoria em Compras Públicas

**Projeto:** Mineração e Auditoria de Cartão de Pagamento do Governo Federal (CPGF)
**Tecnologia:** Python 3.x + PySpark (MLlib & SQL)
**Versão do Pipeline:** 2.0 (Consolidado: ETL + Machine Learning Supervisionado)

---

## 1. Visão Geral e Arquitetura

Este projeto implementa um pipeline de dados completo ("End-to-End") para processar, higienizar e auditar despesas públicas. A solução foi desenhada para transformar arquivos governamentais despadronizados em inteligência auditável, utilizando técnicas de Big Data e Machine Learning para identificar anomalias de preços e desvios semânticos.

### Arquitetura do Pipeline

O fluxo é dividido em duas grandes fases sequenciais:

1. **Fase 1 (ETL & Saneamento):** Foca na ingestão de arquivos `.xls/.xlsx` sujos, normalização de schema e aplicação de regras de negócio "Forenses" para criar uma camada de dados confiável ("Gold").
2. **Fase 2 (Mineração & Auditoria):** Aplica Inteligência Artificial (NLP e Clusterização) sobre os dados limpos para detectar fraudes através de uma abordagem híbrida (Estatística + Preditiva).

---

## 2. Fase 1: Extração, Transformação e Carga (ETL)

Nesta etapa, o objetivo é garantir que os dados brutos sejam utilizáveis, resolvendo problemas de formatação, caracteres ocultos e inconsistência de colunas ao longo dos anos.

### 2.1. Estratégia de Mapeamento "Slugify"

Arquivos governamentais mudam de cabeçalho anualmente (ex: "Objeto da Aquisição", "MOTIVO", "Objeto Aquisicao").

* **Solução:** Implementação de um dicionário baseado em *slugs*. O pipeline remove acentos, espaços e converte para minúsculo antes de mapear para o schema final (ex: `objetodaaquisicao` → `objeto_aquisicao`). Isso blinda o processo contra erros de digitação na origem.

### 2.2. Limpeza "Blindada" (Regex Allowlist)

Limpeza agressiva para campos monetários e textuais que frequentemente contêm caracteres invisíveis ou formatação quebrada.

* **Valores Monetários:** Utiliza Regex `[^0-9,-]` para remover tudo que não seja número ou vírgula, corrigindo casos como `R$ 1.200,50` para `DoubleType`.
* **Texto:** Normalização de acentos e remoção de caracteres especiais via `regexp_replace`.

### 2.3. Regras de Negócio e Consolidação

* **Filtro de "Linhas Fantasmas":** Remoção automática de linhas em branco ou nulas que representavam até 47% de arquivos Excel antigos.
* **Recuperação de Dados ("Salvar a Leroy Merlin"):** Registros com valor monetário válido mas sem descrição (`objeto_aquisicao` vazio) são preservados e marcados como "NAO INFORMADO", garantindo que o dinheiro gasto não seja descartado da análise.

**Saída da Fase 1:** Arquivo único `Consolidado_Final.parquet` (Particionado por Ano).

---

## 3. Fase 2: Mineração de Dados e Auditoria Avançada

A Fase 2 consome os dados consolidados e aplica modelos matemáticos para identificar sobrepreço baseando-se em **semântica** (o que é o item) e **contexto** (em qual grupo ele se encaixa).

### 3.1. Processamento de Linguagem Natural (NLP)

Transformação de texto livre em tokens significativos para o modelo.

* **Stopwords Customizadas:** Remoção de ~150 termos burocráticos que não descrevem o produto (ex: `necessidade`, `urgencia`, `vulto`, `solicitado`).
* **Filtros de Radical (SQL):** Remoção de termos institucionais como `almox*` (almoxarifado) e `reemb*` (reembolso) para focar no objeto real da compra.

### 3.2. Vetorização (Word2Vec)

Ensina o computador a entender contexto e sinônimos, transformando palavras em vetores matemáticos de 50 dimensões.

* **Configuração:** `minCount=5` ignora palavras raras (erros de digitação) para limpar o ruído.

### 3.3. Clusterização (Bisecting K-Means)

Agrupamento não-supervisionado para criar "gavetas" temáticas de comparação.

* **Lógica:** O algoritmo divide a base em **20 clusters** baseados em similaridade semântica (Distância de Cosseno), separando, por exemplo, "Peças Automotivas" de "Material de Escritório" automaticamente.

### 3.4. Metodologia de Detecção Híbrida (O "Cérebro" da Auditoria)

Utilizamos duas abordagens simultâneas para reduzir falsos positivos:

1. **Método Estatístico Intra-Cluster (IQR):**
* Analisa o item em relação aos seus pares no mesmo cluster.
* **Gatilho:** Detecta itens que furam o teto do Boxplot ().


2. **Método Preditivo Supervisionado (Random Forest):**
* Analisa o item em relação ao "conhecimento global" da base, prevendo quanto ele *deveria* custar baseado na descrição.
* **Gatilho:** Preço Pago > 3x Preço Estimado pelo modelo.



### 3.5. Score de Risco e Enriquecimento

O pipeline cruza os resultados dos dois métodos para gerar um **Score de Risco** unificado. Apenas itens com `Score > 10` (alta gravidade) são exportados para o relatório final, que é enriquecido com dados cadastrais originais (CPF/CNPJ) para permitir a investigação.

---

## 4. Configuração do Ambiente

O projeto foi configurado para execução local em ambiente Windows, simulando um cluster Spark.

* **Engine:** Spark Session com `spark-excel` (`com.crealytics:spark-excel_2.12:3.5.0_0.20.3`).
* **Recursos:** Otimizado com `spark.driver.memory = "4g"` e `spark.sql.shuffle.partitions = "8"`.
* **Fix de Rede:** Configuração `spark.driver.bindAddress = "127.0.0.1"` para evitar erros de VPN/Wi-Fi.

---

## 5. Dicionário de Artefatos (Saídas)

Ao final da execução, os seguintes arquivos são gerados na pasta do projeto:

| Arquivo | Fase Origem | Descrição | Uso Principal |
| --- | --- | --- | --- |
| `Consolidado_Final` | Fase 1 | Dataset Parquet limpo e unificado. | Base para análises de BI e input da Fase 2. |
| `relatorio_outliers_iqr.csv` | Fase 2 | Anomalias estatísticas por cluster. | Análise de dispersão de preços dentro dos grupos. |
| `auditoria_ml_random_forest.csv` | Fase 2 | Discrepâncias Texto vs. Preço. | Identificar itens caros mal classificados ou superfaturados. |
| **`AUDITORIA_COMPLETA_RASTREAVEL.csv`** | **Fase 2** | **Relatório Final Cruzado.** | **Lista de investigação forense com Score de Risco e dados de CPF/CNPJ.** |

---

## 6. Como Executar

1. **Entrada:** Coloque os arquivos `.xlsx` originais na pasta `dados/input/{ANO}/`.
2. **Fase 1:** Execute o notebook/script de ETL para gerar o `Consolidado_Final`.
3. **Fase 2:** Execute o notebook/script de Mineração. Acompanhe os logs de calibração do Random Forest (RMSE).
4. **Resultado:** O arquivo `AUDITORIA_COMPLETA_RASTREAVEL.csv` estará disponível na raiz do projeto. Abra-o no Excel (separador `;`, UTF-8) e ordene pela coluna `Score_Risco`.
