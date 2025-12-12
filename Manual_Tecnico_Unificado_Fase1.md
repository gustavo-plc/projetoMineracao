# 📘 Manual Técnico Unificado: Pipeline de Engenharia de Dados (ETL) com PySpark

**Projeto:** Mineração de Gastos Públicos (Cartão Corporativo)
**Nível:** Completo (Iniciante ao Avançado)
**Tecnologia:** Python 3.11 + PySpark 3.5.3 (Local no Windows)
**Status:** Fase 1 Concluída (ETL e Consolidação - Versão Estável 1.0)

---

## 1. Visão Geral do Projeto

Este projeto tem como objetivo processar uma grande quantidade de planilhas governamentais (despesas públicas), que estão despadronizadas e em formatos variados, transformando-as em uma base de dados única, limpa e otimizada para Inteligência Artificial.

### O Fluxo de Dados (Pipeline)

1.  **Entrada:** Arquivos Excel (`.xls`, `.xlsx`) e OpenDocument (`.ods`) separados por ano (2016-2025).
2.  **Pré-processamento:** Conversão de formatos não suportados nativamente.
3.  **Processamento (Spark):** Leitura, limpeza de texto, padronização de colunas e conversão de tipos via motor nativo (JVM).
4.  **Armazenamento Intermediário:** Conversão para formato Parquet (colunar).
5.  **Saída Final:** Um arquivo único consolidado (`Consolidado.parquet`).

---

## 2. Configuração Crítica do Ambiente (Windows)

Para viabilizar a execução do Spark (originalmente nativo Linux) no Windows e evitar erros de *NativeIO* ou *Heartbeat*, as seguintes configurações foram aplicadas obrigatoriamente.

### 2.1 Binários do Hadoop (Winutils)
O Spark requer emulação do sistema de arquivos HDFS.
* **Versão Hadoop:** 3.3.5 (Compatível com Spark 3.5.3).
* **Variável de Ambiente:** `HADOOP_HOME` configurada para `C:\hadoop`.
* **Path:** `%HADOOP_HOME%\bin` adicionado ao Path do sistema.

### 2.2 Correção de DLL (Erro `UnsatisfiedLinkError`)
Para corrigir o erro `java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0`, foi necessário instalar manualmente a biblioteca dinâmica.
* **Arquivo:** `hadoop.dll`
* **Origem (Download):** [GitHub - cdarlint/winutils (Hadoop 3.3.5)](https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.5/bin/hadoop.dll)
* **Instalação:** O arquivo foi copiado para:
    1.  `C:\hadoop\bin`
    2.  `C:\Windows\System32` (Essencial para o carregamento global pelo Java).

### 2.3 Bibliotecas Python Adicionais
* **`pyspark==3.5.3`:** Versão LTS escolhida após instabilidade da v4.0.1 no Windows (erros de Netty/BlockManager).
* **`xlrd`:** Necessário para ler arquivos Excel antigos (`.xls`) gerados antes de 2019.
* **`odfpy`:** Para conversão prévia de arquivos OpenDocument (`.ods`).
* **`pandas`:** Utilizado para introspecção rápida de metadados (nomes de abas) antes da leitura massiva.

---

## 3. Detalhamento do Pipeline (Passo a Passo)

### Etapa 0: Conversão de Arquivos (Script `converter_arquivos.py`)
**Motivo:** O Spark não lê nativamente arquivos `.ods` (LibreOffice) com eficiência ou estabilidade.
* **Entrada:** Pasta `dados/input` contendo arquivos misturados (`.ods`, `.xls`, `.xlsx`).
* **Ação:** O script varre as pastas recursivamente, detecta arquivos `.ods` e usa a biblioteca `odfpy` + `pandas` para salvar uma cópia em `.xlsx`.
* **Saída:** Arquivos `.xlsx` criados ao lado dos originais.

### Etapa 1: Inicialização e Otimização (Célula 1)
Inicia o "motor" do Spark com configurações específicas para hardware local (Ryzen 7700).
* **Memória (`spark.driver.memory="4g"`):** Aumentada para prevenir estouro de memória (Heap Space) ao ler múltiplos Excels.
* **Paralelismo (`shuffle.partitions="8"`):** Reduzido de 200 (padrão) para 8, otimizando para a CPU local e evitando overhead.
* **Rede (`bindAddress="127.0.0.1"`):** Força o uso da rede local interna, corrigindo quedas de conexão do driver (*WinError 10054*) causadas por oscilação de Wi-Fi/VPN.

### Etapa 2: Regras e Limpeza Nativa (Células 2 a 5)
Nesta etapa, definimos o schema e as funções de transformação. Houve uma mudança arquitetural importante aqui: **Substituição de UDFs Python por Spark SQL Nativo**.

#### A. O Schema (Contrato de Dados)
Define a estrutura rígida para evitar inferência lenta.
* `valor`: `DecimalType(12,2)`
* `ano`: `IntegerType`

#### B. Funções de Limpeza (Otimizadas)
Em vez de usar Python (lento devido à serialização), usamos expressões nativas da JVM (`pyspark.sql.functions`).

1.  **Padronização de Colunas (`clean_column_names`):**
    * Utiliza uma única projeção `select` com `alias` em vez de loops com `withColumnRenamed` (que geram planos de execução complexos).
    * Remove acentos e resolve colisões de nomes (ex: `valor`, `valor_1`).

2.  **Limpeza de Dados (`process_dataframe`):**
    * **Acentuação:** Uso de `F.translate` para mapear caracteres (`ç` -> `c`, `ã` -> `a`) *antes* da sanitização. Isso corrige o problema onde "instalação" virava "instalao".
    * **Sanitização:** Uso de `regexp_replace` para remover símbolos e pontuação.
    * **Monetário:** Remove "R$", pontos de milhar e converte para decimal.

### Etapa 3: Ingestão e Conversão (Célula 6)
O motor de processamento que transforma planilhas lentas em dados rápidos.

* **Entrada:** Pastas de anos (`dados/input/2016` a `2025`).
* **Desafio Superado (Abas Desconhecidas):** Os arquivos governamentais não padronizam o nome da aba ("Planilha1", "Sheet1", "Dados").
* **Solução:** Implementação de **Detecção Dinâmica**. O script usa `pd.ExcelFile(path).sheet_names[0]` para descobrir o nome real da aba antes de instanciar o leitor do Spark.
* **Desafio Superado (Arquivos Legados):** Instalação da lib `xlrd` para suportar arquivos `.xls` de 2016-2018.
* **Ação:** Leitura, Aplicação de `process_dataframe` e Escrita.
* **Saída:** Milhares de arquivos particionados em formato **Parquet** (compressão Snappy) organizados em `dados/Parquet/{ANO}/{ARQUIVO}`.

### Etapa 4: Consolidação Blindada (Célula 7)
Junta os milhares de arquivos particionados em um único arquivo mestre.

#### Desafios Críticos Resolvidos nesta Etapa:
1.  **Race Condition (Conflito de Leitura/Escrita):**
    * *Erro:* O Spark tentava ler a pasta `dados/Parquet/*` enquanto escrevia o resultado dentro de `dados/Parquet/final`, causando travamento e `FileNotFoundException`.
    * *Solução:* Isolamento de I/O. Leitura ocorre em `dados/Parquet/20*` e a escrita vai para uma pasta externa `dados/Consolidado`.
2.  **Erro de Inferência (`UNABLE_TO_INFER_SCHEMA`):**
    * *Erro:* Pastas vazias geradas por falhas anteriores impediam o Spark de adivinhar o schema.
    * *Solução:*
        * Uso de `glob` para listar e filtrar apenas pastas que realmente contêm arquivos `.parquet`.
        * Aplicação forçada do schema (`.schema(schema_base)`).

* **Ação Final:** Leitura unificada, filtragem de qualidade (remoção de nulos) e `coalesce(1)` para gerar um arquivo físico único.

---

## 4. Estrutura de Saída e Métricas

O pipeline gerou com sucesso a base consolidada em:
`C:\VSCode\projetoMineracao\dados\Consolidado`

**Métricas Finais:**
* **Arquivos Processados:** 72 de 72 (100% de sucesso).
* **Registros Totais Brutos:** ~78.000.
* **Registros Válidos (Limpos):** ~54.000 (Registros nulos ou vazios descartados).

**Schema Final Garantido:**
| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `ano` | Integer | Ano de exercício |
| `cpf_suprido` | String | Apenas dígitos |
| `cpf_cnpj_favorecido` | String | Apenas dígitos |
| `objeto_aquisicao` | String | Texto normalizado (sem acentos, minúsculo) |
| `valor` | Decimal(12,2) | Valor monetário formatado |

---

## 5. Resumo de Comandos Úteis (Cheat Sheet)

* **Rodar o script:**
    ```powershell
    python analise_dados.py
    ```
* **Instalar nova biblioteca Python:**
    ```powershell
    pip install nome_da_lib
    ```
* **Verificar se o Spark está vivo:**
    Olhe o terminal. Se houver uma barra de progresso `[Stage 0:=>   (0 + 1) / 1]`, ele está processando.
* **Interpretar Arquivos na pasta Parquet:**
    * `_SUCCESS`: Indica que o processamento terminou bem.
    * `.crc`: Arquivos de verificação de integridade (não apagar).
    * `part-0000...parquet`: O arquivo de dados real.

---
**Próximos Passos (Fase 2):** Carregar o arquivo `Consolidado` e aplicar algoritmos de Machine Learning (K-Means) para clusterização e detecção de anomalias (outliers).
