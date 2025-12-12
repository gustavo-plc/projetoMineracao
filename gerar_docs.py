import os

# Conteúdo da documentação didática e completa
conteudo_docs = """# 📘 Manual Técnico: Pipeline de Engenharia de Dados (ETL)

**Projeto:** Mineração de Gastos Públicos (Cartão Corporativo)
**Nível:** Iniciante / Intermediário
**Tecnologia:** Python + PySpark (Local no Windows)

---

## 1. Visão Geral do Projeto

Este projeto tem como objetivo processar uma grande quantidade de planilhas governamentais (despesas públicas), que estão despadronizadas e em formatos variados, transformando-as em uma base de dados única, limpa e otimizada para Inteligência Artificial.

### O Fluxo de Dados (Pipeline)


[Image of data processing pipeline]

1.  **Entrada:** Arquivos Excel (`.xls`, `.xlsx`) e OpenDocument (`.ods`) separados por ano.
2.  **Pré-processamento:** Conversão de formatos não suportados.
3.  **Processamento (Spark):** Leitura, limpeza de texto, padronização de colunas e conversão de tipos.
4.  **Armazenamento Intermediário:** Conversão para formato Parquet (colunar).
5.  **Saída Final:** Um arquivo único consolidado (`Consolidado.parquet`).

---

## 2. Configuração do Ambiente (Pré-requisitos)

Para que o Spark (originalmente feito para Linux) rode no Windows, o ambiente foi configurado manualmente.

* **Motor:** PySpark 3.5.3 (Versão LTS estável).
* **Java:** JDK 17.
* **Emulador Hadoop (Winutils):**
    * **O que é:** Pequenos programas (`winutils.exe`, `hadoop.dll`) que "enganam" o Spark para ele achar que está num cluster Hadoop.
    * **Instalação:**
        * `winutils.exe` -> `C:\\hadoop\\bin`
        * `hadoop.dll` -> `C:\\Windows\\System32` (Crucial para evitar erro `NativeIO`).
    * **Fonte:** [Repositório Winutils (Hadoop 3.3.5)](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin)

---

## 3. Detalhamento das Etapas (Step-by-Step)

### Etapa 0: Conversão de Arquivos (Script `converter_arquivos.py`)
O Spark não lê nativamente arquivos `.ods` (LibreOffice) com eficiência. Antes de tudo, convertemos eles.

* **Entrada:** Pasta `dados/input` contendo arquivos misturados (`.ods`, `.xls`, `.xlsx`).
* **Ação:** O script varre as pastas, detecta arquivos `.ods` e usa a biblioteca `odfpy` + `pandas` para salvar uma cópia em `.xlsx`.
* **Saída:** Arquivos `.xlsx` criados ao lado dos originais.

---

### Etapa 1: Configuração da Sessão (Célula 1)
Inicia o "motor" do Spark.

* **Configurações Chave:**
    * `spark.driver.memory = "4g"`: Dá 4GB de RAM para o processo.
    * `shuffle.partitions = "8"`: Divide os dados em 8 pedaços (ideal para processadores de 8-16 threads locais).
    * `bindAddress = "127.0.0.1"`: Força o Spark a usar a rede local interna, evitando queda de conexão se o Wi-Fi oscilar.

---

### Etapa 2: Definição de Regras (Células 2 a 5)
Aqui não processamos dados ainda, apenas ensinamos ao Spark "como" processar.

#### A. O Schema (Contrato de Dados)
Define que a coluna "valor" deve ser decimal e "ano" deve ser inteiro.
* **Entrada:** Nenhuma.
* **Saída:** Objeto `StructType` (Schema).

#### B. Funções de Limpeza (Nativas)
Criamos funções que rodam direto na JVM (Java Virtual Machine) do Spark para máxima velocidade.

1.  **`clean_column_names(df)`**:
    * **Entrada:** DataFrame com colunas sujas (ex: `Objeto da Aquisição`, `Valor (R$)`).
    * **Ação:** Remove acentos, troca espaços por `_` e remove parênteses.
    * **Saída:** DataFrame com colunas limpas (ex: `objeto_aquisicao`, `valor`).

2.  **`process_dataframe(df)`**:
    * **Entrada:** DataFrame bruto.
    * **Ação:**
        * *Texto:* `translate` (troca `ç`->`c`, `ã`->`a`) + `regexp_replace` (remove símbolos).
        * *Valor:* Remove "R$", pontos e converte para número.
        * *CPF:* Remove pontos e traços.
    * **Saída:** DataFrame higienizado.

---

### Etapa 3: Ingestão e Conversão (Célula 6)
O "coração" do processamento. Transforma planilhas lentas em dados rápidos.

* **Entrada:** Pastas de anos (`dados/input/2016`, `2017`...) contendo arquivos Excel.
* **Processo (Loop):**
    1.  Usa `pandas` para abrir apenas o cabeçalho do Excel e descobrir o **nome da primeira aba** (evita erro "Sheet1 not found").
    2.  O Spark lê essa aba específica.
    3.  Aplica `process_dataframe`.
    4.  Salva em Parquet.
* **Saída:** Milhares de arquivos `.parquet` organizados em `dados/Parquet/{ANO}/{ARQUIVO}`.

> **Por que Parquet?** É um formato binário e colunar. Um arquivo Excel de 50MB vira um Parquet de 5MB e o Spark consegue lê-lo 100x mais rápido.

---

### Etapa 4: Consolidação (Célula 7)
Junta os milhares de pedacinhos em um arquivo mestre.

* **Entrada:** Todas as subpastas válidas dentro de `dados/Parquet`.
* **Ação:**
    1.  Usa `glob` para listar apenas pastas que realmente contêm dados (ignora pastas vazias de erros passados).
    2.  `spark.read.parquet(*pastas)`: Lê tudo simultaneamente.
    3.  Aplica filtro final (remove linhas onde Valor é 0 ou Descrição é vazia).
    4.  `coalesce(1)`: Funde os dados em um único arquivo físico.
* **Saída:** Um único arquivo (pasta) em `dados/Consolidado`.

---

## 4. Resumo de Comandos Úteis

* **Rodar o script:**
    ```powershell
    python analise_dados.py
    ```
* **Instalar nova biblioteca:**
    ```powershell
    pip install nome_da_lib
    ```
* **Verificar se o Spark está vivo:** Olhe o terminal. Se houver uma barra de progresso `[Stage 0:=>   (0 + 1) / 1]`, ele está trabalhando.

---

**Status Atual:** O ETL foi executado com sucesso. 72 arquivos processados, 0 falhas, gerando uma base consolidada de ~54 mil registros prontos para Machine Learning.
"""

nome_arquivo = "Manual_Tecnico_ETL_Iniciante.md"
caminho_completo = os.path.join(os.getcwd(), nome_arquivo)

try:
    with open(caminho_completo, "w", encoding="utf-8") as f:
        f.write(conteudo_docs)
    print(f"✅ Manual Técnico gerado com sucesso!")
    print(f"📂 Local: {caminho_completo}")
    print("💡 Abra no VS Code para ler com formatação.")
except Exception as e:
    print(f"❌ Erro: {e}")