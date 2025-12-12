# 📄 Documentação Técnica: Pipeline de ETL Local (PySpark) - Fase 1 Completa

**Projeto:** Mineração de Dados de Gastos Públicos (Cartão Corporativo)
**Ambiente:** Local (Windows 11 / VS Code)
**Tecnologia:** Python 3.11 + PySpark 3.5.3 (Single Node)
**Status:** Ingestão, Limpeza e Conversão para Parquet (Concluído)

## 🏗️ Configuração Crítica do Ambiente (Windows)

Para viabilizar a execução do Spark no Windows sem erros de *NativeIO* ou *Hadoop*, as seguintes configurações foram aplicadas:

### 1. Binários do Hadoop (Winutils)
O Spark requer emulação do sistema de arquivos HDFS.
* **Versão Hadoop:** 3.3.5 (Compatível com Spark 3.5.3).
* **Variável de Ambiente:** `HADOOP_HOME` configurada para `C:\hadoop`.
* **Path:** `%HADOOP_HOME%\bin` adicionado ao Path do sistema.

### 2. Correção de DLL (UnsatisfiedLinkError)
Para corrigir o erro `java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0`, foi necessário instalar manualmente a biblioteca dinâmica do Hadoop.

* **Arquivo:** `hadoop.dll`
* **Origem (Download):** [https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.5/bin/hadoop.dll](https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.5/bin/hadoop.dll)
* **Instalação:** O arquivo foi copiado para:
    1.  `C:\hadoop\bin`
    2.  `C:\Windows\System32` (Essencial para o carregamento global pelo Java).

### 3. Bibliotecas Python Adicionais
Além do PySpark, foram instaladas dependências para manipulação de arquivos legados:
* **`xlrd`**: Necessário para ler arquivos Excel antigos (`.xls`) gerados antes de 2019.
* **`odfpy`**: Para conversão de arquivos OpenDocument (`.ods`).
* **`pandas`**: Utilizado para introspecção rápida de metadados (nomes de abas) antes da leitura com Spark.

---

## 🛠️ Detalhamento do Pipeline (Célula a Célula)

### Célula 1: Configuração e Inicialização
* **Spark Session:** Downgrade estratégico para **PySpark 3.5.3** para estabilidade.
* **Configuração de Rede:** `spark.driver.bindAddress` fixado em `127.0.0.1` para evitar quedas de conexão (*WinError 10054*).
* **Recursos:** Memória do driver ajustada para `4g` e partições de shuffle reduzidas para `8` para evitar *OOM (Out Of Memory)* no processamento local.

### Célula 2: Schema e Mapeamento
* Define a estrutura rígida dos dados (`DecimalType` para valores) e normaliza nomes de colunas variantes (ex: "Motivo" vs "Objeto da Aquisição").

### Células 3, 4 e 5: Processamento Nativo (Otimizado)
* **Estratégia:** Substituição de UDFs (Python) por funções nativas do Spark (`expr`, `regexp_replace`).
* **Ganho:** Eliminação da sobrecarga de serialização Python/JVM, resultando em processamento in-memory de alta performance.
* **Limpeza:** Remoção de acentos, caracteres especiais e formatação monetária realizada em uma única passada (*lazy evaluation*).

### Célula 6: Motor de Execução e Conversão (XLS -> Parquet)
Esta célula orquestra a leitura e gravação dos dados massivos.

**Lógica de Execução:**
1.  **Iteração:** Varre as pastas de input ano a ano (2016-2025).
2.  **Detecção Dinâmica de Abas:** Utiliza `pandas.ExcelFile` para ler os metadados do arquivo e descobrir o nome exato da primeira aba (ex: "Planilha1", "Sheet1", "Relatorio"), evitando erros de "Unknown Sheet".
3.  **Leitura Spark:** Carrega os dados usando a biblioteca `com.crealytics:spark-excel`.
4.  **Transformação:** Aplica a função `process_dataframe` (Células 3-5).
5.  **Carga (Write):** Salva o resultado particionado em formato **Parquet** com compressão **Snappy**.

**Resultado da Execução:**
* **Total Processado:** 72 arquivos.
* **Sucesso:** 100% (72/72).
* **Erros Superados:**
    * *Missing xlrd:* Resolvido via instalação da lib.
    * *Unknown Sheet:* Resolvido via detecção dinâmica com Pandas.
    * *NativeIO/DLL:* Resolvido via `hadoop.dll` no System32.

---

## 📂 Estrutura de Saída
Os dados limpos encontram-se em:
`C:\VSCode\projetoMineracao\dados\Parquet\{ANO}\{NOME_ARQUIVO}`

Cada pasta contém os arquivos `.parquet` prontos para a etapa de consolidação e análise (Machine Learning).
