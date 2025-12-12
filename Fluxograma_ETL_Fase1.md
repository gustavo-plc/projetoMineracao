# Fluxograma Detalhado: Pipeline ETL (Fase 1)

Este documento contém o diagrama visual completo do processo de Engenharia de Dados executado no projeto.

## Visualização do Processo

```mermaid
graph TD
    %% --- DEFINIÇÃO DE ESTILOS (CORES) ---
    classDef default font-size:14px;
    
    %% Estilos para a Legenda (Fonte menor)
    classDef legCinza fill:#f9f9f9,stroke:#333,stroke-width:2px,font-size:11px;
    classDef legAzul fill:#e1f5fe,stroke:#0277bd,stroke-width:2px,font-size:11px;
    classDef legVerde fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,font-size:11px;
    classDef legLaranja fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,font-size:11px;

    %% --- ETAPA 1: CONFIGURAÇÃO ---
    subgraph Setup_Ambiente ["1. Configuração do Ambiente (Windows)"]
        style Setup_Ambiente fill:#f9f9f9,stroke:#333,stroke-width:2px
        A1[Instalar Python 3.11 + Java JDK 17] --> A2[Baixar Binários Hadoop (Winutils 3.3.5)]
        A2 --> A3[Configurar HADOOP_HOME e PATH]
        A3 --> A4{Correção Crítica: NativeIO}
        A4 -->|Erro UnsatisfiedLinkError| A5[Baixar hadoop.dll]
        A5 --> A6[Copiar hadoop.dll para C:/Windows/System32]
        A6 --> A7[Instalar Libs: pyspark, xlrd, odfpy, pandas]
    end

    %% --- ETAPA 2: PRÉ-PROCESSAMENTO ---
    subgraph Pre_Processamento ["2. Pré-Processamento (converter_arquivos.py)"]
        style Pre_Processamento fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
        B1((Início Script)) --> B2[Varrer pasta dados/input]
        B2 --> B3{Arquivo é .ODS?}
        B3 -- Sim --> B4[Ler com odfpy]
        B4 --> B5[Salvar cópia como .XLSX via Pandas]
        B3 -- Não (.xls/.xlsx) --> B6[Ignorar / Manter original]
    end

    Setup_Ambiente --> B1

    %% --- ETAPA 3: SPARK ---
    subgraph ETL_Spark ["3. Pipeline Spark (analise_dados.py)"]
        style ETL_Spark fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
        
        C1((Início Spark)) --> C2[Inicializar SparkSession]
        C2 --> C3[Config: Driver Mem 4GB | Partitions 8]
        C3 --> C4[Config: BindAddress 127.0.0.1 (Fix Conexão)]
        C4 --> C5[Carregar Dependência: spark-excel]
        
        C5 --> C6[Definir Schema Typed (Decimal, Integer)]
        C6 --> C7[Compilar Funções Nativas (Lazy Eval)]

        C7 --> C8{Loop: Anos 2016-2025}
        C8 -- Próximo Ano --> C9[Listar Arquivos Excel na pasta]
        C9 --> C10{Loop: Arquivos}
        
        C10 -- Próximo Arquivo --> C11[Pandas: Ler Metadados]
        C11 --> C12[Detectar Nome da 1ª Aba (Fix Unknown Sheet)]
        C12 --> C13[Spark: Ler Aba Específica]
        
        C13 --> C14[Processamento: clean_column_names]
        C14 --> C15[Processamento: process_dataframe]
        
        subgraph Transformacao_Detalhe ["Detalhe da Limpeza (JVM)"]
            style Transformacao_Detalhe fill:#fff,stroke:#666,stroke-dasharray: 5 5
            T1[Input: Texto Bruto] --> T2[Lower Case]
            T2 --> T3[Translate: Trocar Acentos (ã->a, ç->c)]
            T3 --> T4[Regexp: Remover Símbolos não-alfanum]
            T4 --> T5[Trim: Remover Espaços]
        end
        
        C15 --> T1
        T5 --> C16[Output: Dados Limpos]

        C16 --> C17[Escrever Parquet (Partitioned)]
        C17 --> C18[Destino: dados/Parquet/{ANO}/{ARQUIVO}]
        C18 --> C10
        C10 -- Fim Arquivos --> C8
    end

    B6 --> C1

    %% --- ETAPA 4: CONSOLIDAÇÃO ---
    subgraph Consolidacao ["4. Consolidação Blindada (Célula 7)"]
        style Consolidacao fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
        
        D1[Início Consolidação] --> D2[Glob: Listar Pastas '20*' com conteúdo]
        D2 --> D3{Pasta tem .parquet?}
        D3 -- Não --> D4[Ignorar (Evita Erro Schema)]
        D3 -- Sim --> D5[Adicionar à lista de leitura]
        
        D5 --> D6[Spark Read: Lista de Caminhos]
        D6 --> D7[Forçar Schema Base (Evita Inferência)]
        D7 --> D8[Filtro de Qualidade (Remover Nulos/Zeros)]
        D8 --> D9[Coalesce(1): Fundir Partições]
        
        D9 --> D10[Escrita Final]
        D10 --> D11[Destino: dados/Consolidado (Isolamento I/O)]
        
        D11 --> D12((FIM FASE 1))
    end

    C8 -- Fim Anos --> D1

    %% --- LEGENDA (ESTILO MAPA) ---
    %% Usamos links invisíveis (~~~) para forçar a legenda a ficar embaixo
    D12 ~~~ LegendaTitulo
    
    subgraph LegendaArea [" "]
        style LegendaArea fill:none,stroke:none
        direction LR
        LegendaTitulo[LEGENDA:]:::default
        L1(Configuração / Windows):::legCinza
        L2(Pré-Processamento / Python):::legAzul
        L3(Processamento / Spark):::legVerde
        L4(Consolidação / Final):::legLaranja
        
        LegendaTitulo ~~~ L1 ~~~ L2 ~~~ L3 ~~~ L4
    end
```

---
**Como visualizar este gráfico:**
1. Instale a extensão **"Markdown Preview Mermaid Support"** no VS Code.
2. Pressione `Ctrl + Shift + V` para abrir o visualizador.
