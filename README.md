# 🏛️ Emendas Parlamentares · Pipeline de Transparência

> **O dinheiro público tem endereço — mas nem sempre é fácil encontrá-lo.**

Pipeline de dados ponta a ponta para análise de emendas parlamentares brasileiras. Construído como projeto-demo da disciplina **Introdução à Ciência de Dados** (PPGCA · Prof. Dr. Josenildo Silva).

---

## 🎯 Contexto de Negócio

A cada ano, parlamentares brasileiros destinam bilhões de reais por meio de **emendas ao Orçamento da União** — verbas que financiam hospitais, escolas, obras de infraestrutura e programas sociais em todo o país. Os dados são públicos, disponíveis no Portal da Transparência, mas estão dispersos, em formato bruto, e exigem esforço técnico para se tornarem informação acionável.

Este projeto responde três perguntas concretas:

1. **Para onde vai o dinheiro?** Quais funções orçamentárias concentram mais recursos?
2. **Quem recebe mais — e quem recebe menos?** A distribuição geográfica é equitativa?
3. **O dinheiro empenhado realmente chega?** Qual a diferença entre empenho e pagamento?

---

## ⚡ Reprodutibilidade (Zero Config)

```bash
# Clone → instale → execute: três comandos, zero configuração manual
git clone https://github.com/seu-usuario/emendas-parlamentares.git
cd emendas-parlamentares

cp .env.example .env          # edite com sua API Key (opcional — modo demo funciona sem ela)
uv sync                       # instala todas as dependências com versões exatas
uv run ingest    # Sprint 1: coleta → data/raw/
uv run trusted   # Sprint 2: saneamento → data/trusted/
uv run mart      # Sprint 3a: Star Schema → data/mart/
uv run feat      # Sprint 3b: Feature Store → data/feat/
uv run app       # Sprint 4: dashboard interativo

# Ou rodar o pipeline completo de uma vez:
uv run pipeline
uv run streamlit run app.py     # Sprint 4: dashboard interativo
```

> **Modo demo:** sem API Key, o pipeline usa dados de amostra incluídos no repositório (`data/raw/emendas_2023.json`). Funciona sem nenhuma configuração adicional.

---

## 🏗️ Arquitetura Medallion

```
Portal da Transparência (API)
        │
        ▼
📂 data/raw/          ← Bronze: dados brutos, imutáveis, append-only
        │  ingest.py (Sprint 1)
        ▼
📂 data/trusted/      ← Silver: validados com Pandera, limpos, em Parquet
        │  trusted.py (Sprint 2)
        ▼
📂 data/mart/         ← Gold: Star Schema (BI) + banco OLAP DuckDB
📂 data/feat/         ← Gold: Feature Store (ML)
        │  mart.py (Sprint 3a)
        │  feat.py (Sprint 3b)
        ▼
🌐 app.py             ← Produto: Dashboard Streamlit (Sprint 4)
📄 reports/           ← Relatório executivo em Quarto (Sprint 4)
```

---

## 📁 Estrutura de Diretórios

```
emendas-parlamentares/
├── src/
│   ├── ingest.py       # S1: Ingestão com idempotência e checkpoint
│   ├── trusted.py    # S2: Qualidade com Pandera + DuckDB
│   ├── mart.py   # S3a: Star Schema (dimensões + fato + views)
│   └── feat.py   # S3b: Feature Store (features para ML)
├── app.py              # S4: Dashboard Streamlit
├── reports/
│   └── analise.qmd     # S4: Relatório executivo (Quarto)
├── data/
│   ├── raw/            # Bronze - imutável (não versionado)
│   ├── trusted/        # Silver - Parquet validado (não versionado)
│   ├── mart/           # Gold - OLAP + dicionário (não versionado)
│   └── feat/           # Gold - Feature Store (não versionado)
├── .env.example        # Template de variáveis de ambiente
├── .gitignore          # Protege segredos e dados grandes
├── pyproject.toml      # Dependências gerenciadas pelo uv
└── README.md
```

---

## 🔧 Variáveis de Ambiente

| Variável | Obrigatória | Descrição |
|---|---|---|
| `API_KEY` | Não | Chave do Portal da Transparência. Sem ela, usa dados de amostra. |
| `ANO_INICIO` | Não | Ano inicial da coleta (padrão: 2023) |
| `ANO_FIM` | Não | Ano final da coleta (padrão: 2023) |
| `RAW_DIR` | Não | Diretório da camada Raw (padrão: `data/raw`) |
| `TRUSTED_DIR` | Não | Diretório da camada Trusted (padrão: `data/trusted`) |

Obtenha uma API Key gratuita em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email

---

## 📐 Conceitos por Sprint

### Sprint 1 — Ingestão (`src/ingest.py`)
- Consumo de API REST com paginação por cursor
- **Idempotência:** checkpoint por hash SHA256 — rodar duas vezes não duplica dados
- Camada Raw imutável com timestamp no nome do arquivo
- Variáveis de ambiente via `python-dotenv`

### Sprint 2 — Qualidade (`src/trusted.py`)
- **Push Down Computation:** DuckDB lê JSON sem estourar RAM
- **Data Contract:** schema Pandera bloqueante (script para se dado estiver sujo)
- Rastreabilidade: log de cada registro descartado e motivo
- Exportação para Parquet (compressão Snappy)

### Sprint 3 — Modelagem (`src/mart.py` + `src/feat.py`)
- **Star Schema:** tabelas Fato + 4 Dimensões (Tempo, Função, Localidade, Autor)
- **Window Functions SQL:** ranking, running totals, KPIs calculados sem subqueries
- **Feature Store:** 10+ features para ML com seleção por correlação
- **Dicionário de dados** gerado automaticamente

### Sprint 4 — Produto (`app.py`)
- Cache com `@st.cache_data` (app não trava ao recarregar)
- Narrativa: Contexto → Dados → Conclusão → Ação
- Três perspectivas analíticas em abas independentes
- Fallback progressivo de dados: OLAP → Parquet → JSON

---

## 📊 Dicionário de Dados (Camada Trusted)

| Campo | Tipo | Nullable | Descrição |
|---|---|---|---|
| `codigoEmenda` | string | Não | Identificador único da emenda |
| `ano` | int | Não | Ano de exercício orçamentário |
| `tipoEmenda` | string | Não | Individual / Comissão / Bancada |
| `autor` | string | Não | Nome do parlamentar ou comissão |
| `funcao` | string | Não | Função orçamentária (Saúde, Educação...) |
| `subfuncao` | string | Sim | Subfunção (Atenção básica, Ensino superior...) |
| `localidadeDoGasto` | string | Sim | UF ou Nacional |
| `uf` | string | Não | Sigla da UF extraída (NA = Nacional) |
| `valorEmpenhado` | float | Não | Valor comprometido pelo orçamento (R$) |
| `valorLiquidado` | float | Não | Valor com serviço/entrega confirmada (R$) |
| `valorPago` | float | Não | Valor efetivamente transferido (R$) |
| `valorRestoInscrito` | float | Não | Comprometido mas não pago no exercício (R$) |
| `taxa_execucao_pct` | float | Não | `valorPago / valorEmpenhado × 100` |

---

## 🧠 Decisões Arquiteturais

**Por que DuckDB em vez de Spark?**
O dataset de emendas é anual e cabe em memória de um laptop. DuckDB processa arquivos Parquet de dezenas de GB sem servidor — complexidade zero, performance próxima ao Spark para esse porte.

**Por que Parquet em vez de CSV?**
Formato colunar: leitura 10-50x mais rápida para análises. Compressão automática (tipicamente 5-10x menor). Schema embutido preserva tipos sem ambiguidade.

**Por que Pandera em vez de validação manual?**
Data Contracts declarativos — o schema é código, versionado junto ao projeto. Mensagens de erro explícitas ("coluna CPF contém 342 valores inválidos"), não genéricas.

**Por que Streamlit em vez de Dash/Tableau?**
Produtividade: um cientista de dados entrega um app funcional em horas. O app é Python puro, versionável, testável e extensível — sem dependência de licença.

---

## 📚 Referências

- Reis & Housley, *Fundamentals of Data Engineering*, O'Reilly 2022
- Kimball & Ross, *The Data Warehouse Toolkit*, Wiley 2013
- DuckDB Documentation: https://duckdb.org/docs
- Portal da Transparência API: https://portaldatransparencia.gov.br/api-de-dados

---

## 🎓 Sobre o Projeto

Desenvolvido como demonstração pedagógica da disciplina **Introdução à Ciência de Dados** — Mestrado Profissional em Computação Aplicada (PPGCA).

> *Transparência não é só publicar dados. É garantir que eles possam ser verificados, replicados e questionados.*
