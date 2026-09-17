"""
Sprint 3b — Feature Store (Machine Learning)
============================================
Lê o Star Schema (mart/) e produz a flat table de features para ML.

Diferença fundamental entre Star Schema e Feature Store:
  - Star Schema: normalizado, otimizado para JOINs analíticos (BI)
  - Feature Store: desnormalizado, otimizado para vetores de entrada (ML)

Entrada : data/mart/{fato_emendas, dim_*}.parquet
Saída   : data/feat/features.parquet

Uso:
    uv run python src/feat.py

Pré-requisito:
    uv run python src/mart.py
"""

import os
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

MART_DIR = Path(os.getenv("MART_DIR", "data/mart"))
FEAT_DIR = Path(os.getenv("FEAT_DIR", "data/feat"))


# ── Carregamento do Mart ──────────────────────────────────────────────────────

def carregar_mart(con: duckdb.DuckDBPyConnection) -> None:
    """
    Registra os Parquets do mart como views DuckDB.
    Isso evita carregar tudo na RAM — o DuckDB lê só o necessário.
    """
    tabelas = ["fato_emendas", "dim_localidade", "dim_autor"]
    for tabela in tabelas:
        path = MART_DIR / f"{tabela}.parquet"
        if not path.exists():
            logger.error(f"'{path}' não encontrado. Execute: uv run pipeline.py mart")
            raise SystemExit(1)

        # ATENÇÃO — SQL montado com f-string (leia antes de copiar este padrão)
        #
        # O ruff sinaliza esta linha como S608, "possível vetor de SQL
        # injection", e está certo em sinalizar. Aqui o alerta não se
        # concretiza, por duas razões concretas:
        #
        #   1. `tabela` vem da lista fixa logo acima, escrita no código;
        #   2. `path` vem de MART_DIR, que é configuração de quem executa
        #      o pipeline, não entrada de terceiros.
        #
        # Nenhum valor vindo da API, de formulário ou de requisição HTTP
        # chega até aqui. Em um pipeline local, interpolar assim é aceitável
        # — e muitas vezes inevitável, porque nome de tabela e caminho de
        # arquivo NÃO podem ser parametrizados: `?` liga valores, nunca
        # identificadores.
        #
        # O perigo é levar o hábito para onde o dado é de terceiros. Num app
        # web, `SELECT * FROM t WHERE uf = '{uf}'` com `uf` vindo da URL
        # entrega o banco ao visitante. Lá o certo é parametrizar:
        #
        #     con.execute("SELECT * FROM t WHERE uf = ?", [uf])
        #
        # Regra prática: valor sempre vai por parâmetro; identificador, se
        # precisar ser dinâmico, só depois de validado contra uma lista
        # conhecida — como a `tabelas` acima.
        con.execute(f"CREATE VIEW {tabela} AS SELECT * FROM read_parquet('{path}')")
        logger.debug(f"  view registrada: {tabela}")


# ── Feature Engineering ───────────────────────────────────────────────────────

def criar_features(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Cria a flat table de features.

    Conceitos:
    - Features proporcionais: volume relativo ao total do autor
    - Features de desvio: comportamento do autor vs. a própria média
    - Z-score: detecção de outliers por padronização estatística
    - Features binárias: indicadores 0/1 para modelos de classificação
    - Label: variável alvo ('executou_bem')
    """
    logger.info("Calculando features...")

    df = con.execute("""
        WITH stats_autor AS (
            -- Estatísticas agregadas por autor para features de desvio
            SELECT
                autor,
                COUNT(*)               AS total_emendas_autor,
                SUM(valorEmpenhado)    AS total_empenhado_autor,
                AVG(taxa_execucao_pct) AS media_execucao_autor
            FROM fato_emendas
            GROUP BY autor
        )
        SELECT
            -- Identificadores
            fe.codigoEmenda,
            fe.autor,
            fe.funcao,
            fe.uf,
            dl.regiao,
            fe.ano,

            -- Features brutas
            fe.valorEmpenhado,
            fe.valorPago,
            fe.taxa_execucao_pct,

            -- Feature: concentração de recursos por autor
            -- (parlamentar que destina muitos recursos para uma emenda específica)
            ROUND(
                fe.valorEmpenhado / NULLIF(sa.total_empenhado_autor, 0) * 100, 2
            ) AS pct_do_total_autor,

            -- Feature: essa emenda executa melhor ou pior que a média do autor?
            fe.taxa_execucao_pct - sa.media_execucao_autor
                AS desvio_execucao_vs_autor,

            -- Feature: z-score para detectar valores anômalos
            -- Subquery escalar: calculada uma vez, aplicada a todas as linhas
            ROUND(
                (fe.valorEmpenhado
                    - (SELECT AVG(valorEmpenhado)   FROM fato_emendas WHERE valorEmpenhado > 0))
                / NULLIF(
                    (SELECT STDDEV(valorEmpenhado)  FROM fato_emendas WHERE valorEmpenhado > 0),
                    0
                ), 3
            ) AS zscore_valor,

            -- Features binárias (0/1)
            fe.emenda_vitrine::INT                      AS flag_vitrine,
            (fe.valorRestoInscrito > 0)::INT            AS flag_em_restos,
            (dl.regiao = 'Nordeste')::INT               AS flag_nordeste,
            (dl.regiao = 'Norte')::INT                  AS flag_norte,
            (da.tipo_simplificado = 'Individual')::INT  AS flag_individual,

            -- Label: emenda com execução satisfatória (>= 80%)?
            (fe.taxa_execucao_pct >= 80)::INT           AS executou_bem

        FROM fato_emendas fe
        LEFT JOIN dim_localidade dl ON fe.localidade_id = dl.localidade_id
        LEFT JOIN dim_autor      da ON fe.autor_id      = da.autor_id
        JOIN  stats_autor        sa ON fe.autor         = sa.autor
        WHERE fe.valorEmpenhado > 0
    """).df()

    logger.success(f"✅ {len(df)} registros, {len(df.columns)} features")
    return df


# ── Seleção de Features por Correlação ───────────────────────────────────────

def analisar_correlacoes(df: pd.DataFrame) -> None:
    """
    Correlação entre cada feature e o label 'executou_bem'.

    Boas práticas:
    - Features com |corr| próximo de 0 não agregam informação
    - Features com |corr| próximo de 1 entre si são redundantes (multicolinearidade)
    - Decisão de incluir/excluir deve ser baseada nesses números, não em intuição
    """
    features = [
        "valorEmpenhado", "taxa_execucao_pct", "pct_do_total_autor",
        "desvio_execucao_vs_autor", "zscore_valor",
        "flag_vitrine", "flag_em_restos", "flag_nordeste",
        "flag_norte", "flag_individual",
    ]
    label = "executou_bem"

    df_corr = df[[*features, label]].dropna()
    correlacoes = df_corr.corr()[label].drop(label).sort_values(ascending=False)

    logger.info("═══ Correlação das Features com 'executou_bem' ═══")
    for feat, corr in correlacoes.items():
        if pd.isna(corr):
            logger.info(f"  ? {feat:<35}   NaN  (variância zero — feature constante no dataset atual)")
            continue
        barra = "█" * int(abs(corr) * 20)
        sinal = "+" if corr >= 0 else "-"
        logger.info(f"  {sinal} {feat:<35} {corr:+.3f}  {barra}")


# ── Exportação ────────────────────────────────────────────────────────────────

def exportar(df: pd.DataFrame) -> Path:
    FEAT_DIR.mkdir(parents=True, exist_ok=True)
    caminho = FEAT_DIR / "features.parquet"
    df.to_parquet(caminho, index=False, compression="snappy")
    logger.success(f"✅ Salvo: {caminho}")
    return caminho


# ── Orquestração ──────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("═══ Sprint 3b: Feature Store ═══")

    con = duckdb.connect()
    carregar_mart(con)

    df = criar_features(con)
    analisar_correlacoes(df)
    exportar(df)

    con.close()
    logger.success("🎉 Feature Store concluída! Próximo passo → uv run streamlit run app.py")


if __name__ == "__main__":
    main()
