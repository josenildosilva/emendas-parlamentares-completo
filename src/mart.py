"""
Sprint 3a — Star Schema (Business Intelligence)
================================================
Lê a camada Trusted e produz o modelo dimensional para BI:
  - dim_tempo, dim_funcao, dim_localidade, dim_autor
  - fato_emendas (com KPIs calculados)
  - Views analíticas com Window Functions e CTEs

Entrada : data/trusted/emendas_trusted.parquet
Saída   : data/mart/{dim_*.parquet, fato_emendas.parquet, dicionario_dados.json}

Uso:
    uv run python src/mart.py
"""

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

TRUSTED_DIR = Path(os.getenv("TRUSTED_DIR", "data/trusted"))
MART_DIR    = Path(os.getenv("MART_DIR",    "data/mart"))


# ── Dimensões ─────────────────────────────────────────────────────────────────

def criar_dim_tempo(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("  dim_tempo...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_tempo AS
        SELECT DISTINCT
            ano AS ano_id,
            ano,
            CASE
                WHEN ano <= 2022 THEN 'Pré-Pandemia/Pandemia'
                WHEN ano = 2023  THEN 'Pós-Pandemia'
                ELSE 'Atual'
            END AS periodo
        FROM trusted
    """)
    n = con.execute("SELECT COUNT(*) FROM dim_tempo").fetchone()[0]
    logger.success(f"  ✅ dim_tempo: {n} linhas")


def criar_dim_funcao(con: duckdb.DuckDBPyConnection) -> None:
    """
    Chave natural: funcao (~20 funções orçamentárias no Brasil).
    subfuncao: atributo dependente — usamos a mais frequente por função.
    """
    logger.info("  dim_funcao...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_funcao AS
        WITH ranked AS (
            SELECT
                funcao,
                subfuncao,
                ROW_NUMBER() OVER (
                    PARTITION BY funcao
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM trusted
            GROUP BY funcao, subfuncao
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY funcao) AS funcao_id,
            funcao,
            subfuncao,
            CASE funcao
                WHEN 'Saúde'              THEN 'Social'
                WHEN 'Educação'           THEN 'Social'
                WHEN 'Assistência Social' THEN 'Social'
                WHEN 'Infraestrutura'     THEN 'Infraestrutura'
                WHEN 'Transporte'         THEN 'Infraestrutura'
                WHEN 'Defesa Nacional'    THEN 'Segurança'
                ELSE 'Outros'
            END AS area_tematica
        FROM ranked
        WHERE rn = 1
    """)
    n = con.execute("SELECT COUNT(*) FROM dim_funcao").fetchone()[0]
    logger.success(f"  ✅ dim_funcao: {n} linhas")


def criar_dim_localidade(con: duckdb.DuckDBPyConnection) -> None:
    """
    Chave natural: (localidadeDoGasto, uf).
    Cada município/UF é uma localidade distinta — esse número pode ser alto
    em datasets reais, mas é correto: não estamos duplicando, estamos enumerando.
    """
    logger.info("  dim_localidade...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_localidade AS
        WITH base AS (
            SELECT DISTINCT localidadeDoGasto, uf
            FROM trusted
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY uf, localidadeDoGasto) AS localidade_id,
            localidadeDoGasto AS localidade,
            uf,
            CASE uf
                WHEN 'AM' THEN 'Norte'        WHEN 'PA' THEN 'Norte'
                WHEN 'AC' THEN 'Norte'        WHEN 'RR' THEN 'Norte'
                WHEN 'RO' THEN 'Norte'        WHEN 'AP' THEN 'Norte'
                WHEN 'TO' THEN 'Norte'
                WHEN 'MA' THEN 'Nordeste'     WHEN 'PI' THEN 'Nordeste'
                WHEN 'CE' THEN 'Nordeste'     WHEN 'RN' THEN 'Nordeste'
                WHEN 'PB' THEN 'Nordeste'     WHEN 'PE' THEN 'Nordeste'
                WHEN 'AL' THEN 'Nordeste'     WHEN 'SE' THEN 'Nordeste'
                WHEN 'BA' THEN 'Nordeste'
                WHEN 'MG' THEN 'Sudeste'      WHEN 'ES' THEN 'Sudeste'
                WHEN 'RJ' THEN 'Sudeste'      WHEN 'SP' THEN 'Sudeste'
                WHEN 'PR' THEN 'Sul'          WHEN 'SC' THEN 'Sul'
                WHEN 'RS' THEN 'Sul'
                WHEN 'MS' THEN 'Centro-Oeste' WHEN 'MT' THEN 'Centro-Oeste'
                WHEN 'GO' THEN 'Centro-Oeste' WHEN 'DF' THEN 'Centro-Oeste'
                ELSE 'Nacional'
            END AS regiao
        FROM base
    """)
    n = con.execute("SELECT COUNT(*) FROM dim_localidade").fetchone()[0]
    logger.success(f"  ✅ dim_localidade: {n} linhas")


def criar_dim_autor(con: duckdb.DuckDBPyConnection) -> None:
    """
    Chave natural: autor.
    tipoEmenda: pegamos o tipo mais frequente do parlamentar.
    """
    logger.info("  dim_autor...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_autor AS
        WITH ranked AS (
            SELECT
                autor,
                tipoEmenda,
                ROW_NUMBER() OVER (
                    PARTITION BY autor
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM trusted
            GROUP BY autor, tipoEmenda
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY autor) AS autor_id,
            autor,
            tipoEmenda,
            CASE
                WHEN tipoEmenda ILIKE '%Individual%' THEN 'Individual'
                WHEN tipoEmenda ILIKE '%Comissão%'   THEN 'Comissão'
                WHEN tipoEmenda ILIKE '%Bancada%'    THEN 'Bancada'
                ELSE 'Outro'
            END AS tipo_simplificado
        FROM ranked
        WHERE rn = 1
    """)
    n = con.execute("SELECT COUNT(*) FROM dim_autor").fetchone()[0]
    logger.success(f"  ✅ dim_autor: {n} linhas")


# ── Fato ──────────────────────────────────────────────────────────────────────

def criar_fato(con: duckdb.DuckDBPyConnection) -> None:
    """
    Tabela Fato: apenas métricas numéricas + chaves estrangeiras.
    KPIs calculados via CTEs para manter a query legível e testável.
    """
    logger.info("  fato_emendas...")
    con.execute("""
        CREATE OR REPLACE TABLE fato_emendas AS
        WITH base AS (
            SELECT
                t.codigoEmenda,
                t.ano,
                t.autor,
                t.funcao,
                t.subfuncao,
                t.localidadeDoGasto,
                t.uf,
                t.tipoEmenda,
                t.valorEmpenhado,
                t.valorLiquidado,
                t.valorPago,
                t.valorRestoInscrito,
                t.valorRestoCancelado,
                t.valorRestoPago
            FROM trusted t
        ),
        com_kpis AS (
            SELECT
                b.*,
                CASE
                    WHEN valorEmpenhado > 0
                    THEN ROUND(valorPago / valorEmpenhado * 100, 2)
                    ELSE 0
                END AS taxa_execucao_pct,
                valorRestoInscrito - valorRestoCancelado AS restos_liquidos,
                CASE
                    WHEN valorEmpenhado > 0
                     AND valorPago / valorEmpenhado < 0.1
                    THEN TRUE
                    ELSE FALSE
                END AS emenda_vitrine
            FROM base b
        )
        SELECT
            ck.*,
            dt.ano_id,
            dl.localidade_id,
            da.autor_id
        FROM com_kpis ck
        LEFT JOIN dim_tempo       dt ON ck.ano               = dt.ano_id
        LEFT JOIN dim_localidade  dl ON ck.uf                = dl.uf
                                    AND ck.localidadeDoGasto = dl.localidade
        LEFT JOIN dim_autor       da ON ck.autor             = da.autor
    """)
    n = con.execute("SELECT COUNT(*) FROM fato_emendas").fetchone()[0]
    logger.success(f"  ✅ fato_emendas: {n} linhas")


# ── Views Analíticas ──────────────────────────────────────────────────────────
#
# Views são lazy — não materializam dados, apenas definem a query.
# Nunca usar ORDER BY em views (quem consulta decide a ordenação).
#

def criar_views(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("  vw_ranking_autores...")
    con.execute("""
        CREATE OR REPLACE VIEW vw_ranking_autores AS
        WITH totais AS (
            SELECT
                fe.autor,
                da.tipo_simplificado,
                SUM(fe.valorEmpenhado)    AS total_empenhado,
                SUM(fe.valorPago)         AS total_pago,
                COUNT(*)                  AS qtd_emendas,
                AVG(fe.taxa_execucao_pct) AS media_execucao
            FROM fato_emendas fe
            JOIN dim_autor da ON fe.autor_id = da.autor_id
            GROUP BY fe.autor, da.tipo_simplificado
        )
        SELECT
            *,
            RANK() OVER (ORDER BY total_empenhado DESC) AS rank_empenhado,
            SUM(total_empenhado) OVER (
                ORDER BY total_empenhado DESC
                ROWS UNBOUNDED PRECEDING
            ) AS total_acumulado
        FROM totais
    """)
    logger.success("  ✅ vw_ranking_autores")

    logger.info("  vw_distribuicao_funcao_regiao...")
    con.execute("""
        CREATE OR REPLACE VIEW vw_distribuicao_funcao_regiao AS
        SELECT
            df.funcao,
            df.area_tematica,
            dl.regiao,
            COUNT(*)                    AS qtd_emendas,
            SUM(fe.valorEmpenhado)      AS total_empenhado,
            SUM(fe.valorPago)           AS total_pago,
            AVG(fe.taxa_execucao_pct)   AS media_execucao,
            SUM(fe.emenda_vitrine::INT) AS emendas_vitrine
        FROM fato_emendas fe
        JOIN  dim_funcao     df ON fe.funcao       = df.funcao
        LEFT JOIN dim_localidade dl ON fe.localidade_id = dl.localidade_id
        GROUP BY df.funcao, df.area_tematica, dl.regiao
    """)
    logger.success("  ✅ vw_distribuicao_funcao_regiao")

    logger.info("  vw_execucao_orcamentaria...")
    con.execute("""
        CREATE OR REPLACE VIEW vw_execucao_orcamentaria AS
        SELECT
            fe.funcao,
            fe.uf,
            dl.regiao,
            COUNT(*)                                                  AS total_emendas,
            SUM(CASE WHEN fe.emenda_vitrine THEN 1 ELSE 0 END)       AS vitrines,
            ROUND(
                SUM(CASE WHEN fe.emenda_vitrine THEN 1 ELSE 0 END)::FLOAT
                / COUNT(*) * 100, 1
            )                                                         AS pct_vitrines,
            AVG(fe.taxa_execucao_pct)                                 AS media_execucao
        FROM fato_emendas fe
        LEFT JOIN dim_localidade dl ON fe.localidade_id = dl.localidade_id
        GROUP BY fe.funcao, fe.uf, dl.regiao
    """)
    logger.success("  ✅ vw_execucao_orcamentaria")


# ── Governança ────────────────────────────────────────────────────────────────

def gerar_dicionario(con: duckdb.DuckDBPyConnection) -> None:
    """Dicionário de dados automático — documentação que acompanha o schema."""
    logger.info("  Gerando dicionário de dados...")
    tabelas = ["dim_tempo", "dim_funcao", "dim_localidade", "dim_autor", "fato_emendas"]
    dicionario = {t: con.execute(f"DESCRIBE {t}").df().to_dict(orient="records")
                  for t in tabelas}
    caminho = MART_DIR / "dicionario_dados.json"
    caminho.write_text(json.dumps(dicionario, indent=2, ensure_ascii=False))
    logger.success("  ✅ dicionario_dados.json")


# ── Exportação ────────────────────────────────────────────────────────────────

def exportar(con: duckdb.DuckDBPyConnection) -> None:
    """Persiste cada tabela como Parquet independente."""
    MART_DIR.mkdir(parents=True, exist_ok=True)
    tabelas = ["dim_tempo", "dim_funcao", "dim_localidade", "dim_autor", "fato_emendas"]
    for tabela in tabelas:
        df = con.execute(f"SELECT * FROM {tabela}").df()
        df.to_parquet(MART_DIR / f"{tabela}.parquet", index=False, compression="snappy")
        logger.success(f"  ✅ {tabela}.parquet ({len(df)} linhas)")


# ── Orquestração ──────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("═══ Sprint 3a: Star Schema ═══")

    trusted_path = TRUSTED_DIR / "emendas_trusted.parquet"
    if not trusted_path.exists():
        logger.error("Trusted não encontrado. Execute: uv run pipeline.py trusted")
        raise SystemExit(1)

    n = len(pd.read_parquet(trusted_path))
    logger.info(f"Entrada: {trusted_path} ({n} registros)")

    con = duckdb.connect()
    con.execute(f"CREATE VIEW trusted AS SELECT * FROM read_parquet('{trusted_path}')")

    logger.info("── Dimensões ──")
    criar_dim_tempo(con)
    criar_dim_funcao(con)
    criar_dim_localidade(con)
    criar_dim_autor(con)

    logger.info("── Tabela Fato ──")
    criar_fato(con)

    logger.info("── Views Analíticas ──")
    criar_views(con)

    logger.info("── Governança ──")
    gerar_dicionario(con)

    logger.info("── Exportando para Parquet ──")
    exportar(con)

    con.close()
    logger.success("🎉 Star Schema concluído! Próximo passo → uv run pipeline.py feat")


if __name__ == "__main__":
    main()
