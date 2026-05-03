"""
trusted_duckdb.py  --  Camada Trusted (variante DuckDB-first)
=============================================================

Disciplina: Introducao a Ciencia de Dados · PPGCA · IFMA
Professor : Prof. Dr. Josenildo Silva

Arquitetura Medallion: Raw -> Trusted
Abordagem  : DuckDB-first.
             O objeto central e o *caminho* do arquivo (Path),
             nao um DataFrame. Pandas entra apenas para o lookup
             de UF sobre valores unicos -- no maximo 28 linhas.

Fluxo:
  data/raw/emendas_*.json
      |
      v  DuckDB COPY TO (QUERY_RAW)
      |  CASTs, conversao monetaria em SQL, filtros de sanidade
      v
  data/staging/emendas_staging.parquet   <- artefato interno (.gitignore)
      |
      v  DuckDB projeta valores unicos de localidadeDoGasto
      |  Pandas resolve lookup UF -> dim_uf (28 linhas, max)
      |  DuckDB registra dim_uf como tabela virtual
      v
  COPY TO com CTE encadeado:
      quarentena  <- WHERE valorPago > valorEmpenhado * 1.01
      deduplicado <- ROW_NUMBER OVER (PARTITION BY codigoEmenda)
      normalizado <- REPLACE trim/upper/initcap + COALESCE
      + LEFT JOIN dim_uf USING (localidadeDoGasto)
      v
  data/trusted/emendas_trusted.parquet
  data/quarentena/emendas_financeiro_invalido.parquet

Uso:
    uv run python src/trusted_duckdb.py
    uv run python src/trusted_duckdb.py --force
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuracao de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------
BASE_DIR      = Path(__file__).resolve().parent.parent
RAW_GLOB      = str(BASE_DIR / "data" / "raw" / "emendas_*.json")
STAGING_PATH  = BASE_DIR / "data" / "staging" / "emendas_staging.parquet"
TRUSTED_PATH  = BASE_DIR / "data" / "trusted" / "emendas_trusted.parquet"
QUARENT_PATH  = BASE_DIR / "data" / "quarentena" / "emendas_financeiro_invalido.parquet"

# ---------------------------------------------------------------------------
# Lookup de UF: nome completo -> sigla de dois caracteres
# ---------------------------------------------------------------------------
_NOME_PARA_UF: dict[str, str] = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAZONAS": "AM",
    "BAHIA": "BA", "CEARA": "CE", "DISTRITO FEDERAL": "DF",
    "ESPIRITO SANTO": "ES", "GOIAS": "GO", "MARANHAO": "MA",
    "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARA": "PA", "PARAIBA": "PB", "PARANA": "PR", "PERNAMBUCO": "PE",
    "PIAUI": "PI", "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS", "RONDONIA": "RO", "RORAIMA": "RR",
    "SANTA CATARINA": "SC", "SAO PAULO": "SP", "SERGIPE": "SE",
    "TOCANTINS": "TO",
}


def _extrair_uf(localidade: str | None) -> str:
    """
    Converte 'PERNAMBUCO (UF)' -> 'PE', 'Nacional' -> 'NA'.

    A estrategia e extrair o nome base (antes do parentese) e
    buscar no dicionario. Sem regex -- so strip e upper.
    """
    if not localidade:
        return "NA"
    nome = localidade.split("(")[0].strip().upper()
    return _NOME_PARA_UF.get(nome, "NA")


# ---------------------------------------------------------------------------
# Conversao monetaria em SQL
# ---------------------------------------------------------------------------
def _col_monetaria(coluna: str) -> str:
    """
    Gera expressao SQL para converter '1.234,56' -> 1234.56.

    TRY_CAST devolve NULL em vez de erro para valores invalidos,
    o que e preferivel a interromper o pipeline inteiro.
    """
    return (
        f"TRY_CAST("
        f"  replace(replace({coluna}, '.', ''), ',', '.')"
        f"  AS DOUBLE"
        f") AS {coluna}"
    )


# ---------------------------------------------------------------------------
# Query de leitura do Raw -> Staging
# ---------------------------------------------------------------------------
COLUNAS_MONETARIAS = [
    "valorEmpenhado",
    "valorLiquidado",
    "valorPago",
    "valorRestoInscrito",
    "valorRestoCancelado",
    "valorRestoPago",
]

QUERY_RAW = f"""
    SELECT
        CAST(codigoEmenda        AS VARCHAR)  AS codigoEmenda,
        CAST(ano                 AS INTEGER)  AS ano,
        CAST(tipoEmenda          AS VARCHAR)  AS tipoEmenda,
        CAST(autor               AS VARCHAR)  AS autor,
        CAST(nomeAutor           AS VARCHAR)  AS nomeAutor,
        CAST(numeroEmenda        AS VARCHAR)  AS numeroEmenda,
        CAST(localidadeDoGasto   AS VARCHAR)  AS localidadeDoGasto,
        CAST(funcao              AS VARCHAR)  AS funcao,
        CAST(subfuncao           AS VARCHAR)  AS subfuncao,
        {_col_monetaria('valorEmpenhado')},
        {_col_monetaria('valorLiquidado')},
        {_col_monetaria('valorPago')},
        {_col_monetaria('valorRestoInscrito')},
        {_col_monetaria('valorRestoCancelado')},
        {_col_monetaria('valorRestoPago')}
    FROM read_json_auto('{RAW_GLOB}', union_by_name=true)
    WHERE ano BETWEEN 2020 AND 2025
      AND codigoEmenda IS NOT NULL
"""


# ---------------------------------------------------------------------------
# Etapa 1: Raw -> Staging
# ---------------------------------------------------------------------------
def _raw_para_staging(con: duckdb.DuckDBPyConnection, force: bool) -> int:
    """
    Le todos os JSONs do Raw, converte tipos e grava em Staging.
    Devolve a quantidade de registros gravados.
    """
    if STAGING_PATH.exists() and not force:
        log.info("Staging ja existe. Use --force para reprocessar.")
        n = con.sql(f"SELECT count(*) FROM '{STAGING_PATH}'").fetchone()[0]
        log.info("Staging: %d registros (cache).", n)
        return n

    STAGING_PATH.parent.mkdir(parents=True, exist_ok=True)

    con.sql(f"""
        COPY (
            {QUERY_RAW}
        ) TO '{STAGING_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    n = con.sql(f"SELECT count(*) FROM '{STAGING_PATH}'").fetchone()[0]
    log.info("Staging gravado: %d registros -> %s", n, STAGING_PATH)
    return n


# ---------------------------------------------------------------------------
# Etapa 2: Lookup de UF (Pandas sobre valores unicos)
# ---------------------------------------------------------------------------
def _construir_dim_uf(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Projeta os valores distintos de localidadeDoGasto (max 28 + alguns
    valores especiais como 'Nacional') e resolve a sigla UF via dicionario
    Python. O resultado e um DataFrame pequeno registrado no DuckDB como
    tabela virtual 'dim_uf'.

    Pandas e usado aqui porque o lookup e algoritmo Python puro --
    nao ha expressao SQL equivalente ao dicionario de estados.
    O dataset tem no maximo ~30 linhas neste passo.
    """
    localidades = con.sql(f"""
        SELECT DISTINCT localidadeDoGasto
        FROM '{STAGING_PATH}'
        WHERE localidadeDoGasto IS NOT NULL
    """).df()

    localidades["uf"] = localidades["localidadeDoGasto"].apply(_extrair_uf)

    log.info(
        "dim_uf construida: %d localidades distintas.", len(localidades)
    )
    return localidades


# ---------------------------------------------------------------------------
# Etapa 3: Quarentena (registros com integridade financeira invalida)
# ---------------------------------------------------------------------------
SQL_QUARENTENA = f"""
    SELECT *
    FROM '{STAGING_PATH}'
    WHERE valorPago > valorEmpenhado * 1.01
"""


def _gravar_quarentena(con: duckdb.DuckDBPyConnection) -> int:
    """
    Grava os registros com valorPago > valorEmpenhado * 1.01 na camada
    de quarentena. Esses registros sao preservados com os valores
    originais ja convertidos para inspecao posterior.
    Devolve a quantidade de registros descartados.
    """
    QUARENT_PATH.parent.mkdir(parents=True, exist_ok=True)

    con.sql(f"""
        COPY (
            {SQL_QUARENTENA}
        ) TO '{QUARENT_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    n = con.sql(f"SELECT count(*) FROM '{QUARENT_PATH}'").fetchone()[0]
    if n > 0:
        log.warning(
            "Quarentena: %d registro(s) com valorPago > valorEmpenhado * 1.01"
            " -> %s",
            n, QUARENT_PATH,
        )
    else:
        log.info("Quarentena: nenhum registro com integridade financeira invalida.")
    return n


# ---------------------------------------------------------------------------
# Etapa 4: CTE de limpeza principal -> Trusted
# ---------------------------------------------------------------------------
def _sql_trusted() -> str:
    """
    Monta a query final com CTEs encadeados:
      1. deduplicado  -- ROW_NUMBER OVER (PARTITION BY codigoEmenda)
      2. normalizado  -- REPLACE: trim/upper/initcap + COALESCE
      + LEFT JOIN dim_uf USING (localidadeDoGasto)
    """
    return f"""
        WITH deduplicado AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY codigoEmenda
                        ORDER BY ano DESC
                    ) AS rn
                FROM '{STAGING_PATH}'
                WHERE valorPago <= valorEmpenhado * 1.01
            )
            WHERE rn = 1
        ),
        normalizado AS (
            SELECT * REPLACE (
                trim(upper(autor))                              AS autor,
                trim(upper(nomeAutor))                         AS nomeAutor,
                trim(initcap(funcao))                          AS funcao,
                trim(initcap(coalesce(subfuncao,
                    'Nao informada')))                         AS subfuncao,
                coalesce(localidadeDoGasto, 'Nacional')        AS localidadeDoGasto
            )
            FROM deduplicado
            WHERE funcao IS NOT NULL
        )
        SELECT
            n.*,
            coalesce(d.uf, 'NA') AS uf
        FROM normalizado n
        LEFT JOIN dim_uf d USING (localidadeDoGasto)
    """


def _gravar_trusted(con: duckdb.DuckDBPyConnection) -> int:
    """
    Executa o CTE de limpeza e grava o Parquet final na camada Trusted.
    Devolve a quantidade de registros aprovados.
    """
    TRUSTED_PATH.parent.mkdir(parents=True, exist_ok=True)

    con.sql(f"""
        COPY (
            {_sql_trusted()}
        ) TO '{TRUSTED_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    n = con.sql(f"SELECT count(*) FROM '{TRUSTED_PATH}'").fetchone()[0]
    log.info("Trusted gravado: %d registros -> %s", n, TRUSTED_PATH)
    return n


# ---------------------------------------------------------------------------
# Relatorio de qualidade (texto simples no log)
# ---------------------------------------------------------------------------
def _relatorio(n_raw: int, n_quarentena: int, n_trusted: int) -> None:
    descartados = n_raw - n_trusted - n_quarentena
    pct_aprovados = (n_trusted / n_raw * 100) if n_raw else 0.0

    log.info("=" * 52)
    log.info("RELATORIO DE QUALIDADE")
    log.info("  Registros lidos (raw)       : %8d", n_raw)
    log.info("  Quarentena financeira        : %8d", n_quarentena)
    log.info("  Descartados (nulos/dupl.)    : %8d", descartados)
    log.info("  Aprovados -> Trusted         : %8d  (%.1f%%)",
             n_trusted, pct_aprovados)
    log.info("=" * 52)


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
def main(force: bool = False) -> None:
    con = duckdb.connect()

    log.info("--- Raw -> Staging ---")
    n_raw = _raw_para_staging(con, force=force)

    if n_raw == 0:
        log.error("Nenhum registro encontrado em %s. Rode ingest.py primeiro.",
                  RAW_GLOB)
        sys.exit(1)

    log.info("--- Construindo dim_uf (Pandas, valores unicos) ---")
    dim_uf = _construir_dim_uf(con)
    con.register("dim_uf", dim_uf)  # tabela virtual, sem copia em disco

    log.info("--- Quarentena financeira ---")
    n_quarentena = _gravar_quarentena(con)

    log.info("--- Staging -> Trusted (CTE encadeado) ---")
    n_trusted = _gravar_trusted(con)

    _relatorio(n_raw, n_quarentena, n_trusted)

    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Camada Trusted -- variante DuckDB-first"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocessa o staging mesmo que ja exista.",
    )
    args = parser.parse_args()
    main(force=args.force)
