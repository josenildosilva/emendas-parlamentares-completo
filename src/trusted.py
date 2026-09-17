"""
Sprint 2 - Qualidade & Saneamento (Camada Trusted)
===================================================
Transforma dados Raw (Bronze) em dados confiáveis (Silver/Trusted).

Conceitos demonstrados:
  - Push Down Computation: filtros pesados no DuckDB, não no Pandas
  - Data Contracts com Pandera (validação bloqueante)
  - Engenharia Defensiva: script para se dado estiver sujo
  - Logs de rastreabilidade: quantos registros foram descartados e por quê
  - Exportação para Parquet (formato colunar eficiente)

Uso:
    uv run python src/trusted.py
"""

import json
import os
import re
from pathlib import Path

import duckdb
import pandas as pd
import pandera.pandas as pa
from dotenv import load_dotenv
from loguru import logger
from pandera.pandas import Column, DataFrameSchema, Check

# ── Configuração ──────────────────────────────────────────────────────────────
load_dotenv()

RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))
META_DIR = RAW_DIR / ".meta"
TRUSTED_DIR = Path(os.getenv("TRUSTED_DIR", "data/trusted"))

# Qualidade mínima aceitável (Engenharia Defensiva)
# Se mais que X% dos registros forem inválidos, o pipeline PARA.
LIMITE_DESCARTE_PCT = 0.30  # 30% máximo de descarte


# ── Conceito: Data Contract (Pandera Schema) ───────────────────────────────────
#
# Um Data Contract é um acordo formal entre quem produz e quem consome dados.
# O schema abaixo define EXATAMENTE o que esperamos da camada Raw.
# Se os dados não atenderem, o pipeline para com mensagem clara.
#
SCHEMA_RAW = DataFrameSchema(
    columns={
        "codigoEmenda": Column(str, nullable=False),
        "ano": Column(int, Check.isin([2020, 2021, 2022, 2023, 2024, 2025]), nullable=False),
        "tipoEmenda": Column(str, nullable=False),
        "autor": Column(str, nullable=False),
        "localidadeDoGasto": Column(str, nullable=True),
        "funcao": Column(str, nullable=False),
        "subfuncao": Column(str, nullable=True),
        "valorEmpenhado": Column(str, nullable=True),   # vem como string com vírgula
        "valorLiquidado": Column(str, nullable=True),
        "valorPago": Column(str, nullable=True),
    },
    checks=[
        # Nível de dataframe: não aceitar datasets completamente vazios
        Check(lambda df: len(df) > 0, error="Dataset vazio — verifique a ingestão"),
    ],
    strict=False,  # permite colunas extras (tolerante a novos campos da API)
    coerce=True,   # tenta converter tipos automaticamente
)


# ── Step 1: Leitura com DuckDB (Push Down Computation) ───────────────────────
#
# Conceito chave: NÃO carregamos tudo na RAM com pd.read_json().
# O DuckDB lê diretamente do arquivo e aplica filtros antes de criar o DataFrame.
# Para datasets de milhões de linhas, isso é a diferença entre funcionar e travar.
#
# Nome dos arquivos gravados pelo ingest: emendas_<ano>_<AAAAMMDD>_<HHMMSS>.json
_PADRAO_RAW = re.compile(r"^emendas_(\d{4})_(\d{8}_\d{6})\.json$")
# Amostra versionada do modo demo: emendas_<ano>_sample.json
_PADRAO_AMOSTRA = re.compile(r"^emendas_(\d{4})_sample\.json$")


def selecionar_arquivos_raw(raw_dir: Path) -> list[Path]:
    """
    Escolhe UM arquivo raw por ano — o registrado no checkpoint da ingestão.

    Conceito: a camada Raw é append-only. Cada execução do ingest grava um
    arquivo NOVO com timestamp, sem apagar os anteriores — isso é proposital,
    é o que torna o histórico auditável.

    A consequência é que ler `emendas_*.json` de uma vez reprocessa as mesmas
    emendas N vezes. O deduplicador descarta as cópias, o percentual de descarte
    cresce a cada rodada e, na segunda execução, a trava de qualidade
    (LIMITE_DESCARTE_PCT) derruba o pipeline por um problema que não existe
    nos dados.

    Raw imutável exige, portanto, um ponteiro para o "corrente" — aqui, o
    checkpoint escrito pelo ingest. Em produção, o equivalente seria ler uma
    partição (ex.: data/raw/ano=2023/ingestao=.../).
    """
    meta_dir = raw_dir / ".meta"
    selecionados: dict[str, Path] = {}

    # ── Caminho principal: o checkpoint diz qual arquivo é o corrente ──────────
    for checkpoint_path in sorted(meta_dir.glob(".checkpoint_*.json")):
        try:
            registro = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning(f"Checkpoint ilegível, ignorado: {checkpoint_path.name}")
            continue

        arquivo = Path(registro.get("arquivo", ""))
        ano = str(registro.get("ano", checkpoint_path.stem.replace(".checkpoint_", "")))

        # O checkpoint guarda o caminho relativo à raiz do projeto. Se o script
        # for chamado de outro diretório, tentamos reencontrar pelo nome.
        if not arquivo.is_file() and arquivo.name:
            alternativa = raw_dir / arquivo.name
            if alternativa.is_file():
                arquivo = alternativa

        if arquivo.is_file():
            selecionados[ano] = arquivo
        else:
            logger.warning(
                f"Checkpoint {checkpoint_path.name} aponta para arquivo inexistente "
                f"({arquivo}) — será ignorado."
            )

    # ── Fallback: sem checkpoint utilizável, pega o mais recente de cada ano ───
    if not selecionados:
        por_ano: dict[str, list[tuple[str, Path]]] = {}
        for caminho in raw_dir.glob("emendas_*.json"):
            m = _PADRAO_RAW.match(caminho.name)
            if m:
                por_ano.setdefault(m.group(1), []).append((m.group(2), caminho))

        for ano, itens in por_ano.items():
            selecionados[ano] = max(itens)[1]

        # Nenhuma saída do ingest: aceita a amostra versionada (modo demo)
        for caminho in raw_dir.glob("emendas_*_sample.json"):
            m = _PADRAO_AMOSTRA.match(caminho.name)
            if m and m.group(1) not in selecionados:
                selecionados[m.group(1)] = caminho

        if selecionados:
            logger.warning(
                "Nenhum checkpoint válido em .meta/ — usando o arquivo mais "
                "recente de cada ano como fallback."
            )

    if not selecionados:
        raise FileNotFoundError(
            f"Nenhum arquivo raw encontrado em {raw_dir}. "
            "Execute primeiro: uv run pipeline.py ingest"
        )

    arquivos = [selecionados[ano] for ano in sorted(selecionados)]
    for ano, caminho in sorted(selecionados.items()):
        logger.info(f"Raw selecionado para {ano}: {caminho.name}")
    return arquivos


# ── Step 1: Leitura com DuckDB (Push Down Computation) ───────────────────────
def carregar_raw_com_duckdb(raw_dir: Path) -> pd.DataFrame:
    """
    Lê o raw corrente de cada ano usando DuckDB.

    Push Down Computation: o DuckDB processa o arquivo no disco.
    Só traz para a RAM os registros que passam no filtro inicial.
    """
    arquivos_json = selecionar_arquivos_raw(raw_dir)
    logger.info(f"Arquivos raw selecionados: {len(arquivos_json)}")

    # DuckDB lê JSON nativo — sem pd.read_json(), sem carregar na RAM
    lista_arquivos = ", ".join(f"'{a.as_posix()}'" for a in arquivos_json)

    con = duckdb.connect()
    query = f"""
        SELECT
            codigoEmenda,
            CAST(ano AS INTEGER)        AS ano,
            tipoEmenda,
            autor,
            nomeAutor,
            numeroEmenda,
            localidadeDoGasto,
            funcao,
            subfuncao,
            valorEmpenhado,
            valorLiquidado,
            valorPago,
            valorRestoInscrito,
            valorRestoCancelado,
            valorRestoPago
        FROM read_json_auto([{lista_arquivos}], union_by_name=True)
        -- Filtro inicial no DuckDB: só anos válidos (evita corrupção de dados)
        WHERE ano BETWEEN 2020 AND 2025
    """

    df = con.execute(query).df()
    logger.info(f"Registros carregados do Raw: {len(df)}")
    return df


# ── Step 2: Validação com Pandera ─────────────────────────────────────────────
#
# Conceito: Engenharia Defensiva — falhar rápido e com mensagem clara.
# É melhor parar o pipeline no S2 do que descobrir dados corrompidos no S4.
#
def validar_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida o DataFrame contra o Data Contract.
    Se falhar, o script para com mensagem explicativa.
    """
    logger.info("Validando schema com Pandera...")
    try:
        df_validado = SCHEMA_RAW.validate(df)
        logger.success("✅ Schema válido — todos os campos obrigatórios presentes")
        return df_validado
    except pa.errors.SchemaError as e:
        logger.error("❌ FALHA NA VALIDAÇÃO DO DATA CONTRACT")
        logger.error(f"   Detalhe: {e.args[0]}")
        logger.error("   Corrija o arquivo raw ou atualize o schema antes de continuar.")
        # 'from e' preserva o erro original do Pandera no traceback, que e
        # justamente o que diz QUAL coluna e QUANTOS valores falharam.
        raise SystemExit(1) from e


# ── Step 3: Limpeza com Pandas (Fine Tuning) ──────────────────────────────────
#
# Conceito: Paradigma Híbrido
# - DuckDB faz o trabalho pesado (leitura, filtros, joins em SQL)
# - Pandas faz o ajuste fino (limpeza de strings, conversões específicas)
#
def limpar_e_tipar(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Aplica transformações de limpeza e retorna relatório de qualidade.

    Rastreabilidade: registra cada decisão de descarte.
    """
    total_inicial = len(df)
    relatorio = {"total_inicial": total_inicial, "descartes": []}

    def registrar_descarte(motivo: str, n_antes: int, n_depois: int):
        descartados = n_antes - n_depois
        pct = descartados / total_inicial * 100
        if descartados > 0:
            relatorio["descartes"].append({
                "motivo": motivo,
                "registros_descartados": descartados,
                "percentual": round(pct, 2),
            })
            logger.warning(f"   Descarte [{motivo}]: {descartados} registros ({pct:.1f}%)")

    # ── 3.1 Converter valores monetários (vírgula → float) ────────────────────
    #
    # Os valores chegam como string no formato brasileiro: "1.234,56"
    # Precisamos converter para float: 1234.56
    #
    colunas_valor = [
        "valorEmpenhado", "valorLiquidado", "valorPago",
        "valorRestoInscrito", "valorRestoCancelado", "valorRestoPago",
    ]

    for col in colunas_valor:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)   # remove separador de milhar
                .str.replace(",", ".", regex=False)  # vírgula → ponto decimal
                .str.strip()
                .apply(lambda x: float(x) if x not in ("", "nan", "None") else 0.0)
            )

    logger.info("✅ Valores monetários convertidos para float")

    # ── 3.2 Remover duplicatas ────────────────────────────────────────────────
    n_antes = len(df)
    df = df.drop_duplicates(subset=["codigoEmenda"])
    registrar_descarte("duplicata por codigoEmenda", n_antes, len(df))

    # ── 3.3 Remover registros sem função orçamentária ─────────────────────────
    n_antes = len(df)
    df = df.dropna(subset=["funcao"])
    registrar_descarte("funcao orçamentária nula", n_antes, len(df))

    # ── 3.4 Normalizar strings ────────────────────────────────────────────────
    df["funcao"] = df["funcao"].str.strip().str.title()
    df["subfuncao"] = df["subfuncao"].fillna("Não informada").str.strip().str.title()
    df["autor"] = df["autor"].str.strip().str.upper()
    df["localidadeDoGasto"] = df["localidadeDoGasto"].fillna("Nacional").str.strip()

    # ── 3.5 Criar coluna de UF extraída da localidade ─────────────────────────
    df["uf"] = df["localidadeDoGasto"].apply(_extrair_uf)

    # ── 3.6 Verificar integridade financeira ─────────────────────────────────
    # valorPago não pode ser maior que valorEmpenhado (seria erro de dados)
    n_antes = len(df)
    df = df[df["valorPago"] <= df["valorEmpenhado"] * 1.01]  # 1% de tolerância
    registrar_descarte("valorPago > valorEmpenhado (corrompido)", n_antes, len(df))

    relatorio["total_final"] = len(df)
    relatorio["total_descartado"] = total_inicial - len(df)
    relatorio["pct_descartado"] = round(
        relatorio["total_descartado"] / total_inicial * 100, 2
    )

    return df, relatorio


def _extrair_uf(localidade: str) -> str:
    """
    Extrai a sigla de UF da string de localidade.

    O Portal da Transparência usa o formato "NOME DO ESTADO (UF)" onde
    "(UF)" é um sufixo literal indicando nível estadual — não é o código.
    Ex: "PERNAMBUCO (UF)" → "PE", "Nacional" → "NA"
    """
    NOME_PARA_SIGLA = {
        "ACRE": "AC", "ALAGOAS": "AL", "AMAPÁ": "AP", "AMAZONAS": "AM",
        "BAHIA": "BA", "CEARÁ": "CE", "DISTRITO FEDERAL": "DF",
        "ESPÍRITO SANTO": "ES", "GOIÁS": "GO", "MARANHÃO": "MA",
        "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
        "PARÁ": "PA", "PARAÍBA": "PB", "PARANÁ": "PR", "PERNAMBUCO": "PE",
        "PIAUÍ": "PI", "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN",
        "RIO GRANDE DO SUL": "RS", "RONDÔNIA": "RO", "RORAIMA": "RR",
        "SANTA CATARINA": "SC", "SÃO PAULO": "SP", "SERGIPE": "SE",
        "TOCANTINS": "TO",
    }
    # Remove o sufixo " (UF)" e busca no mapa
    nome = str(localidade).replace(" (UF)", "").strip().upper()
    return NOME_PARA_SIGLA.get(nome, "NA")  # NA = Nacional ou não identificado


# ── Step 4: Validação de Qualidade Mínima ────────────────────────────────────
def verificar_qualidade_minima(relatorio: dict) -> None:
    """
    Engenharia Defensiva: para o pipeline se o descarte for alto demais.

    Critério: se mais de LIMITE_DESCARTE_PCT dos registros foram descartados,
    algo está errado na fonte — melhor parar e investigar do que gerar análises erradas.
    """
    pct = relatorio["pct_descartado"] / 100
    if pct > LIMITE_DESCARTE_PCT:
        logger.error("❌ QUALIDADE INSUFICIENTE — pipeline interrompido")
        logger.error(
            f"   {relatorio['pct_descartado']:.1f}% dos registros descartados "
            f"(limite: {LIMITE_DESCARTE_PCT*100:.0f}%)"
        )
        logger.error("   Investigue os dados raw antes de continuar.")
        raise SystemExit(1)

    logger.success(
        f"✅ Qualidade aprovada: {relatorio['total_final']} registros válidos "
        f"({relatorio['pct_descartado']:.1f}% descartados)"
    )


# ── Step 5: Exportar para Parquet ─────────────────────────────────────────────
def exportar_trusted(df: pd.DataFrame) -> Path:
    """
    Salva camada Trusted em formato Parquet.

    Por que Parquet?
    - Formato colunar: leitura 10-50x mais rápida para análises
    - Compressão automática: tipicamente 5-10x menor que CSV
    - Schema embutido: tipos de dados preservados sem ambiguidade
    """
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    caminho = TRUSTED_DIR / "emendas_trusted.parquet"

    df.to_parquet(caminho, index=False, compression="snappy")

    tamanho_mb = caminho.stat().st_size / 1_048_576
    logger.success(f"✅ Trusted salvo: {caminho} ({tamanho_mb:.2f} MB, {len(df)} linhas)")
    return caminho


# ── Relatório de Qualidade ────────────────────────────────────────────────────
def imprimir_relatorio(relatorio: dict) -> None:
    """Exibe resumo legível do processo de limpeza."""
    logger.info("═══ Relatório de Qualidade de Dados ═══")
    logger.info(f"  Registros iniciais : {relatorio['total_inicial']}")
    logger.info(f"  Registros finais   : {relatorio['total_final']}")
    logger.info(f"  Total descartados  : {relatorio['total_descartado']} ({relatorio['pct_descartado']:.1f}%)")

    if relatorio["descartes"]:
        logger.info("  Detalhamento dos descartes:")
        for descarte in relatorio["descartes"]:
            logger.info(
                f"    • {descarte['motivo']}: "
                f"{descarte['registros_descartados']} registros ({descarte['percentual']:.1f}%)"
            )
    else:
        logger.info("  ✅ Nenhum descarte realizado — dados limpos!")


# ── Orquestração Principal ────────────────────────────────────────────────────
def transformar() -> Path:
    logger.info("═══ Sprint 2: Saneamento & Qualidade (Trusted) ═══")

    # 1. Leitura eficiente com DuckDB
    df = carregar_raw_com_duckdb(RAW_DIR)

    # 2. Validação do Data Contract
    df = validar_schema(df)

    # 3. Limpeza e tipagem
    df, relatorio = limpar_e_tipar(df)

    # 4. Verificação de qualidade mínima (Engenharia Defensiva)
    verificar_qualidade_minima(relatorio)

    # 5. Relatório de qualidade (rastreabilidade)
    imprimir_relatorio(relatorio)

    # 6. Exportar para Parquet
    caminho = exportar_trusted(df)

    return caminho


if __name__ == "__main__":
    caminho = transformar()
    logger.info("Próximo passo → uv run pipeline.py mart")
