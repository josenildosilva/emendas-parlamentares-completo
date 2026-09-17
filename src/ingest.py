"""
Sprint 1 - Ingestão de Dados
============================
Coleta emendas parlamentares do Portal da Transparência.

Conceitos demonstrados:
  - Consumo de API REST com paginação
  - Idempotência: reexecutar não duplica dados (checkpoint por hash)
  - Estrutura Raw imutável (append-only)
  - Variáveis de ambiente com python-dotenv
  - Logging estruturado com loguru

Uso:
    uv run python src/ingest.py                        # anos do .env
    uv run python src/ingest.py --anos 2022 2023       # override dos anos
    uv run python src/ingest.py --force                # ignora checkpoint
"""

import argparse
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from loguru import logger

# ── Configuração ──────────────────────────────────────────────────────────────
load_dotenv()

API_BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_KEY  = os.getenv("API_KEY", "")
RAW_DIR  = Path(os.getenv("RAW_DIR", "data/raw"))
META_DIR = RAW_DIR / ".meta"

# Anos padrão lidos do .env — override possível via --anos na CLI
_anos_env = os.getenv("ANOS", "2023")
ANOS_PADRAO = [int(a.strip()) for a in _anos_env.split(",") if a.strip()]

# Se não há API_KEY, usamos os dados de amostra locais (modo demo)
MODO_DEMO = not API_KEY or API_KEY == "seu_token_aqui"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _hash_arquivo(caminho: Path) -> str:
    """Gera SHA256 do arquivo para checagem de idempotência."""
    sha256 = hashlib.sha256()
    sha256.update(caminho.read_bytes())
    return sha256.hexdigest()


def _carregar_checkpoint(ano: int) -> dict:
    """Lê checkpoint salvo (hash do último arquivo baixado)."""
    checkpoint_path = META_DIR / f".checkpoint_{ano}.json"
    if checkpoint_path.exists():
        return json.loads(checkpoint_path.read_text())
    return {}


def _salvar_checkpoint(ano: int, dados: dict) -> None:
    """Persiste checkpoint para evitar re-download."""
    META_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_path = META_DIR / f".checkpoint_{ano}.json"
    checkpoint_path.write_text(json.dumps(dados, indent=2))


# ── Ingestão via API ──────────────────────────────────────────────────────────

def _buscar_emendas_api(ano: int) -> list[dict]:
    """
    Consome a API do Portal da Transparência com paginação por cursor.

    Conceito: Full Load vs Incremental
    - Full Load: baixa tudo sempre (simples, mas lento para dados grandes)
    - Incremental: usa parâmetro 'dataInicio' como watermark

    Aqui usamos Full Load por ser um dataset anual (estático após o exercício).
    """
    headers = {"chave-api-dados": API_KEY}
    emendas = []
    pagina = 1
    tamanho_pagina = 100  # máximo permitido pela API

    logger.info(f"Iniciando coleta via API para o ano {ano}...")

    while True:
        params = {
            "ano": ano,
            "pagina": pagina,
            "tamanhoPagina": tamanho_pagina,
        }

        try:
            response = requests.get(
                f"{API_BASE}/emendas",
                headers=headers,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            dados_pagina = response.json()

        except requests.HTTPError as e:
            logger.error(f"Erro HTTP na página {pagina}: {e}")
            break
        except requests.RequestException as e:
            logger.error(f"Erro de rede: {e}")
            raise

        if not dados_pagina:
            logger.info(f"Paginação encerrada na página {pagina - 1}")
            break

        emendas.extend(dados_pagina)
        logger.debug(f"Página {pagina}: {len(dados_pagina)} registros coletados")

        # Respeitar rate limit da API
        time.sleep(0.5)
        pagina += 1

    return emendas


def _carregar_dados_demo(ano: int) -> list[dict]:
    """
    Modo demo: usa arquivo local quando não há API_KEY.
    Permite que o projeto funcione sem credenciais para fins pedagógicos.
    """
    logger.warning("⚠️  API_KEY não configurada — usando dados de demonstração locais.")
    logger.warning("   Configure .env com sua chave do Portal da Transparência para dados reais.")

    # A amostra versionada tem nome fixo e previsível. Não usamos glob aqui:
    # depois da primeira execução o Raw contém arquivos com timestamp, e um
    # glob pegaria a saída da rodada anterior em vez da amostra de origem.
    arquivo = RAW_DIR / f"emendas_{ano}_sample.json"
    if arquivo.exists():
        dados = json.loads(arquivo.read_text(encoding="utf-8"))
        logger.info(f"Demo: carregando {len(dados)} registros de '{arquivo.name}'")
        return dados

    logger.error(f"Amostra de demo não encontrada para o ano {ano}: {arquivo}")
    raise FileNotFoundError(
        f"Amostra não encontrada. Esperado: {arquivo}\n"
        f"O repositório versiona emendas_2023_sample.json — para outros anos, "
        f"configure a API_KEY no .env."
    )


# ── Persistência ──────────────────────────────────────────────────────────────

def _inferir_schema(emendas: list[dict]) -> dict:
    """
    Infere o schema dos dados a partir dos registros coletados.

    O exemplo real de cada campo é o detalhe mais valioso — teria revelado
    imediatamente que localidadeDoGasto usava "PERNAMBUCO (UF)" e não "PE".
    """
    if not emendas:
        return {}

    campos = {}
    todos_os_campos = set().union(*[r.keys() for r in emendas])

    for campo in sorted(todos_os_campos):
        valores  = [r.get(campo) for r in emendas]
        nao_nulos = [v for v in valores if v is not None and v != ""]

        tipos = [type(v).__name__ for v in nao_nulos]
        tipo  = max(set(tipos), key=tipos.count) if tipos else "null"

        unicos = list(set(str(v) for v in nao_nulos))
        cardinalidade = len(unicos)

        campos[campo] = {
            "tipo": tipo,
            "total": len(valores),
            "nulos": len(valores) - len(nao_nulos),
            "cardinalidade": cardinalidade,
            "exemplo": nao_nulos[0] if nao_nulos else None,
            "valores_unicos": sorted(unicos) if cardinalidade <= 20 else None,
        }

    return campos


def _salvar_schema(emendas: list[dict], caminho_dados: Path, ano: int) -> Path:
    META_DIR.mkdir(parents=True, exist_ok=True)
    nome_schema = caminho_dados.stem + ".schema.json"
    caminho_schema = META_DIR / nome_schema

    schema = {
        "arquivo_origem": caminho_dados.name,
        "ano": ano,
        "gerado_em": datetime.now().isoformat(),
        "total_registros": len(emendas),
        "total_campos": len(emendas[0]) if emendas else 0,
        "campos": _inferir_schema(emendas),
    }

    caminho_schema.write_text(
        json.dumps(schema, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.success(f"✅ Schema salvo: {caminho_schema}")
    return caminho_schema


def _salvar_raw(emendas: list[dict], ano: int) -> tuple:
    """
    Salva dados na camada Raw (Bronze) e o schema inferido em .meta/.

    Princípio: Raw é IMUTÁVEL e APPEND-ONLY.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"emendas_{ano}_{timestamp}.json"
    caminho = RAW_DIR / nome_arquivo

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    caminho.write_text(
        json.dumps(emendas, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    hash_arquivo = _hash_arquivo(caminho)
    logger.success(f"✅ Raw salvo: {caminho} ({len(emendas)} registros)")
    logger.debug(f"   SHA256: {hash_arquivo[:16]}...")

    _salvar_schema(emendas, caminho, ano)

    return caminho, hash_arquivo


# ── Orquestração ──────────────────────────────────────────────────────────────

def ingerir(ano: int, forcar: bool = False) -> Path:
    """Coleta um ano específico com checagem de idempotência."""
    logger.info(f"═══ Ingestão — Ano {ano} ═══")

    checkpoint = _carregar_checkpoint(ano)
    if checkpoint and not forcar:
        logger.info(
            f"Checkpoint encontrado: {checkpoint.get('arquivo')} "
            f"(coletado em {checkpoint.get('coletado_em')})"
        )
        logger.info("Use --force para forçar nova coleta. Abortando.")
        return Path(checkpoint["arquivo"])

    if MODO_DEMO:
        emendas = _carregar_dados_demo(ano)
    else:
        emendas = _buscar_emendas_api(ano)

    if not emendas:
        logger.error("Nenhum dado coletado. Verifique a API_KEY e o ano informado.")
        raise ValueError("Coleta retornou zero registros")

    logger.info(f"Total coletado: {len(emendas)} emendas")

    caminho, hash_arquivo = _salvar_raw(emendas, ano)

    _salvar_checkpoint(ano, {
        "ano": ano,
        "arquivo": str(caminho),
        "hash": hash_arquivo,
        "registros": len(emendas),
        "coletado_em": datetime.now().isoformat(),
        "modo": "demo" if MODO_DEMO else "api",
    })

    return caminho


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingestão de emendas parlamentares do Portal da Transparência"
    )
    parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        default=ANOS_PADRAO,
        help=f"Anos a coletar (padrão via .env: {ANOS_PADRAO})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Força nova coleta mesmo que checkpoint exista",
    )
    args = parser.parse_args()

    logger.info(f"Anos a processar: {args.anos}")
    for ano in args.anos:
        ingerir(ano=ano, forcar=args.force)

    logger.success("🎉 Ingestão concluída!")
