"""
Orquestrador do pipeline de dados — Emendas Parlamentares.

Uso:
    uv run pipeline.py                  # executa todos os passos
    uv run pipeline.py all              # idem, explícito
    uv run pipeline.py ingest           # apenas ingestão
    uv run pipeline.py ingest --force   # ingestão forçando novo download
    uv run pipeline.py trusted          # apenas saneamento
    uv run pipeline.py mart             # apenas Star Schema
    uv run pipeline.py feat             # apenas Feature Store
"""

import subprocess
import sys

STEPS = {
    "ingest":  ["python", "src/ingest.py"],
    "trusted": ["python", "src/trusted.py"],
    "mart":    ["python", "src/mart.py"],
    "feat":    ["python", "src/feat.py"],
}

PIPELINE = ["ingest", "trusted", "mart", "feat"]


def run(steps: list[str], extra_args: list[str] = []) -> None:
    for nome in steps:
        cmd = STEPS[nome] + extra_args
        print(f"\n{'═' * 50}")
        print(f"▶  {' '.join(cmd)}")
        print(f"{'═' * 50}")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"\n❌ Falhou em '{nome}'. Pipeline interrompido.")
            sys.exit(result.returncode)
    print("\n✅ Pipeline concluído.")


if __name__ == "__main__":
    args = sys.argv[1:]

    # Separa o passo dos argumentos extras (ex: --force)
    passo = args[0] if args and not args[0].startswith("--") else "all"
    extra = [a for a in args if a.startswith("--")]

    if passo == "all":
        run(PIPELINE, extra)
    elif passo in STEPS:
        run([passo], extra)
    else:
        print(f"Passo desconhecido: '{passo}'")
        print(f"Opções: all, {', '.join(STEPS)}")
        sys.exit(1)
