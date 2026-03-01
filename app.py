"""
Sprint 4 - Data App: Emendas Parlamentares
==========================================
Interface interativa para análise de emendas parlamentares.

Conceitos demonstrados:
  - @st.cache_data: evita reprocessamento a cada interação do usuário
  - Session State: mantém estado dos filtros entre re-renders
  - Narrativa de dados: Contexto → Dados → Conclusão → Ação
  - UX em dados: hierarquia visual, progressive disclosure
  - Conexão direta com DuckDB/Parquet (sem pandas intermediário)

Uso:
    uv run streamlit run app.py
    uv run streamlit run app.py --server.port 8501 --server.address 0.0.0.0
"""

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

# ── Configuração da Página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Emendas Parlamentares · Dashboard",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Caminhos ──────────────────────────────────────────────────────────────────
MART_DIR = Path("data/mart")
FEAT_DIR = Path("data/feat")

# Fallback para demo sem banco pré-gerado
TRUSTED_PATH = Path("data/trusted/emendas_trusted.parquet")
RAW_PATH = Path("data/raw/emendas_2023.json")


def _extrair_uf_app(localidade: str) -> str:
    """Mapeia nome completo do estado para sigla de 2 letras."""
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
    nome = str(localidade).replace(" (UF)", "").strip().upper()
    return NOME_PARA_SIGLA.get(nome, "NA")


# ── Cache: Carregamento de Dados ──────────────────────────────────────────────
#
# Conceito: @st.cache_data mantém o resultado em memória.
# Sem cache, cada interação do usuário recalcula tudo — app trava.
# Com cache, o dado é carregado uma vez e reutilizado.
#
@st.cache_data(ttl=3600)  # 1 hora de cache
def carregar_dados() -> pd.DataFrame:
    """Carrega dados da camada mais processada disponível."""

    fato_path = MART_DIR / "fato_emendas.parquet"

    # Prioridade: mart (Parquets do Star Schema) > trusted > raw JSON
    if fato_path.exists():
        fato = pd.read_parquet(fato_path)
        loc_path = MART_DIR / "dim_localidade.parquet"
        fun_path = MART_DIR / "dim_funcao.parquet"
        aut_path = MART_DIR / "dim_autor.parquet"
        if loc_path.exists():
            dim_loc = pd.read_parquet(loc_path)[["localidade_id", "regiao"]]
            fato = fato.merge(dim_loc, on="localidade_id", how="left")
        if fun_path.exists():
            dim_fun = pd.read_parquet(fun_path)[["funcao", "area_tematica"]]
            fato = fato.merge(dim_fun, on="funcao", how="left")
        if aut_path.exists():
            dim_aut = pd.read_parquet(aut_path)[["autor_id", "tipo_simplificado"]]
            fato = fato.merge(dim_aut, on="autor_id", how="left")
        return fato

    elif TRUSTED_PATH.exists():
        return pd.read_parquet(TRUSTED_PATH)

    elif RAW_PATH.exists():
        import json
        dados = json.loads(RAW_PATH.read_text(encoding="utf-8"))
        df = pd.DataFrame(dados)
        for col in ["valorEmpenhado", "valorLiquidado", "valorPago"]:
            df[col] = (df[col].str.replace(".", "", regex=False)
                              .str.replace(",", ".", regex=False)
                              .astype(float))
        df["uf"] = df["localidadeDoGasto"].apply(_extrair_uf_app)
        return df

    else:
        st.error("⚠️ Nenhum dado encontrado. Execute o pipeline primeiro.")
        st.code(
            "uv run python src/ingest.py\n"
            "uv run python src/transform.py\n"
            "uv run python src/mart.py\n"
            "uv run python src/feat.py"
        )
        st.stop()


@st.cache_data(ttl=3600)
def calcular_kpis(df: pd.DataFrame) -> dict:
    """Calcula KPIs globais uma vez e cacheia."""
    return {
        "total_emendas": len(df),
        "total_empenhado": df["valorEmpenhado"].sum(),
        "total_pago": df["valorPago"].sum(),
        "taxa_execucao": df["valorPago"].sum() / df["valorEmpenhado"].sum() * 100
                         if df["valorEmpenhado"].sum() > 0 else 0,
        "autores_unicos": df["autor"].nunique(),
        "funcoes": df["funcao"].nunique(),
        "ufs": df["uf"].nunique(),
    }


# ── Sidebar: Filtros ──────────────────────────────────────────────────────────
def renderizar_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtros na sidebar.
    Session State garante que os filtros persistam ao navegar entre abas.
    """
    with st.sidebar:
        st.image("https://www.gov.br/pt-br/imagens/simbolo-do-governo-federal.png/@@images/image", width=120)
        st.title("🏛️ Filtros")
        st.markdown("---")

        # Ano
        anos = sorted(df["ano"].unique())
        ano_sel = st.multiselect(
            "Ano de exercício",
            options=anos,
            default=anos,
            key="filtro_ano",
        )

        # Função orçamentária
        funcoes = sorted(df["funcao"].dropna().unique())
        funcao_sel = st.multiselect(
            "Função orçamentária",
            options=funcoes,
            default=funcoes,
            key="filtro_funcao",
        )

        # Região
        regioes = ["Todas"] + sorted(df["uf"].dropna().unique().tolist())
        uf_sel = st.selectbox("UF / Localidade", options=regioes, key="filtro_uf")

        # Tipo de emenda
        if "tipo_simplificado" in df.columns:
            tipos = sorted(df["tipo_simplificado"].dropna().unique())
            tipo_sel = st.multiselect("Tipo de emenda", options=tipos, default=tipos)
        else:
            tipo_sel = None

        st.markdown("---")
        st.caption("Dados: Portal da Transparência · Gov.br")

    # Aplicar filtros
    mask = (
        df["ano"].isin(ano_sel) &
        df["funcao"].isin(funcao_sel)
    )
    if uf_sel != "Todas":
        mask &= df["uf"] == uf_sel
    if tipo_sel and "tipo_simplificado" in df.columns:
        mask &= df["tipo_simplificado"].isin(tipo_sel)

    return df[mask]


# ── Componentes de Visualização ───────────────────────────────────────────────

def cartao_kpi(coluna, label: str, valor, prefixo: str = "", sufixo: str = "", delta=None):
    """Componente padronizado de KPI card."""
    with coluna:
        st.metric(label=label, value=f"{prefixo}{valor:,.0f}{sufixo}", delta=delta)


def grafico_barras_funcao(df: pd.DataFrame) -> go.Figure:
    """Top funções por valor empenhado com comparativo de execução."""
    agg = (df.groupby("funcao")
             .agg(total_empenhado=("valorEmpenhado", "sum"),
                  total_pago=("valorPago", "sum"))
             .reset_index()
             .sort_values("total_empenhado", ascending=True)
             .tail(10))

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=agg["funcao"], x=agg["total_empenhado"],
        name="Empenhado", orientation="h",
        marker_color="#1f77b4", opacity=0.8,
    ))
    fig.add_trace(go.Bar(
        y=agg["funcao"], x=agg["total_pago"],
        name="Pago", orientation="h",
        marker_color="#2ca02c", opacity=0.9,
    ))
    fig.update_layout(
        barmode="overlay",
        title="Empenhado vs. Pago por Função Orçamentária",
        xaxis_title="Valor (R$)",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=10, r=10, t=50, b=10),
    )
    return fig


def grafico_mapa_uf(df: pd.DataFrame) -> go.Figure:
    """Distribuição geográfica das emendas por UF."""
    agg = (df[df["uf"] != "NA"]
             .groupby("uf")
             .agg(total=("valorEmpenhado", "sum"), qtd=("codigoEmenda", "count"))
             .reset_index())

    fig = px.choropleth(
        agg,
        locations="uf",
        locationmode="geojson-id",
        color="total",
        hover_name="uf",
        hover_data={"total": ":,.0f", "qtd": True},
        color_continuous_scale="Blues",
        title="Distribuição por UF (valor empenhado)",
        labels={"total": "Total Empenhado (R$)", "qtd": "Nº Emendas"},
    )
    # Fallback: gráfico de barras se o choropleth não renderizar
    if agg.empty:
        fig = px.bar(agg.sort_values("total", ascending=False),
                     x="uf", y="total", title="Valor Empenhado por UF")
    return fig


def grafico_execucao(df: pd.DataFrame) -> go.Figure:
    """Scatter: valor empenhado vs taxa de execução — identifica 'emendas vitrine'."""
    if "taxa_execucao_pct" not in df.columns:
        df = df.copy()
        df["taxa_execucao_pct"] = df.apply(
            lambda r: r["valorPago"] / r["valorEmpenhado"] * 100
                      if r["valorEmpenhado"] > 0 else 0, axis=1
        )

    df_plot = df[df["valorEmpenhado"] > 0].copy()

    fig = px.scatter(
        df_plot,
        x="valorEmpenhado",
        y="taxa_execucao_pct",
        color="funcao",
        hover_data=["autor", "localidadeDoGasto"],
        title="Execução Orçamentária: Empenhado vs. % Executado",
        labels={
            "valorEmpenhado": "Valor Empenhado (R$)",
            "taxa_execucao_pct": "Taxa de Execução (%)",
        },
        opacity=0.7,
    )
    # Linha de referência: 80% de execução (critério de "execução satisfatória")
    fig.add_hline(y=80, line_dash="dash", line_color="green",
                  annotation_text="Meta: 80% de execução")
    fig.add_hline(y=10, line_dash="dot", line_color="red",
                  annotation_text="Alerta: possível emenda vitrine (<10%)")
    fig.update_layout(height=400)
    return fig


def tabela_vitrines(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna emendas com baixíssima execução (potenciais 'vitrines')."""
    if "taxa_execucao_pct" not in df.columns:
        df = df.copy()
        df["taxa_execucao_pct"] = df.apply(
            lambda r: r["valorPago"] / r["valorEmpenhado"] * 100
                      if r["valorEmpenhado"] > 0 else 0, axis=1
        )

    vitrines = (df[
            (df["valorEmpenhado"] > 1000) &
            (df["taxa_execucao_pct"] < 10)
        ][["codigoEmenda", "autor", "funcao", "localidadeDoGasto",
           "valorEmpenhado", "valorPago", "taxa_execucao_pct"]]
        .sort_values("valorEmpenhado", ascending=False)
        .rename(columns={
            "codigoEmenda": "Código",
            "autor": "Autor",
            "funcao": "Função",
            "localidadeDoGasto": "Localidade",
            "valorEmpenhado": "Empenhado (R$)",
            "valorPago": "Pago (R$)",
            "taxa_execucao_pct": "Execução (%)",
        }))
    return vitrines


# ── Layout Principal ──────────────────────────────────────────────────────────

def main():
    # ── Cabeçalho ─────────────────────────────────────────────────────────────
    st.title("🏛️ Emendas Parlamentares · Painel de Transparência")
    st.markdown(
        "**O dinheiro público tem endereço.** "
        "Acompanhe para onde vão as emendas parlamentares, "
        "quais áreas recebem mais recursos e onde o dinheiro empenhado não chegou."
    )
    st.markdown("---")

    # ── Dados e Filtros ────────────────────────────────────────────────────────
    with st.spinner("Carregando dados..."):
        df_completo = carregar_dados()

    df = renderizar_sidebar(df_completo)

    if df.empty:
        st.warning("Nenhum dado para os filtros selecionados. Ajuste os filtros na sidebar.")
        return

    kpis = calcular_kpis(df)

    # ── Contexto: KPIs ────────────────────────────────────────────────────────
    st.subheader("📊 Visão Geral")
    c1, c2, c3, c4, c5 = st.columns(5)
    cartao_kpi(c1, "Total de Emendas", kpis["total_emendas"])
    cartao_kpi(c2, "Total Empenhado", kpis["total_empenhado"], prefixo="R$ ")
    cartao_kpi(c3, "Total Pago", kpis["total_pago"], prefixo="R$ ")
    cartao_kpi(c4, "Taxa de Execução", kpis["taxa_execucao"], sufixo="%")
    cartao_kpi(c5, "Parlamentares", kpis["autores_unicos"])

    st.markdown("---")

    # ── Narrativa Aba 1: Para onde vai o dinheiro? ────────────────────────────
    aba1, aba2, aba3 = st.tabs([
        "📍 Para onde vai?",
        "🗺️ Mapa por UF",
        "⚠️ Execução & Vitrines",
    ])

    with aba1:
        st.markdown(
            "**Pergunta:** Quais funções orçamentárias concentram mais recursos? "
            "A diferença entre o empenhado e o pago revela o que foi prometido mas não entregue."
        )
        col1, col2 = st.columns([2, 1])

        with col1:
            st.plotly_chart(grafico_barras_funcao(df), use_container_width=True)

        with col2:
            st.subheader("Top Parlamentares")
            top = (df.groupby("autor")["valorEmpenhado"].sum()
                     .sort_values(ascending=False)
                     .head(10)
                     .reset_index())
            top.columns = ["Parlamentar", "Empenhado (R$)"]
            top["Empenhado (R$)"] = top["Empenhado (R$)"].map("R$ {:,.2f}".format)
            st.dataframe(top, use_container_width=True, hide_index=True)

    with aba2:
        st.markdown(
            "**Pergunta:** A distribuição geográfica é equitativa? "
            "Regiões com menor IDH estão recebendo recursos proporcionais à sua demanda?"
        )
        agg_uf = (df[df["uf"] != "NA"]
                    .groupby("uf")
                    .agg(total=("valorEmpenhado", "sum"), qtd=("codigoEmenda", "count"))
                    .reset_index()
                    .sort_values("total", ascending=False))

        col1, col2 = st.columns([1, 1])
        with col1:
            fig_uf = px.bar(
                agg_uf.head(15),
                x="uf", y="total",
                color="total",
                color_continuous_scale="Blues",
                title="Top 15 UFs por Valor Empenhado",
                labels={"uf": "UF", "total": "Total Empenhado (R$)"},
            )
            st.plotly_chart(fig_uf, use_container_width=True)

        with col2:
            fig_qtd = px.pie(
                agg_uf.head(10),
                names="uf", values="qtd",
                title="Distribuição de Emendas por UF (quantidade)",
            )
            st.plotly_chart(fig_qtd, use_container_width=True)

        st.dataframe(
            agg_uf.rename(columns={"uf": "UF", "total": "Total Empenhado (R$)", "qtd": "Nº Emendas"}),
            use_container_width=True,
            hide_index=True,
        )

    with aba3:
        st.markdown(
            "**Pergunta:** O dinheiro empenhado realmente chega? "
            "Emendas com menos de 10% de execução são candidatas a 'emendas vitrine' — "
            "anunciadas para o eleitor, mas que ficam anos em restos a pagar."
        )
        col1, col2 = st.columns([2, 1])

        with col1:
            st.plotly_chart(grafico_execucao(df), use_container_width=True)

        with col2:
            vitrines = tabela_vitrines(df)
            n_vitrines = len(vitrines)

            # Calcula o total ANTES de formatar — nunca parsear string de display
            total_vitrine_valor = (
                df[
                    (df["valorEmpenhado"] > 1000) &
                    (df["taxa_execucao_pct"] < 10)
                ]["valorEmpenhado"].sum()
                if not vitrines.empty else 0
            )

            st.metric("Possíveis Emendas Vitrine", n_vitrines)
            st.metric(
                "Recursos Comprometidos",
                f"R$ {total_vitrine_valor:,.0f}",
                help="Total empenhado em emendas com menos de 10% de execução"
            )

        if not vitrines.empty:
            st.subheader("⚠️ Emendas com Baixíssima Execução (<10%)")
            st.dataframe(vitrines, use_container_width=True, hide_index=True)
        else:
            st.success("✅ Nenhuma emenda vitrine identificada nos dados filtrados.")

    # ── Rodapé ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.caption(
        "Fonte: Portal da Transparência do Governo Federal · "
        "Dados: emendas parlamentares ao Orçamento da União · "
        "Pipeline reprodutível: github.com/seu-usuario/emendas-parlamentares"
    )


if __name__ == "__main__" or True:
    main()
