"""
=============================================================
ETAPA 3: VISUALIZACIÓN — Gráficas profesionales
=============================================================
Genera 6 visualizaciones de alta calidad:
  1. Heatmap de demanda hora × día de semana
  2. Revenue mensual con tendencia y estacionalidad
  3. Top 20 zonas — barras horizontales con KPIs inline
  4. Métodos de pago — donut + comparativa avanzada
  5. Distribución de tarifas por método de pago (violin)
  6. Dashboard ejecutivo con KPIs y minigrágicas
=============================================================
"""

import os, glob, json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns

BASE_DIR   = "/app"
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "output")
CHARTS_DIR = os.path.join(BASE_DIR, "charts")
os.makedirs(CHARTS_DIR, exist_ok=True)

# ── Paleta NYC ────────────────────────────────────────────────
P = {
    "bg":      "#0D1117",
    "card":    "#161B22",
    "panel":   "#21262D",
    "red":     "#FF6B6B",
    "orange":  "#FFA94D",
    "yellow":  "#FFD43B",
    "green":   "#69DB7C",
    "teal":    "#38D9A9",
    "blue":    "#4DABF7",
    "purple":  "#DA77F2",
    "gray":    "#8B949E",
    "white":   "#F0F6FC",
}

ACCENT = [P["red"], P["orange"], P["blue"], P["purple"], P["green"], P["teal"]]

# colormap personalizado NYC
NYC_CMAP = LinearSegmentedColormap.from_list(
    "nyc", ["#0D1117", "#0F3460", "#E94560", "#FFD43B"])

# Tema global oscuro
plt.rcParams.update({
    "figure.facecolor":  P["bg"],
    "axes.facecolor":    P["card"],
    "axes.edgecolor":    P["panel"],
    "axes.labelcolor":   P["white"],
    "axes.titlecolor":   P["white"],
    "xtick.color":       P["gray"],
    "ytick.color":       P["gray"],
    "text.color":        P["white"],
    "grid.color":        P["panel"],
    "grid.linestyle":    "--",
    "grid.alpha":        0.5,
    "axes.grid":         True,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.spines.left":  False,
    "axes.spines.bottom":False,
    "font.family":       "DejaVu Sans",
    "font.size":         10,
    "figure.dpi":        150,
})


def load_csv(subdir: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(OUTPUT_DIR, subdir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV en {OUTPUT_DIR}/{subdir}/")
    return pd.read_csv(files[0])


def save(fig, name: str):
    path = os.path.join(CHARTS_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=P["bg"])
    plt.close(fig)
    print(f"   ✅ {name}")


def run():
    print("\n  Cargando datos de agregaciones...")
    df_hour    = load_csv("agg_by_hour")
    df_day     = load_csv("agg_by_day")
    df_zone    = load_csv("agg_by_zone")
    df_payment = load_csv("agg_payment")
    df_monthly = load_csv("agg_monthly")

    with open(os.path.join(OUTPUT_DIR, "pipeline_summary.json")) as f:
        summary = json.load(f)

    years = summary.get("years", "2025")
    print(f"  ✅ Datos cargados | {summary['total_trips']:,} viajes | Años: {years}\n")

    # ════════════════════════════════════════════════════════════
    # GRÁFICA 1: Heatmap Hora × Día de Semana
    # ════════════════════════════════════════════════════════════
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.patch.set_facecolor(P["bg"])

    # Construir pivot: horas × días
    dias_orden = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
    df_h = df_hour.copy()
    df_d = df_day.sort_values("pickup_dayofweek")

    # Crear matriz hora×día proporcional (estimada desde los dos datasets)
    # Distribuir viajes por hora ponderados por demanda del día
    hour_total   = df_h.set_index("pickup_hour")["total_trips"]
    day_weights  = df_d.set_index("day_name")["total_trips"] / df_d["total_trips"].sum()
    day_weights  = day_weights.reindex(dias_orden, fill_value=1/7)

    matrix = pd.DataFrame(
        {dia: hour_total * w for dia, w in day_weights.items()},
        index=hour_total.index
    ).reindex(range(24)).fillna(0)

    sns.heatmap(
        matrix[dias_orden].T / 1000,
        ax=ax,
        cmap=NYC_CMAP,
        linewidths=0.3,
        linecolor=P["bg"],
        annot=False,
        fmt=".0f",
        cbar_kws={"label": "Miles de viajes (est.)", "shrink": 0.8},
    )
    ax.set_xlabel("Hora del día", fontsize=11, labelpad=8)
    ax.set_ylabel("Día de semana", fontsize=11, labelpad=8)
    ax.set_title(
        f"NYC Taxi {years} — Mapa de Calor: Demanda por Hora y Día",
        fontsize=14, fontweight="bold", pad=15, color=P["white"]
    )
    ax.set_xticks(range(0, 24, 2))
    ax.set_xticklabels([f"{h:02d}h" for h in range(0, 24, 2)], fontsize=8)
    ax.tick_params(axis="both", length=0)

    # Marcar horas pico
    for h in [7, 8, 9, 16, 17, 18, 19]:
        ax.axvline(x=h, color=P["orange"], alpha=0.4, linewidth=1.5)

    fig.tight_layout()
    save(fig, "01_heatmap_demanda.png")

    # ════════════════════════════════════════════════════════════
    # GRÁFICA 2: Revenue mensual con tendencia
    # ════════════════════════════════════════════════════════════
    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.patch.set_facecolor(P["bg"])
    fig.suptitle(
        f"NYC Taxi {years} — Evolución Mensual de Viajes y Revenue",
        fontsize=14, fontweight="bold", color=P["white"], y=0.98
    )

    df_m = df_monthly.copy()
    df_m["periodo"] = df_m["pickup_year"].astype(str) + "-" + df_m["pickup_month"].apply(lambda x: f"{x:02d}")
    df_m = df_m.sort_values(["pickup_year","pickup_month"])

    season_colors = {"Invierno": P["blue"], "Primavera": P["green"],
                     "Verano": P["orange"], "Otoño": P["red"]}
    bar_colors = [season_colors.get(s, P["gray"]) for s in df_m["season"]]

    # Panel superior: viajes
    axes[0].bar(range(len(df_m)), df_m["total_trips"]/1e6,
                color=bar_colors, alpha=0.85, width=0.7, edgecolor=P["bg"], linewidth=0.3)
    # Línea de tendencia
    z = np.polyfit(range(len(df_m)), df_m["total_trips"]/1e6, 1)
    p = np.poly1d(z)
    axes[0].plot(range(len(df_m)), p(range(len(df_m))),
                 color=P["white"], linewidth=1.5, linestyle="--", alpha=0.6, label="Tendencia")
    axes[0].set_ylabel("Millones de viajes", fontsize=10)
    axes[0].set_title("Volumen mensual de viajes", fontsize=11, pad=8)
    axes[0].legend(fontsize=9)
    axes[0].yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1fM"))

    # Panel inferior: revenue
    axes[1].fill_between(range(len(df_m)), df_m["total_revenue"]/1e6,
                         alpha=0.3, color=P["teal"])
    axes[1].plot(range(len(df_m)), df_m["total_revenue"]/1e6,
                 color=P["teal"], linewidth=2.5, marker="o", markersize=4)
    axes[1].set_ylabel("Revenue (millones USD)", fontsize=10)
    axes[1].set_title("Revenue mensual total", fontsize=11, pad=8)
    axes[1].yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.0fM"))
    axes[1].set_xticks(range(len(df_m)))
    axes[1].set_xticklabels(df_m["periodo"], rotation=45, ha="right", fontsize=8)

    # Leyenda estaciones
    legend_patches = [mpatches.Patch(color=c, label=s) for s, c in season_colors.items()]
    fig.legend(handles=legend_patches, loc="upper right", fontsize=9,
               facecolor=P["card"], edgecolor=P["panel"], ncol=4,
               bbox_to_anchor=(0.98, 0.95))

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    save(fig, "02_evolucion_mensual.png")

    # ════════════════════════════════════════════════════════════
    # GRÁFICA 3: Top 20 zonas — horizontal bars con KPIs
    # ════════════════════════════════════════════════════════════
    fig, axes = plt.subplots(1, 2, figsize=(16, 8), gridspec_kw={"width_ratios": [2, 1]})
    fig.patch.set_facecolor(P["bg"])
    fig.suptitle(f"NYC Taxi {years} — Top 20 Zonas de Mayor Demanda",
                 fontsize=14, fontweight="bold", color=P["white"])

    df_z = df_zone.sort_values("total_pickups", ascending=True)
    norm = plt.Normalize(df_z["total_pickups"].min(), df_z["total_pickups"].max())
    colors_z = NYC_CMAP(norm(df_z["total_pickups"].values))

    bars = axes[0].barh(range(len(df_z)), df_z["total_pickups"]/1000,
                        color=colors_z, edgecolor=P["bg"], linewidth=0.3, height=0.7)

    for i, (bar, rev, tip) in enumerate(zip(
            bars, df_z["avg_revenue_per_trip"], df_z["avg_tip_pct"])):
        w = bar.get_width()
        axes[0].text(w + w*0.01, i, f"${rev:.0f} | {tip:.0f}% tip",
                     va="center", fontsize=7.5, color=P["gray"])

    axes[0].set_yticks(range(len(df_z)))
    axes[0].set_yticklabels([f"Zona {z}" for z in df_z["PULocationID"]], fontsize=9)
    axes[0].set_xlabel("Miles de pickups", fontsize=10)
    axes[0].set_title("Pickups por zona (etiqueta: tarifa prom. | % propina)", fontsize=10)
    axes[0].xaxis.set_major_formatter(mticker.FormatStrFormatter("%.0fK"))

    # Panel derecho: top 5 detalle
    top5 = df_z.tail(5).iloc[::-1]
    for i, (_, row) in enumerate(top5.iterrows()):
        y = i * 1.5
        axes[1].barh(y, row["total_revenue"]/1e6, color=ACCENT[i], height=0.6,
                     edgecolor=P["bg"])
        axes[1].text(0.02, y, f"Zona {row['PULocationID']}",
                     va="center", fontsize=10, fontweight="bold", color=P["white"])
        axes[1].text(row["total_revenue"]/1e6 * 0.95, y,
                     f"${row['total_revenue']/1e6:.1f}M",
                     va="center", ha="right", fontsize=9, color=P["white"])

    axes[1].set_yticks([])
    axes[1].set_xlabel("Revenue total (M USD)", fontsize=10)
    axes[1].set_title("Revenue Top 5 zonas", fontsize=10)
    axes[1].xaxis.set_major_formatter(mticker.FormatStrFormatter("$%.0fM"))

    fig.tight_layout()
    save(fig, "03_top20_zonas.png")

    # ════════════════════════════════════════════════════════════
    # GRÁFICA 4: Métodos de pago — donut + comparativa
    # ════════════════════════════════════════════════════════════
    fig = plt.figure(figsize=(14, 6))
    fig.patch.set_facecolor(P["bg"])
    gs4 = gridspec.GridSpec(1, 3, figure=fig, wspace=0.4)
    fig.suptitle(f"NYC Taxi {years} — Análisis de Métodos de Pago",
                 fontsize=14, fontweight="bold", color=P["white"])

    # Donut
    ax_donut = fig.add_subplot(gs4[0])
    ax_donut.set_facecolor(P["card"])
    pct = df_payment["total_trips"] / df_payment["total_trips"].sum() * 100
    wedges, _ = ax_donut.pie(
        df_payment["total_trips"], colors=ACCENT[:len(df_payment)],
        startangle=90, wedgeprops={"width": 0.55, "edgecolor": P["bg"], "linewidth": 2},
    )
    # Etiquetas con porcentaje
    for wedge, label, p in zip(wedges, df_payment["payment_label"], pct):
        angle = (wedge.theta1 + wedge.theta2) / 2
        x = 0.85 * np.cos(np.radians(angle))
        y = 0.85 * np.sin(np.radians(angle))
        ax_donut.annotate(
            f"{label}\n{p:.1f}%",
            xy=(x*0.7, y*0.7), xytext=(x*1.2, y*1.2),
            ha="center", fontsize=8.5, color=P["white"],
            arrowprops={"arrowstyle": "-", "color": P["gray"], "lw": 0.8},
        )
    ax_donut.text(0, 0, f"{df_payment['total_trips'].sum()/1e6:.1f}M\nviajes",
                  ha="center", va="center", fontsize=11, fontweight="bold",
                  color=P["white"])
    ax_donut.set_title("Distribución\nde métodos de pago", fontsize=10)

    # Barras agrupadas tarifa vs propina
    ax_bar = fig.add_subplot(gs4[1:])
    ax_bar.set_facecolor(P["card"])
    x = np.arange(len(df_payment))
    w = 0.28
    b1 = ax_bar.bar(x - w, df_payment["avg_fare"],  w, label="Tarifa base",    color=P["blue"],   alpha=0.9)
    b2 = ax_bar.bar(x,     df_payment["avg_tip"],   w, label="Propina prom.",  color=P["green"],  alpha=0.9)
    b3 = ax_bar.bar(x + w, df_payment["avg_tip_pct"], w, label="% Propina",    color=P["orange"], alpha=0.9)

    for bars_g in [b1, b2, b3]:
        for bar in bars_g:
            h = bar.get_height()
            ax_bar.text(bar.get_x() + bar.get_width()/2, h + 0.1,
                        f"{h:.1f}", ha="center", va="bottom", fontsize=8, color=P["gray"])

    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(df_payment["payment_label"], fontsize=10)
    ax_bar.set_ylabel("USD / Porcentaje", fontsize=10)
    ax_bar.set_title("Tarifa, propina y % propina por método de pago", fontsize=10)
    ax_bar.legend(fontsize=9, facecolor=P["panel"], edgecolor=P["panel"])

    fig.tight_layout()
    save(fig, "04_metodos_pago.png")

    # ════════════════════════════════════════════════════════════
    # GRÁFICA 5: Análisis hora del día — versión mejorada
    # ════════════════════════════════════════════════════════════
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.patch.set_facecolor(P["bg"])
    fig.suptitle(f"NYC Taxi {years} — Demanda y Propinas por Hora del Día",
                 fontsize=14, fontweight="bold", color=P["white"])

    rush_pal = {
        "morning_rush": P["red"],
        "evening_rush": P["purple"],
        "overnight":    P["blue"],
        "normal":       P["orange"],
    }
    colors_h = [rush_pal.get(r, P["orange"]) for r in df_hour["is_rush_hour"]]

    # Barras con gradiente visual
    bars = axes[0].bar(df_hour["pickup_hour"], df_hour["total_trips"]/1000,
                       color=colors_h, width=0.8, edgecolor=P["bg"], linewidth=0.3)
    # Línea de media
    mean_trips = df_hour["total_trips"].mean()/1000
    axes[0].axhline(mean_trips, color=P["white"], linewidth=1, linestyle="--", alpha=0.5,
                    label=f"Media: {mean_trips:.0f}K")
    axes[0].set_xlabel("Hora del día", fontsize=10)
    axes[0].set_ylabel("Miles de viajes", fontsize=10)
    axes[0].set_title("Volumen de viajes por hora", fontsize=11)
    axes[0].set_xticks(range(0, 24, 2))
    axes[0].set_xticklabels([f"{h:02d}h" for h in range(0, 24, 2)])
    axes[0].legend(handles=[
        mpatches.Patch(color=P["red"],    label="Rush mañana (7–9h)"),
        mpatches.Patch(color=P["purple"], label="Rush tarde (16–19h)"),
        mpatches.Patch(color=P["orange"], label="Normal"),
        mpatches.Patch(color=P["blue"],   label="Nocturno (0–5h)"),
        mpatches.Patch(color="none",      label=f"Media: {mean_trips:.0f}K"),
    ], fontsize=8, facecolor=P["panel"], edgecolor=P["panel"])

    # Scatter: duración vs propina por hora (tamaño = viajes)
    sc = axes[1].scatter(
        df_hour["avg_duration_min"],
        df_hour["avg_tip_pct"],
        s=df_hour["total_trips"]/df_hour["total_trips"].max()*400 + 30,
        c=df_hour["pickup_hour"],
        cmap="plasma",
        alpha=0.85,
        edgecolors=P["bg"],
        linewidths=0.5,
    )
    for _, row in df_hour.iterrows():
        if row["total_trips"] > df_hour["total_trips"].quantile(0.75):
            axes[1].annotate(f"{int(row['pickup_hour'])}h",
                             (row["avg_duration_min"], row["avg_tip_pct"]),
                             fontsize=7, color=P["gray"],
                             xytext=(3, 3), textcoords="offset points")
    cbar = fig.colorbar(sc, ax=axes[1], shrink=0.8)
    cbar.set_label("Hora del día", color=P["white"])
    cbar.ax.yaxis.set_tick_params(color=P["gray"])
    axes[1].set_xlabel("Duración promedio (min)", fontsize=10)
    axes[1].set_ylabel("% Propina promedio", fontsize=10)
    axes[1].set_title("Duración vs % propina (tamaño = volumen de viajes)", fontsize=10)

    fig.tight_layout()
    save(fig, "05_analisis_hora.png")

    # ════════════════════════════════════════════════════════════
    # GRÁFICA 6: Dashboard ejecutivo
    # ════════════════════════════════════════════════════════════
    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(P["bg"])
    gs6 = gridspec.GridSpec(3, 4, figure=fig, hspace=0.55, wspace=0.35,
                            left=0.05, right=0.97, top=0.88, bottom=0.05)

    fig.text(0.5, 0.94,
             f"NYC TAXI PIPELINE — DASHBOARD EJECUTIVO  |  Años: {years}",
             ha="center", fontsize=16, fontweight="bold", color=P["white"])
    fig.text(0.5, 0.91,
             f"Dataset real TLC NYC  •  {summary['total_trips']:,} viajes  •  Procesado con Apache Spark",
             ha="center", fontsize=10, color=P["gray"])

    # ── Fila 1: 4 KPI cards ──────────────────────────────────────
    kpis = [
        ("🚕 Viajes totales",   f"{summary['total_trips']/1e6:.2f}M",    P["red"]),
        ("💰 Revenue total",    f"${summary['total_revenue_usd']/1e6:.0f}M", P["orange"]),
        ("💵 Tarifa promedio",  f"${summary['avg_fare']:.2f}",           P["yellow"]),
        ("💳 Propina promedio", f"{summary['avg_tip_pct']:.1f}%",        P["green"]),
    ]
    for col, (label, value, color) in enumerate(kpis):
        ax = fig.add_subplot(gs6[0, col])
        ax.set_facecolor(P["card"])
        ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")
        ax.add_patch(plt.Rectangle((0,0),1,1, fill=False,
                                   edgecolor=color, linewidth=2, transform=ax.transAxes))
        ax.text(0.5, 0.62, value, ha="center", va="center",
                fontsize=18, fontweight="bold", color=color)
        ax.text(0.5, 0.22, label, ha="center", va="center",
                fontsize=9, color=P["gray"])

    # ── Fila 1-2 col 0-1: Mini línea mensual ─────────────────────
    ax_ml = fig.add_subplot(gs6[1:3, 0:2])
    ax_ml.set_facecolor(P["card"])
    df_m2 = df_monthly.sort_values(["pickup_year","pickup_month"])
    df_m2["idx"] = range(len(df_m2))
    ax_ml.fill_between(df_m2["idx"], df_m2["total_trips"]/1e6,
                       alpha=0.2, color=P["teal"])
    ax_ml.plot(df_m2["idx"], df_m2["total_trips"]/1e6,
               color=P["teal"], linewidth=2, marker="o", markersize=4)
    ax_ml.set_xticks(df_m2["idx"])
    labels_m = df_m2.apply(lambda r: f"{r['pickup_year']}\n{int(r['pickup_month']):02d}", axis=1)
    ax_ml.set_xticklabels(labels_m, fontsize=6)
    ax_ml.set_ylabel("M viajes", fontsize=9)
    ax_ml.set_title("Tendencia mensual de viajes", fontsize=10, pad=6)
    ax_ml.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1fM"))

    # ── Fila 1-2 col 2: Mini barras por día ──────────────────────
    ax_day = fig.add_subplot(gs6[1:3, 2])
    ax_day.set_facecolor(P["card"])
    df_d2 = df_day.sort_values("pickup_dayofweek")
    wknd = [P["red"] if w else P["blue"] for w in df_d2["is_weekend"]]
    ax_day.barh(df_d2["day_name"], df_d2["total_trips"]/1000,
                color=wknd, edgecolor=P["bg"], linewidth=0.3)
    ax_day.set_xlabel("K viajes", fontsize=8)
    ax_day.set_title("Viajes por día\n(rojo=fin de semana)", fontsize=10, pad=6)
    ax_day.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.0fK"))
    ax_day.tick_params(labelsize=8)

    # ── Fila 1-2 col 3: Mini donut pago ──────────────────────────
    ax_do = fig.add_subplot(gs6[1:3, 3])
    ax_do.set_facecolor(P["card"])
    ax_do.pie(
        df_payment["total_trips"],
        labels=[f"{l}\n{t/df_payment['total_trips'].sum()*100:.0f}%"
                for l, t in zip(df_payment["payment_label"], df_payment["total_trips"])],
        colors=ACCENT[:len(df_payment)],
        startangle=90,
        wedgeprops={"width": 0.5, "edgecolor": P["bg"], "linewidth": 1.5},
        textprops={"fontsize": 7.5, "color": P["white"]},
    )
    ax_do.set_title("Métodos de pago", fontsize=10, pad=6)

    fig.tight_layout(rect=[0, 0, 1, 0.90])
    save(fig, "06_dashboard_ejecutivo.png")

    print(f"\n  🎉 6 gráficas guardadas en {CHARTS_DIR}/")
    for name in [
        "01_heatmap_demanda.png",
        "02_evolucion_mensual.png",
        "03_top20_zonas.png",
        "04_metodos_pago.png",
        "05_analisis_hora.png",
        "06_dashboard_ejecutivo.png",
    ]:
        print(f"     • {name}")
