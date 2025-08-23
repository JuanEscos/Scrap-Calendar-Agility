# -*- coding: utf-8 -*-
"""
Created on Sat Aug 23 05:42:07 2025

@author: Juan
"""

# -*- coding: utf-8 -*-
import os
import pandas as pd
import folium
from folium.plugins import MarkerCluster, MiniMap, Fullscreen, LocateControl, Search
import unicodedata
from datetime import date
import hashlib
import html

# ---------- Configuración ----------
CSV_PATH = "eventos_agility_2025.csv"
OUTPUT_HTML = "mapa_agility_2025.html"

# ---------- Utilidades ----------
def _norm_ascii_lower(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower().strip()

def find_col(cols, candidates):
    cols_norm = { _norm_ascii_lower(c): c for c in cols }
    for cand in candidates:
        key = _norm_ascii_lower(cand)
        if key in cols_norm:
            return cols_norm[key]
    for k, v in cols_norm.items():
        for cand in candidates:
            if _norm_ascii_lower(cand) in k:
                return v
    return None

def to_float_series(s):
    if s.dtype.kind in "iuf":
        return s.astype(float)
    return (
        s.astype(str)
         .str.replace(",", ".", regex=False)
         .str.replace("º", "", regex=False)
         .str.replace("°", "", regex=False)
         .str.extract(r"([-+]?\d*\.?\d+)")[0]
         .astype(float)
    )

MESES_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
}
def month_label_es(dt: pd.Timestamp) -> str:
    return f"{MESES_ES[int(dt.month)]}-{int(dt.year)}"

def parse_inicio_spanish(series: pd.Series) -> pd.Series:
    es_to_en = {
        "enero": "january", "febrero": "february", "marzo": "march",
        "abril": "april", "mayo": "may", "junio": "june",
        "julio": "july", "agosto": "august",
        "septiembre": "september", "setiembre": "september",
        "octubre": "october", "noviembre": "november", "diciembre": "december",
    }
    s = series.astype(str).str.replace(",", "", regex=False).str.strip()
    s_norm = s.apply(_norm_ascii_lower)
    for es, en in es_to_en.items():
        s_norm = s_norm.str.replace(rf"\b{es}\b", en, regex=True)
    return pd.to_datetime(s_norm, errors="coerce", dayfirst=True, infer_datetime_format=True)

def looks_like_url(val: str) -> bool:
    v = str(val).strip().lower()
    return v.startswith("http://") or v.startswith("https://")

ICON_COLORS = [
    "red","blue","green","purple","orange","darkred","lightcoral","beige",
    "darkblue","darkgreen","cadetblue","indigo","white","pink",
    "lightblue","lightgreen","gray","black","lightgray"
]
def color_for(value: str) -> str:
    if not value or str(value).strip() == "":
        return "blue"
    h = hashlib.md5(str(value).encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(ICON_COLORS)
    return ICON_COLORS[idx]

# ---------- Carga ----------
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"No se encuentra el archivo: {CSV_PATH}")

df = pd.read_csv(CSV_PATH)
if df.empty:
    raise ValueError("El CSV está vacío.")

lat_col = find_col(df.columns, ["lat", "latitude", "latitud"])
lon_col = find_col(df.columns, ["lon", "lng", "long", "longitude", "longitud"])
if not lat_col or not lon_col:
    raise ValueError(f"No encuentro columnas de coordenadas. Detectadas: {list(df.columns)}")
df[lat_col] = to_float_series(df[lat_col])
df[lon_col] = to_float_series(df[lon_col])

if "Inicio" not in df.columns:
    raise ValueError("No se ha encontrado la columna 'Inicio' en el CSV.")

df["_fecha_dt"] = parse_inicio_spanish(df["Inicio"])
df = df[~df["_fecha_dt"].isna()].copy()

today = pd.Timestamp(date.today())
df_future = df[df["_fecha_dt"] > today].copy()
if df_future.empty:
    raise ValueError("No hay competiciones con fecha > hoy tras el filtrado.")

name_col = find_col(
    df_future.columns,
    ["evento", "titulo", "title", "nombre", "localidad", "municipio", "ciudad", "club", "sede", "venue"]
)
color_key_col = (
    find_col(df_future.columns, ["club"]) or
    find_col(df_future.columns, ["organizador", "organizer", "organizador/club"]) or
    find_col(df_future.columns, ["localidad", "municipio", "ciudad", "sede", "venue"]) or
    name_col
)

# ---------- Mapa ----------
center_lat = df_future[lat_col].mean()
center_lon = df_future[lon_col].mean()
m = folium.Map(location=[center_lat, center_lon], zoom_start=6, control_scale=True, tiles=None)

# Base
folium.TileLayer("OpenStreetMap", name="OSM", control=True, show=True).add_to(m)
folium.TileLayer("CartoDB positron", name="Claro", control=True, show=False).add_to(m)
folium.TileLayer("CartoDB dark_matter", name="Oscuro", control=True, show=False).add_to(m)

# Plugins
# (MiniMap y LocateControl los ocultaremos en móvil vía CSS)
MiniMap(toggle_display=True).add_to(m)
LocateControl(position="bottomright").add_to(m)
Fullscreen().add_to(m)

# Título (con clase para CSS responsive)
title_html = """
<div class="map-title" style="
 position: fixed; top: 10px; left: 50%; transform: translateX(-50%);
 z-index: 9999; background: white; padding: 6px 14px;
 border-radius: 12px; box-shadow: 0 2px 6px rgba(0,0,0,.2);
 font-family: system-ui, Segoe UI, Arial, sans-serif; font-size: 16px; font-weight: 600;">
  Próximas Pruebas de Agility
</div>
"""
m.get_root().html.add_child(folium.Element(title_html))

# Capas por meses y “Todos”
df_future["_mes_label"] = df_future["_fecha_dt"].apply(month_label_es)

fg_all = folium.FeatureGroup(name="Todos", show=True)
cl_all = MarkerCluster(name="Cluster Todos")
cl_all.add_to(fg_all)
fg_all.add_to(m)

unique_months = (
    df_future[["_mes_label", "_fecha_dt"]]
    .assign(year=lambda d: d["_fecha_dt"].dt.year, month=lambda d: d["_fecha_dt"].dt.month)
    .drop_duplicates(subset=["year", "month"])
    .sort_values(["year", "month"])
)
groups, clusters = {}, {}
for _, row in unique_months.iterrows():
    label = row["_mes_label"]
    fg = folium.FeatureGroup(name=label, show=True)
    cl = MarkerCluster(name=f"Cluster {label}")
    cl.add_to(fg)
    fg.add_to(m)
    groups[label] = fg
    clusters[label] = cl

exclude_cols = {lat_col, lon_col, "_fecha_dt", "_mes_label"}

url_col_hints = {"url", "enlace", "link", "web", "pagina", "página", "info", "informacion", "información"}
def is_url_column(colname: str) -> bool:
    n = _norm_ascii_lower(colname)
    return any(h in n for h in url_col_hints)

BTN_STYLE = (
    "display:inline-block;padding:6px 10px;border-radius:9999px;"
    "text-decoration:none;background:#2563eb;color:#ffffff;"
    "font-weight:600;font-size:12px;"
)

group_color_map = {}
searchable_cols = [c for c in df_future.columns if c not in exclude_cols]

for _, r in df_future.iterrows():
    lat, lon = float(r[lat_col]), float(r[lon_col])
    tooltip = str(r[name_col]) if name_col and pd.notna(r.get(name_col)) else "Ver detalles"

    group_val = ""
    if color_key_col and pd.notna(r.get(color_key_col)):
        group_val = str(r.get(color_key_col)).strip()
    if not group_val:
        group_val = "(Sin club)"
    icon_color = color_for(group_val)
    group_color_map.setdefault(group_val, icon_color)

    title_parts = []
    for col in searchable_cols:
        val = r.get(col)
        if pd.isna(val):
            continue
        title_parts.append(str(val))
    search_title = " | ".join(title_parts)

    rows_html = []
    for col in df_future.columns:
        if col in exclude_cols:
            continue
        val = r.get(col)
        if pd.isna(val) or (isinstance(val, str) and val.strip() == ""):
            continue
        if looks_like_url(val) or is_url_column(col):
            url = str(val).strip()
            if looks_like_url(url):
                cell = f'<a href="{url}" target="_blank" rel="noopener noreferrer" style="{BTN_STYLE}">Información</a>'
                rows_html.append(f"<tr><th style='text-align:left;padding-right:8px'>{html.escape(col)}</th><td>{cell}</td></tr>")
            else:
                rows_html.append(f"<tr><th style='text-align:left;padding-right:8px'>{html.escape(col)}</th><td>{html.escape(str(val))}</td></tr>")
        else:
            rows_html.append(f"<tr><th style='text-align:left;padding-right:8px'>{html.escape(col)}</th><td>{html.escape(str(val))}</td></tr>")

    if not rows_html:
        rows_html.append("<tr><td>(sin más información)</td></tr>")

    popup_html = f"""
    <div style="font-family:system-ui,Segoe UI,Arial,sans-serif;font-size:12px">
      <table>{''.join(rows_html)}</table>
    </div>
    """

    folium.Marker(
        location=[lat, lon],
        tooltip=tooltip,
        popup=folium.Popup(popup_html, max_width=350),
        icon=folium.Icon(icon="flag", color=icon_color),
        title=search_title
    ).add_to(cl_all)

    label = r["_mes_label"]
    folium.Marker(
        location=[lat, lon],
        tooltip=tooltip,
        popup=folium.Popup(popup_html, max_width=350),
        icon=folium.Icon(icon="flag", color=icon_color),
        title=search_title
    ).add_to(clusters[label])

# Ajuste y capas (colapsadas para móvil)
m.fit_bounds(df_future[[lat_col, lon_col]].values.tolist())
folium.LayerControl(collapsed=True).add_to(m)

# Buscador (colapsado por defecto para ahorrar espacio en móvil)
Search(
    layer=fg_all,
    search_label="title",
    placeholder="Buscar…",
    collapsed=True,        # <- botón en móvil
    search_zoom=10,
    position="topright",
    **{
        "textPlaceholder": "Buscar…",
        "textCancel": "Cancelar",
        "textErr": "No encontrado",
        "hideMarkerOnCollapse": True,
        "minLength": 2,
        "initial": False,
        "autoType": True,
        "caseSensitive": False
    }
).add_to(m)

# Leyenda (colapsable en móvil, visible en escritorio)
def legend_html(mapping: dict) -> str:
    if not mapping:
        return ""
    items = sorted(mapping.items(), key=lambda kv: _norm_ascii_lower(kv[0]))
    rows = []
    for grp, color in items:
        dot = f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{color};margin-right:8px;border:1px solid rgba(0,0,0,.5);"></span>'
        rows.append(f'<div style="margin:2px 0;display:flex;align-items:center;gap:6px;">{dot}<span>{html.escape(grp)}</span></div>')
    return f"""
    <div class="legend-box" style="
      position: fixed; bottom: 12px; left: 12px; z-index: 10000;
      background: rgba(255,255,255,0.98); padding: 10px 12px;
      border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,.25);
      font-family: system-ui, Segoe UI, Arial, sans-serif; font-size: 12px; max-height: 40vh; overflow:auto;">
      <div style="font-weight:700; margin-bottom:6px;">Leyenda (Grupo ↔ Color)</div>
      {''.join(rows)}
    </div>
    <button class="legend-toggle" style="
      position: fixed; bottom: 12px; left: 12px; z-index: 10001;
      padding: 8px 12px; border-radius: 9999px; border: none; background:#111827; color:#fff;
      font-family: system-ui, Segoe UI, Arial, sans-serif; font-size: 12px; font-weight:600; display:none;">
      Leyenda
    </button>
    """
m.get_root().html.add_child(folium.Element(legend_html(group_color_map)))

# CSS + JS responsive
responsive_css = """
<style>
/* Título más compacto en móvil */
@media (max-width: 768px) {
  .map-title { top: 6px !important; padding: 4px 10px !important; font-size: 14px !important; }
  /* Oculta MiniMap y Localizador en móvil */
  .leaflet-control-minimap, .leaflet-control-locate { display: none !important; }
  /* Leyenda oculta por defecto en móvil (se muestra con el botón) */
  .legend-box { display: none; max-width: 80vw; max-height: 30vh; }
  .legend-toggle { display: inline-block; }
}
/* En escritorio, botón de leyenda oculto */
@media (min-width: 769px) {
  .legend-toggle { display: none; }
}
</style>
"""
m.get_root().html.add_child(folium.Element(responsive_css))

toggle_js = """
<script>
(function(){
  function onReady(fn){ if(document.readyState!='loading'){fn()} else {document.addEventListener('DOMContentLoaded', fn)} }
  onReady(function(){
    var legend = document.querySelector('.legend-box');
    var btn = document.querySelector('.legend-toggle');
    if(!legend || !btn) return;

    btn.addEventListener('click', function(){
      if(legend.style.display === 'none' || legend.style.display === ''){
        legend.style.display = 'block';
        btn.textContent = 'Ocultar leyenda';
      } else {
        legend.style.display = 'none';
        btn.textContent = 'Leyenda';
      }
    });

    // En escritorio: aseguramos leyenda visible
    if(window.matchMedia('(min-width: 769px)').matches){
      legend.style.display = 'block';
    } else {
      legend.style.display = 'none';
    }
  });
})();
</script>
"""
m.get_root().html.add_child(folium.Element(toggle_js))

# Guardar
m.save(OUTPUT_HTML)
print(f"Mapa guardado en: {OUTPUT_HTML}")
