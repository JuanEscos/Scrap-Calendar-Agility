#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_eventosproxUnion.py (enriquecido)
- Carga participantes (json/csv) y, si está disponible, eventos del 01/02.
- Rellena campos vacíos (N/D o "") desde el 01/02: PruebaNom, Organiza, Lugar, Fechas.
- Prioridad de entradas (participantes):
    1) participantes.json / participants.json
    2) participantes_*.json / participants_*.json
    3) participantes_procesado_*.csv / 03participantes_*.csv / participants_*.csv
- Para eventos, busca dentro de la ruta indicada: 01events.json, 01events_last.json,
  events.json, competiciones_agility.json, competiciones_detalladas.json.
"""

import os, sys, json, csv, re
from glob import glob
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin

BASE = "https://www.flowagility.com"

# ----------- Args / ENV -----------
OUT_DIR_DEFAULT = "./output"
FINAL_OUT_DEFAULT = "./output/participants_completos_final.json"

def arg_or_default(i, default):
    return sys.argv[i] if len(sys.argv) > i and str(sys.argv[i]).strip() else default

OUT_DIR   = os.getenv("OUT_DIR", OUT_DIR_DEFAULT)
IN_PATH   = arg_or_default(1, OUT_DIR)                     # participantes: archivo o carpeta
FINAL_OUT = arg_or_default(2, FINAL_OUT_DEFAULT)           # salida final
EVENTS_IN = arg_or_default(3, os.getenv("EVENTS_IN_PATH", ""))  # carpeta o archivo con 01/02

def is_file(p): return os.path.isfile(p)
def is_dir(p):  return os.path.isdir(p)
def ext(p):     return os.path.splitext(p.lower())[1]

def newest(paths):
    if not paths: return None
    def date_key(p):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(p))
        return m.group(1) if m else "0000-00-00"
    return sorted(paths, key=lambda p: (date_key(p), os.path.getmtime(p)))[-1]

# ----------- Load helpers -----------
def load_json_list(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    raise ValueError(f"JSON inesperado en {path}. Esperaba lista o {{'records':[...]}}")

def load_csv_rows(path):
    rows=[]
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({k:(v.strip() if isinstance(v,str) else v) for k,v in row.items()})
    return rows

def glob_recursive(d, *patterns):
    out=[]
    for pat in patterns:
        out += glob(os.path.join(d, "**", pat), recursive=True)
    return out

# ----------- Buscar PARTICIPANTES -----------
def list_participants_inputs(d):
    d = os.path.abspath(d)
    # 1) Consolidado
    consolidated = glob_recursive(d, "participantes.json", "participants.json")
    if consolidated:
        return {"json_one": newest(consolidated)}
    # 2) JSONs múltiples
    json_list = glob_recursive(d, "participantes_*.json", "participants_*.json")
    json_list = [p for p in json_list if not re.search(r"progress|tmp|test", os.path.basename(p), re.I)]
    if json_list:
        return {"json_many": sorted(set(json_list))}
    # 3) CSVs
    csv_list = glob_recursive(
        d,
        "participantes_procesado_*.csv",
        "03participantes_*.csv",
        "participants_procesado_*.csv",
        "participants_*.csv",
        "participantes_*.csv",
    )
    csv_list = [p for p in csv_list if not re.search(r"progress|events|tmp|test", os.path.basename(p), re.I)]
    if csv_list:
        return {"csv_many": sorted(set(csv_list))}
    return {}

def load_participants(in_path):
    if is_file(in_path):
        return (load_json_list(in_path) if ext(in_path)==".json" else load_csv_rows(in_path)), [in_path]
    if is_dir(in_path):
        found = list_participants_inputs(in_path)
        used=[]
        if "json_one" in found:
            p = found["json_one"]; used.append(p)
            return load_json_list(p), used
        if "json_many" in found:
            all_rows=[]
            for p in found["json_many"]:
                try:
                    all_rows += load_json_list(p); used.append(p)
                except Exception as e:
                    print(f"[WARN] JSON inválido {p}: {e}")
            return all_rows, used
        if "csv_many" in found:
            all_rows=[]
            for p in found["csv_many"]:
                try:
                    all_rows += load_csv_rows(p); used.append(p)
                except Exception as e:
                    print(f"[WARN] CSV inválido {p}: {e}")
            return all_rows, used
    raise FileNotFoundError(f"No hay entradas de participantes válidas en {in_path}")

# ----------- Buscar EVENTOS (01/02) -----------
UUID_RE = re.compile(r"/zone/events/([0-9a-fA-F-]{36})")
def base_event_url_from_any(url):
    if not isinstance(url, str): return ""
    m = UUID_RE.search(url)
    return f"{BASE}/zone/events/{m.group(1)}" if m else ""

def list_events_inputs(d):
    d = os.path.abspath(d)
    cands = glob_recursive(
        d,
        "01events.json",
        "01events_last.json",
        "events.json",
        "competiciones_agility.json",
        "competiciones_detalladas.json",
    )
    return sorted(set(cands))

def normalize_event_record(ev):
    """
    Devuelve un dict canónico: {event_url, nombre, organizacion, lugar, fechas}
    a partir de distintas estructuras (01 u 02).
    """
    out = {"event_url":"", "nombre":"", "organizacion":"", "lugar":"", "fechas":""}

    # 01_eventosprox.py (extract_event_details):
    enlaces = (ev.get("enlaces") or {}) if isinstance(ev, dict) else {}
    for k in ("info","participantes","runs"):
        u = enlaces.get(k)
        if u and not out["event_url"]:
            out["event_url"] = base_event_url_from_any(u)

    # Algunos JSON pueden traer directamente url base:
    if not out["event_url"]:
        for k in ("event_url","url","url_detalle"):
            out["event_url"] = base_event_url_from_any(str(ev.get(k,"")))

    # Nombre / organización / lugar / fechas en 01:
    out["nombre"]        = ev.get("nombre") or ev.get("titulo") or ""
    out["organizacion"]  = ev.get("organizacion") or ""
    out["lugar"]         = ev.get("lugar") or ""
    out["fechas"]        = ev.get("fechas") or ""

    # 02_detalladas:
    info_g = ev.get("informacion_general") or {}
    out["nombre"] = out["nombre"] or info_g.get("titulo") or ""
    out["lugar"]  = out["lugar"]  or info_g.get("ubicacion_completa") or ""
    out["fechas"] = out["fechas"] or info_g.get("fechas_completas") or ""

    # En ocasiones 02 trae 'organizer' / 'location' / 'dates'
    out["organizacion"] = out["organizacion"] or ev.get("organizer") or ""
    out["lugar"]        = out["lugar"]        or ev.get("location") or ""
    out["fechas"]       = out["fechas"]       or ev.get("dates") or ""

    return out

def load_events_map(events_path):
    """
    Devuelve: dict {event_url_base -> evento_normalizado}
    """
    if not events_path:
        return {}
    paths = []
    if is_file(events_path):
        paths = [events_path]
    elif is_dir(events_path):
        paths = list_events_inputs(events_path)
    else:
        return {}

    # Cogemos el más reciente (si hay varios tipos) o los fundimos
    all_events=[]
    for p in paths:
        try:
            data = load_json_list(p)
            for ev in data:
                all_events.append((normalize_event_record(ev), p))
        except Exception as e:
            print(f"[WARN] Eventos inválidos '{p}': {e}")

    events_by_url = {}
    used_files = set()
    for ev_norm, src in all_events:
        eu = ev_norm["event_url"]
        if not eu:
            continue
        if eu not in events_by_url:
            events_by_url[eu] = ev_norm
            used_files.add(src)
        else:
            # preferir registros más completos
            cur = events_by_url[eu]
            for k in ("nombre","organizacion","lugar","fechas"):
                if not cur.get(k) and ev_norm.get(k):
                    cur[k] = ev_norm[k]
            used_files.add(src)
    return events_by_url, sorted(used_files)

# ----------- Deduplicación -----------
def pick_key(row):
    for keys in [
        ("event_url","BinomID"),
        ("participants_url","BinomID"),
        ("event_url","Dorsal"),
        ("PruebaNom","Guia","Perro"),
        ("event_title","Guía","Perro"),
    ]:
        if all(k in row and (row.get(k) not in (None,"")) for k in keys):
            return "||".join(str(row.get(k,"")) for k in keys)
    try:
        return json.dumps(row, ensure_ascii=False, sort_keys=True)
    except Exception:
        return f"ID::{id(row)}"

def dedupe(records):
    out=[]; seen=set()
    for r in records:
        k = pick_key(r)
        if k in seen: continue
        seen.add(k); out.append(r)
    return out

# ----------- Enriquecimiento desde 01/02 -----------
def is_empty(v):
    if v is None: return True
    s = str(v).strip()
    return s == "" or s.upper() == "N/D"

def enrich_from_events(records, evmap):
    if not evmap: return 0
    touched = 0
    for r in records:
        eu = r.get("event_url") or ""
        eu = base_event_url_from_any(eu)
        if not eu: 
            # a veces solo tenemos participants_url
            eu = base_event_url_from_any(r.get("participants_url",""))
        if not eu or eu not in evmap:
            continue
        ev = evmap[eu]
        # Rellenar si vacío
        if is_empty(r.get("PruebaNom")) and ev.get("nombre"):
            r["PruebaNom"] = ev["nombre"]; touched += 1
        if is_empty(r.get("Organiza")) and ev.get("organizacion"):
            r["Organiza"] = ev["organizacion"]; touched += 1
        if is_empty(r.get("Lugar")) and ev.get("lugar"):
            r["Lugar"] = ev["lugar"]; touched += 1
        if is_empty(r.get("Fechas")) and ev.get("fechas"):
            r["Fechas"] = ev["fechas"]; touched += 1
    return touched

def write_json(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ----------- Main -----------
def main():
    # Participantes
    part_records, part_used = load_participants(IN_PATH)
    if not part_records:
        raise FileNotFoundError(f"No hay datos de participantes en {IN_PATH}")

    before = len(part_records)
    part_records = dedupe(part_records)
    after = len(part_records)

    # Eventos (opcional)
    ev_used = []
    evmap = {}
    if EVENTS_IN and (is_file(EVENTS_IN) or is_dir(EVENTS_IN)):
        evmap, ev_used = load_events_map(EVENTS_IN)
        filled = enrich_from_events(part_records, evmap)
        print(f"[INFO] Enriquecidos {filled} campos desde eventos (01/02).")

    write_json(FINAL_OUT, part_records)

    meta = {
        "_generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_participants_sources": [os.path.relpath(p, ".") for p in part_used],
        "_events_sources": [os.path.relpath(p, ".") for p in ev_used] if ev_used else [],
        "_count_before_dedup": before,
        "_count": after,
        "_output": os.path.relpath(FINAL_OUT, "."),
    }
    print(f"✅ OK -> {FINAL_OUT} (registros: {after}, deduplicados de {before})")
    print("Meta:", json.dumps(meta, ensure_ascii=False))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)
