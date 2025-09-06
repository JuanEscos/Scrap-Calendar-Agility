#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_eventosproxUnion.py
Une participantes (03) + eventos (01) en un único JSON final.

Entrada:
  - Arg1: ruta de PARTICIPANTES (fichero o carpeta con artifacts del 03)
  - Arg2: (opcional) salida final JSON (por compatibilidad con tu workflow)
  - EVENTS_IN_PATH (env, opcional): ruta de EVENTOS del 01 (fichero o carpeta)

Descubrimiento automático:
  - Participantes: como antes (participantes.json / *_procesado_*.csv / etc.)
  - Eventos: busca en ./artifacts01, ./artifacts02, ./artifacts, ./output:
      01events_last.json, 01events.json, competiciones_agility.json,
      competiciones_detalladas.json
Unión:
  - Normaliza base_url de evento (quita /participants_list, /info, query, etc.)
  - Claves posibles: base_url y/o uuid.
  - Completa campos de evento en participantes SOLO si están vacíos o "N/D".
"""

import os, sys, json, csv, re
from glob import glob
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urlunparse

# --------------------- Config / Args ---------------------
OUT_DIR_DEFAULT = "./output"
FINAL_OUT_DEFAULT = "./output/participants_completos_final.json"

def arg_or_default(i, default):
    return sys.argv[i] if len(sys.argv) > i and str(sys.argv[i]).strip() else default

PART_IN_PATH = arg_or_default(1, OUT_DIR_DEFAULT)            # archivo o carpeta (03)
FINAL_OUT    = arg_or_default(2, FINAL_OUT_DEFAULT)          # salida final JSON
EVENTS_IN_PATH = os.getenv("EVENTS_IN_PATH", "")             # archivo o carpeta (01), opcional

FLOW_BASE = "https://www.flowagility.com"
UUID_RE   = re.compile(r"/zone/events/([0-9a-fA-F-]{36})(?:/.*)?$")

# --------------------- Utilidades ------------------------
def is_file(p): return os.path.isfile(p)
def is_dir(p):  return os.path.isdir(p)
def ext(p):     return os.path.splitext(p.lower())[1]

def newest(paths):
    if not paths: return None
    def date_key(p):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(p))
        return m.group(1) if m else "0000-00-00"
    return sorted(paths, key=lambda p: (date_key(p), os.path.getmtime(p)))[-1]

def load_json_list(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # soportar {"records":[...]} o {"data":[...]}
        for k in ("records","data","items"):
            if isinstance(data.get(k), list):
                return data[k]
    raise ValueError(f"JSON inesperado en {path} (se esperaba lista).")

def load_csv_rows(path):
    rows=[]; hdr=None
    with open(path, newline="", encoding="utf-8-sig") as f:
        r=csv.DictReader(f)
        hdr = r.fieldnames
        for row in r:
            rows.append({k:(v.strip() if isinstance(v,str) else v) for k,v in row.items()})
    return rows, hdr or []

def glob_many(base_dir, *patterns, recursive=True):
    out=[]
    for pat in patterns:
        out += glob(str(Path(base_dir) / ("**/" + pat) if recursive else pat), recursive=recursive)
    return out

def newest_if_any(*globs):
    matches=[]
    for g in globs:
        matches += g
    return newest(matches) if matches else None

def write_json(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# --------------------- Normalización URLs/Keys ------------------------
def strip_query(u):
    try:
        pr = urlparse(u)
        pr = pr._replace(query="", fragment="")
        return urlunparse(pr)
    except Exception:
        return u

def base_event_url(u):
    """Normaliza a https://.../zone/events/<uuid> si se puede."""
    if not u: return ""
    u = strip_query(u.strip())
    u = re.sub(r"/+(participants_list|info|register|results|inscrip.*)/*$", "", u, flags=re.I)
    m = UUID_RE.search(u)
    if m:
        return f"{FLOW_BASE}/zone/events/{m.group(1)}"
    return u.rstrip("/")

def uuid_from_any(d):
    if not isinstance(d, dict): return ""
    # directo
    if isinstance(d.get("uuid"), str) and d["uuid"]:
        return d["uuid"]
    # desde enlaces/url
    for k in ("event_url","url","url_event","enlace","link"):
        v = d.get(k)
        if isinstance(v, str) and v:
            m = UUID_RE.search(v)
            if m: return m.group(1)
    # enlaces dict
    if isinstance(d.get("enlaces"), dict):
        for k,v in d["enlaces"].items():
            if isinstance(v, str):
                m = UUID_RE.search(v)
                if m: return m.group(1)
    return ""

def any_event_url(d):
    if not isinstance(d, dict): return ""
    for k in ("event_url","url","url_event","link","enlace"):
        v = d.get(k)
        if isinstance(v, str) and v:
            return v
    # enlaces dict
    if isinstance(d.get("enlaces"), dict):
        for k,v in d["enlaces"].items():
            if isinstance(v, str) and v:
                return v
    return ""

# --------------------- Descubrimiento PARTICIPANTES (03) ------------------------
def list_part_inputs(d):
    d = os.path.abspath(d)
    # 1) Consolidado
    cons = glob_many(d, "participantes.json", "participants.json")
    if cons: return {"type":"json_one", "paths":[newest(cons)]}
    # 2) JSONs versionados
    jlist = glob_many(d, "participantes_*.json", "participants_*.json")
    jlist = [p for p in jlist if not re.search(r"progress|tmp|test", os.path.basename(p), re.I)]
    if jlist: return {"type":"json_many", "paths":sorted(set(jlist))}
    # 3) CSVs
    clist = glob_many(d,
        "participantes_procesado_*.csv",
        "03participantes_*.csv",
        "participants_procesado_*.csv",
        "participants_*.csv",
        "participantes_*.csv"
    )
    clist = [p for p in clist if not re.search(r"progress|events|tmp|test", os.path.basename(p), re.I)]
    if clist: return {"type":"csv_many", "paths":sorted(set(clist))}
    return {"type":"none", "paths":[]}

def load_participants(in_path):
    if is_file(in_path):
        if ext(in_path)==".json":
            return load_json_list(in_path), [in_path]
        if ext(in_path)==".csv":
            rows,_ = load_csv_rows(in_path)
            return rows, [in_path]
        raise ValueError(f"Extensión no soportada (03): {in_path}")
    if is_dir(in_path):
        info = list_part_inputs(in_path)
        used=[]
        if info["type"]=="json_one":
            p = info["paths"][0]; used.append(p)
            return load_json_list(p), used
        if info["type"]=="json_many":
            all_rows=[]
            for p in info["paths"]:
                used.append(p)
                all_rows += load_json_list(p)
            return all_rows, used
        if info["type"]=="csv_many":
            all_rows=[]; all_hdr=set(); files=[]
            for p in info["paths"]:
                rows,h = load_csv_rows(p)
                files.append((rows,h)); used.append(p)
                for r in rows: all_hdr.update(r.keys())
            norm=[]
            for rows,_ in files:
                for r in rows:
                    norm.append({h:r.get(h,"") for h in all_hdr})
            return norm, used
        return [], used
    raise FileNotFoundError(f"No existe ruta (03): {in_path}")

# --------------------- Descubrimiento EVENTOS (01) ------------------------
def find_events_file(preferred=None):
    # 1) Si nos dan ruta explícita (fichero o carpeta)
    search_dirs = []
    if preferred:
        if is_file(preferred):
            return preferred
        if is_dir(preferred):
            search_dirs.append(preferred)

    # 2) Rutas por defecto donde suelen caer artifacts
    for d in ("./artifacts01","./artifacts02","./artifacts","./output","."):
        if os.path.isdir(d):
            search_dirs.append(d)

    candidates=[]
    pats = [
        "01events_last.json",
        "01events.json",
        "competiciones_agility.json",
        "competiciones_detalladas.json",
        "events.json",
    ]
    for d in search_dirs:
        for p in pats:
            candidates += glob_many(d, p, recursive=True)

    # filtra duplicados/irrelevantes
    candidates = [p for p in candidates if re.search(r"\.json$", p, re.I)]
    return newest(sorted(set(candidates))) if candidates else None

def load_events(events_path_or_dir=None):
    path = None
    if events_path_or_dir:
        if is_file(events_path_or_dir):
            path = events_path_or_dir
        elif is_dir(events_path_or_dir):
            path = find_events_file(events_path_or_dir)
    if not path:
        path = find_events_file(None)
    if not path:
        return [], []
    rows = load_json_list(path)
    return rows, [path]

# --------------------- Índices de eventos ------------------------
def build_event_index(ev_rows):
    """
    Devuelve:
      - by_base_url: dict base_url -> evento
      - by_uuid:     dict uuid -> evento
    """
    by_base = {}
    by_uid  = {}
    for e in ev_rows:
        try:
            uid = uuid_from_any(e)
            url = any_event_url(e)
            base = base_event_url(url) if url else ""
            if base:
                by_base.setdefault(base, e)
            if uid:
                by_uid.setdefault(uid, e)
        except Exception:
            continue
    return by_base, by_uid

# --------------------- Mapeo de campos evento ------------------------
def get_ev_title(e):
    for k in ("nombre","name","title","evento","prueba","PruebaNom"):
        v = e.get(k); 
        if isinstance(v,str) and v.strip(): return v.strip()
    return ""

def get_ev_org(e):
    for k in ("organiza","organizador","organizer","Organiza","club","Club"):
        v = e.get(k)
        if isinstance(v,str) and v.strip(): return v.strip()
    # a veces viene en bloques
    org = e.get("organizacion") or e.get("organization")
    if isinstance(org,str) and org.strip(): return org.strip()
    return ""

def get_ev_loc(e):
    # 1) un campo ya montado
    for k in ("lugar","ubicacion","ubicación","Lugar","location","loc"):
        v = e.get(k)
        if isinstance(v,str) and v.strip(): return v.strip()
    # 2) composicion ciudad / pais / provincia
    parts=[]
    for k in ("ciudad","city","localidad","municipio","provincia","region","country","pais","país"):
        v = e.get(k)
        if isinstance(v,str) and v.strip(): parts.append(v.strip())
    if parts: return " / ".join(dict.fromkeys(parts))
    return ""

def get_ev_dates(e):
    for k in ("fechas","fecha","rango_fechas","dates","date_range","Fechas"):
        v = e.get(k)
        if isinstance(v,str) and v.strip(): return v.strip()
    return ""

def maybe_fill(row, key, val):
    """Rellena si está vacío o 'N/D'."""
    cur = row.get(key)
    if cur is None or (isinstance(cur, str) and cur.strip() in ("", "N/D")):
        if isinstance(val, str) and val.strip():
            row[key] = val.strip()

# --------------------- Dedupe ------------------------
def pick_key(row):
    prefer_keys = [
        ("event_url","BinomID"),
        ("participants_url","BinomID"),
        ("event_url","Dorsal"),
        ("PruebaNom","Guia","Perro"),
        ("event_title","Guía","Perro"),
    ]
    for keys in prefer_keys:
        if all(k in row and (row.get(k) not in (None,"")) for k in keys):
            parts = [str(row.get(k,"")) for k in keys]
            return f"{'|'.join(keys)}::" + "||".join(parts)
    try:
        return "RAW::" + json.dumps(row, ensure_ascii=False, sort_keys=True)
    except Exception:
        return f"ID::{id(row)}"

def dedupe(records):
    out=[]; seen=set()
    for r in records:
        k = pick_key(r)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out

# --------------------- MAIN ------------------------
def main():
    # 1) Participantes (03)
    part_rows, part_used = load_participants(PART_IN_PATH)
    if not part_rows:
        raise FileNotFoundError(f"No hay entradas válidas de participantes en '{PART_IN_PATH}'.")

    # 2) Eventos (01)
    ev_rows, ev_used = load_events(EVENTS_IN_PATH)
    if not ev_rows:
        print("[WARN] No se encontró JSON de eventos (01). Se exporta sin cruce.")
        merged = dedupe(part_rows)
        write_json(FINAL_OUT, merged)
        print(f"✅ OK -> {FINAL_OUT} (sin cruce).")
        print("Fuentes participantes:", *part_used, sep="\n  - ")
        return

    by_base, by_uid = build_event_index(ev_rows)

    # 3) Cruce y enriquecimiento
    out=[]
    for r in part_rows:
        # clave por URL base
        p_url = r.get("event_url") or r.get("participants_url") or ""
        base  = base_event_url(p_url) if p_url else ""
        uid   = r.get("event_uuid") or ""
        ev = None
        if base and base in by_base:
            ev = by_base[base]
        elif uid and uid in by_uid:
            ev = by_uid[uid]
        # enriquecer
        if ev:
            maybe_fill(r, "event_url", base or any_event_url(ev))
            maybe_fill(r, "PruebaNom", get_ev_title(ev))
            maybe_fill(r, "Organiza",  get_ev_org(ev))
            maybe_fill(r, "Lugar",     get_ev_loc(ev))
            maybe_fill(r, "Fechas",    get_ev_dates(ev))
        out.append(r)

    out = dedupe(out)
    write_json(FINAL_OUT, out)

    meta = {
        "_generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_count": len(out),
        "_participants_sources": [os.path.relpath(p, start=".") for p in part_used],
        "_events_sources": [os.path.relpath(p, start=".") for p in ev_used],
        "_output": os.path.relpath(FINAL_OUT, start="."),
    }

    print(f"✅ OK -> {FINAL_OUT} (registros: {len(out)})")
    print("Fuentes participantes:", *part_used, sep="\n  - ")
    print("Fuentes eventos:", *ev_used, sep="\n  - ")
    print("Meta:", json.dumps(meta, ensure_ascii=False))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)
