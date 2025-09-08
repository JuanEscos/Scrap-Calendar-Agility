#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_eventosproxUnion.py  (ENRIQUECIDO)
Une la salida de participantes (03) con la de eventos (01).

Uso:
  python 04_eventosproxUnion.py <PARTS_IN> <EVENTS_IN> <FINAL_OUT>

Donde:
  - PARTS_IN  : archivo o carpeta (participantes.json / participantes_procesado_*.csv / etc.)
  - EVENTS_IN : archivo o carpeta (01events_last.json / 01events_*.json)
  - FINAL_OUT : (opcional) salida JSON. Por defecto ./output/participants_completos_final.json
"""

import os, sys, json, csv, re
from glob import glob
from pathlib import Path
from datetime import datetime

OUT_DIR_DEFAULT = "./output"
FINAL_OUT_DEFAULT = "./output/participants_completos_final.json"

def arg_or_default(i, default):
    return sys.argv[i] if len(sys.argv) > i and str(sys.argv[i]).strip() else default

# --- Args
PARTS_IN  = arg_or_default(1, OUT_DIR_DEFAULT)
EVENTS_IN = arg_or_default(2, OUT_DIR_DEFAULT)
FINAL_OUT = arg_or_default(3, FINAL_OUT_DEFAULT)

def is_file(p): return os.path.isfile(p)
def is_dir(p):  return os.path.isdir(p)
def ext(p):     return os.path.splitext(p.lower())[1]

def newest(paths):
    if not paths: return None
    def date_key(p):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(p))
        return m.group(1) if m else "0000-00-00"
    return sorted(paths, key=lambda p: (date_key(p), os.path.getmtime(p)))[-1]

# ---------- Loaders ----------
def load_json_list(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    raise ValueError(f"JSON inesperado en {path}.")

def load_csv_rows(path):
    rows=[]
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({k:(v.strip() if isinstance(v,str) else v) for k,v in row.items()})
    return rows

def globs_rec(root, *patterns):
    out=[]
    root = os.path.abspath(root)
    for pat in patterns:
        out += glob(os.path.join(root, "**", pat), recursive=True)
    return out

# ---------- Descubrimiento de PARTICIPANTES ----------
def find_participants_sources(root_or_file):
    # archivo directo
    if is_file(root_or_file):
        if ext(root_or_file) in (".json", ".csv"):
            return [root_or_file], ext(root_or_file)
        raise ValueError(f"Extensión no soportada en participantes: {root_or_file}")

    # carpeta
    if is_dir(root_or_file):
        # 1) preferir consolidado JSON
        j = globs_rec(root_or_file, "participantes.json", "participants.json")
        if j: return [newest(j)], ".json"
        # 2) múltiples JSON
        j = globs_rec(root_or_file, "participantes_*.json", "participants_*.json")
        j = [p for p in j if not re.search(r"progress|tmp|test", os.path.basename(p), re.I)]
        if j: return sorted(set(j)), ".json"
        # 3) CSV varios
        c = globs_rec(root_or_file,
                      "participantes_procesado_*.csv",
                      "03participantes_*.csv",
                      "participants_procesado_*.csv",
                      "participants_*.csv",
                      "participantes_*.csv")
        c = [p for p in c if not re.search(r"progress|events|tmp|test", os.path.basename(p), re.I)]
        if c: return sorted(set(c)), ".csv"

    return [], None

def load_participants(root_or_file):
    paths, kind = find_participants_sources(root_or_file)
    if not paths:
        raise FileNotFoundError(
            f"No se encontraron participantes en '{root_or_file}'. "
            "Busca: participantes.json / participantes_*.json / participantes_procesado_*.csv, etc."
        )
    rows=[]
    if kind == ".json":
        for p in paths:
            try:
                rows.extend(load_json_list(p))
            except Exception as e:
                print(f"[WARN] JSON inválido '{p}': {e}")
    elif kind == ".csv":
        for p in paths:
            try:
                rows.extend(load_csv_rows(p))
            except Exception as e:
                print(f"[WARN] CSV inválido '{p}': {e}")
    else:
        raise ValueError("Tipo de participantes desconocido.")
    return rows, paths

# ---------- Descubrimiento de EVENTOS (01) ----------
def find_events_file(root_or_file):
    if is_file(root_or_file):
        return root_or_file if ext(root_or_file)==".json" else None
    if is_dir(root_or_file):
        cands = globs_rec(root_or_file, "01events_last.json", "01events_*.json", "01events.json", "events.json")
        return newest(cands) if cands else None
    return None

def load_events(root_or_file):
    f = find_events_file(root_or_file)
    if not f:
        raise FileNotFoundError(
            f"No se encontró 01events JSON en '{root_or_file}'. "
            "Busca 01events_last.json u 01events_*.json."
        )
    data = load_json_list(f)
    return data, f

# ---------- Utilidades de merge ----------
def norm(s):
    return (s or "").strip()

def first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str) and norm(v):
            return v
    return None

def build_event_indexes(events):
    by_url = {}
    by_uuid = {}
    for e in events:
        url = e.get("event_url") or e.get("enlaces",{}).get("info")
        if url: by_url[norm(url)] = e
        uid = e.get("uuid") or e.get("id")
        if uid: by_uuid[norm(uid)] = e
        # también indexar por url base sin sufijos típicos
        if url:
            u = norm(url)
            u2 = re.sub(r"/(info|participants_list|runs)(/.*)?$", "", u)
            by_url.setdefault(u2, e)
    return by_url, by_uuid

def derive_event_url_from_participants(row):
    # 1) si ya viene
    if norm(row.get("event_url")):
        return norm(row.get("event_url"))
    # 2) desde participants_url
    pu = norm(row.get("participants_url"))
    if not pu:
        return None
    # quitar sufijo /participants_list
    base = re.sub(r"/participants_list/?$", "", pu)
    return base if base else None

def merge_row_with_event(row, ev):
    # Campos 01 → finales
    #  - nombre       -> PruebaNom
    #  - organizacion -> Organiza
    #  - lugar        -> Lugar
    #  - fechas       -> Fechas
    #  - event_url    -> event_url (si falta)
    out = dict(row)  # copia

    # event_url
    if not norm(out.get("event_url")):
        out["event_url"] = ev.get("event_url") or ev.get("enlaces",{}).get("info") or ""

    # PruebaNom
    if not norm(out.get("PruebaNom")) or out.get("PruebaNom") == "N/D":
        out["PruebaNom"] = first_nonempty(
            row.get("title"), row.get("event_title"),
            ev.get("nombre")
        ) or "N/D"

    # Organiza
    if not norm(out.get("Organiza")) or out.get("Organiza") == "N/D":
        out["Organiza"] = first_nonempty(
            row.get("organizer"),
            ev.get("organizacion"),
            ev.get("club")
        ) or "N/D"

    # Lugar
    if not norm(out.get("Lugar")) or out.get("Lugar") == "N/D":
        out["Lugar"] = first_nonempty(
            row.get("location"),
            ev.get("lugar")
        ) or "N/D"

    # Fechas
    if not norm(out.get("Fechas")) or out.get("Fechas") == "N/D":
        out["Fechas"] = first_nonempty(
            row.get("dates"),
            ev.get("fechas")
        ) or "N/D"

    return out

def dedupe(records):
    seen=set(); out=[]
    def key(r):
        # claves razonables en orden
        for ks in [("event_url","BinomID"),
                   ("participants_url","BinomID"),
                   ("event_url","Dorsal"),
                   ("PruebaNom","Guia","Perro"),
                   ("event_title","Guía","Perro")]:
            if all(k in r and norm(r.get(k)) for k in ks):
                return "K::" + "||".join(str(r[k]) for k in ks)
        return "RAW::" + json.dumps(r, ensure_ascii=False, sort_keys=True)
    for r in records:
        k = key(r)
        if k in seen: 
            continue
        seen.add(k); out.append(r)
    return out

# ---------- MAIN ----------
def main():
    # Cargar participantes y eventos
    parts, part_paths = load_participants(PARTS_IN)
    events, events_path = load_events(EVENTS_IN)

    by_url, by_uuid = build_event_indexes(events)

    merged=[]
    not_matched=0
    for r in parts:
        # asegurar event_url derivable
        eu = derive_event_url_from_participants(r)
        if eu: r.setdefault("event_url", eu)

        # buscar evento:
        ev = None
        if eu and eu in by_url:
            ev = by_url[eu]
        else:
            uid = norm(r.get("event_uuid") or r.get("uuid"))
            if uid and uid in by_uuid:
                ev = by_uuid[uid]
            elif eu:
                # probar con base de url
                u2 = re.sub(r"/(info|participants_list|runs)(/.*)?$", "", eu)
                ev = by_url.get(u2)

        if ev:
            merged.append(merge_row_with_event(r, ev))
        else:
            not_matched += 1
            merged.append(r)  # sin enriquecer, pero no lo perdemos

    before = len(merged)
    merged = dedupe(merged)
    after  = len(merged)

    os.makedirs(os.path.dirname(FINAL_OUT) or ".", exist_ok=True)
    with open(FINAL_OUT, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    meta = {
        "_generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_participants_sources": [os.path.relpath(p, start=".") for p in part_paths],
        "_events_source": os.path.relpath(events_path, start="."),
        "_count_before_dedup": before,
        "_count": after,
        "_not_matched": not_matched,
        "_output": os.path.relpath(FINAL_OUT, start="."),
    }
    print(f"✅ Unión completada -> {FINAL_OUT}")
    print(json.dumps(meta, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)
