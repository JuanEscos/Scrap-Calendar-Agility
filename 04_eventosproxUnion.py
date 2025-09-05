#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04_eventosproxUnion.py
- Une la/s salida/s de participantes en un único JSON final.
- Entrada: fichero o carpeta (por CLI o env).
- Prioridad:
    1) Consolidado: participantes.json / participants.json
    2) Varios JSON: participantes_*.json / participants_*.json
    3) Varios CSV : participantes_procesado_*.csv / 03participantes_*.csv / participants_*.csv / ...
- Sin dependencias externas (solo stdlib).
"""

import os, sys, json, csv, re
from glob import glob
from pathlib import Path
from datetime import datetime

# --------------------- Config / Args ---------------------
OUT_DIR_DEFAULT = "./output"
FINAL_OUT_DEFAULT = "./output/participants_completos_final.json"

def arg_or_default(i, default):
    return sys.argv[i] if len(sys.argv) > i and str(sys.argv[i]).strip() else default

OUT_DIR = os.getenv("OUT_DIR", OUT_DIR_DEFAULT)
IN_PATH = arg_or_default(1, OUT_DIR)                  # archivo o carpeta
FINAL_OUT = arg_or_default(2, FINAL_OUT_DEFAULT)      # salida final JSON

# --------------------- Utilidades ------------------------
def is_file(p): return os.path.isfile(p)
def is_dir(p):  return os.path.isdir(p)
def ext(p):     return os.path.splitext(p.lower())[1]

def newest(paths):
    """Devuelve el path más reciente por mtime; si hay fecha YYYY-MM-DD, desempata por esa fecha."""
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
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    raise ValueError(f"JSON inesperado en {path}. Se esperaba una lista o {{'records':[ ... ]}}.")

def load_csv_rows(path):
    rows=[]
    with open(path, newline="", encoding="utf-8-sig") as f:
        r=csv.DictReader(f)
        for row in r:
            rows.append({k:(v.strip() if isinstance(v,str) else v) for k,v in row.items()})
    return rows, r.fieldnames if hasattr(r, "fieldnames") else None

def list_inputs_from_dir(d):
    """
    Devuelve una estructura con:
      - consolidated: fichero único (si hay participantes.json / participants.json)
      - json_list: lista de JSONs (si no hay consolidated)
      - csv_list:  lista de CSVs (si no hay JSON)
    Busca recursivamente por si el artefacto se descomprime en subcarpetas.
    """
    d = os.path.abspath(d)
    pats = lambda *ps: [str(p) for p in sum([glob(os.path.join(d, "**", p), recursive=True) for p in ps], [])]

    # 1) Consolidado
    consolidated = pats("participantes.json", "participants.json")
    if consolidated:
        return {"consolidated": newest(consolidated), "json_list": [], "csv_list": []}

    # 2) Conjunto de JSONs
    json_list = pats("participantes_*.json", "participants_*.json")
    # filtra progresos/otros si existieran (por si acaso)
    json_list = [p for p in json_list if not re.search(r"progress|tmp|test", os.path.basename(p), re.I)]

    if json_list:
        return {"consolidated": None, "json_list": sorted(set(json_list)), "csv_list": []}

    # 3) Conjunto de CSVs
    csv_list = pats(
        "participantes_procesado_*.csv",
        "03participantes_*.csv",
        "participants_procesado_*.csv",
        "participants_*.csv",
        "participantes_*.csv"
    )
    csv_list = [p for p in csv_list if not re.search(r"progress|events|tmp|test", os.path.basename(p), re.I)]

    if csv_list:
        return {"consolidated": None, "json_list": [], "csv_list": sorted(set(csv_list))}

    return {"consolidated": None, "json_list": [], "csv_list": []}

def pick_key(row):
    """
    Clave de deduplicación con varias estrategias razonables.
    Devuelve una string que identifica unívocamente el registro si es posible.
    """
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
    # Fallback: JSON canonizado
    try:
        return "RAW::" + json.dumps(row, ensure_ascii=False, sort_keys=True)
    except Exception:
        # Último recurso: por id(obj) -> no deduplica, pero no rompe
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

def write_json(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# --------------------- PIPELINE --------------------------
def consolidate_from_dir(dir_path):
    info = list_inputs_from_dir(dir_path)
    used = []

    # 1) Consolidado directo
    if info["consolidated"]:
        p = info["consolidated"]
        used.append(p)
        return load_json_list(p), used

    # 2) Múltiples JSON
    if info["json_list"]:
        all_rows=[]
        for p in info["json_list"]:
            try:
                rows = load_json_list(p)
                all_rows.extend(rows)
                used.append(p)
            except Exception as e:
                print(f"[WARN] JSON inválido '{p}': {e}")
        return all_rows, used

    # 3) Múltiples CSV
    if info["csv_list"]:
        all_rows=[]
        all_headers=set()
        csv_rows_per_file=[]
        for p in info["csv_list"]:
            try:
                rows, hdr = load_csv_rows(p)
                csv_rows_per_file.append(rows)
                for r in rows:
                    all_headers.update(r.keys())
                used.append(p)
            except Exception as e:
                print(f"[WARN] CSV inválido '{p}': {e}")
        # Normalizar columnas
        norm=[]
        for rows in csv_rows_per_file:
            for r in rows:
                nr={h: r.get(h,"") for h in all_headers}
                norm.append(nr)
        return norm, used

    # Nada encontrado
    return [], used

def load_records(input_path):
    if is_file(input_path):
        if ext(input_path)==".json":
            return load_json_list(input_path), [input_path]
        if ext(input_path)==".csv":
            rows,_ = load_csv_rows(input_path)
            return rows, [input_path]
        raise ValueError(f"Extensión no soportada: {input_path}")

    if is_dir(input_path):
        return consolidate_from_dir(input_path)

    raise FileNotFoundError(f"No existe la ruta: {input_path}")

def main():
    records, used_paths = load_records(IN_PATH)

    if not records:
        raise FileNotFoundError(
            f"No hay entradas válidas en '{IN_PATH}'. "
            "Se buscan: participantes.json, participants.json, participantes_*.json, participants_*.json, "
            "o CSVs como participantes_procesado_*.csv / 03participantes_*.csv / participants_*.csv."
        )

    before = len(records)
    records = dedupe(records)
    after = len(records)

    write_json(FINAL_OUT, records)

    meta = {
        "_generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source_used": [os.path.relpath(p, start=".") for p in used_paths],
        "_count_before_dedup": before,
        "_count": after,
        "_output": os.path.relpath(FINAL_OUT, start="."),
    }

    print(f"✅ OK -> {FINAL_OUT} (registros: {after}, deduplicados de {before})")
    print("Fuentes utilizadas:")
    for p in used_paths:
        print(f"  - {p}")
    print("Meta:", json.dumps(meta, ensure_ascii=False))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)
