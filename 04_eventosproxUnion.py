#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, csv, re
from glob import glob
from pathlib import Path
from datetime import datetime

# Defaults (puedes sobreescribir por args)
OUT_DIR   = os.getenv("OUT_DIR", "./output")
FINAL_OUT = "./output/participantes_completos_final.json"

def arg_or_default(i, default):
    return sys.argv[i] if len(sys.argv) > i and sys.argv[i].strip() else default

# Arg1: ruta a archivo O directorio. Arg2: salida final
IN_PATH   = arg_or_default(1, OUT_DIR)
FINAL_OUT = arg_or_default(2, FINAL_OUT)

def is_file(p):  return os.path.isfile(p)
def is_dir(p):   return os.path.isdir(p)
def ext(p):      return os.path.splitext(p.lower())[1]

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Si viene como {"records":[...]} lo aplanamos
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError(f"JSON inesperado en {path}")

def load_csv_as_records(path):
    rows=[]
    with open(path, newline="", encoding="utf-8-sig") as f:
        r=csv.DictReader(f)
        for row in r:
            rows.append({k:(v.strip() if isinstance(v,str) else v) for k,v in row.items()})
    return rows

def newest(paths):
    """Devuelve el path más reciente por mtime; si hay fecha YYYY-MM-DD en el nombre, desempata por esa fecha."""
    if not paths: return None
    def date_key(p):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(p))
        return m.group(1) if m else "0000-00-00"
    paths = sorted(paths, key=lambda p: (date_key(p), os.path.getmtime(p)))
    return paths[-1]

def pick_input_from_dir(d):
    # Prioridad de patrones
    patterns = [
        "participantes.json",
        "participantes_*.json",
        "participants.json",
        "participants_*.json",
        "03participantes_*.csv",             # <== NUEVO
        "participantes_procesado_*.csv",
        "participants_procesado_*.csv",
        "participants_*.csv",
        "participantes_*.csv",
    ]
    found=[]
    for pat in patterns:
        found += glob(os.path.join(d, pat))
        if found:
            break
    return newest(found)

def load_records(in_path):
    if is_file(in_path):
        if ext(in_path) == ".json":
            return load_json(in_path), in_path
        if ext(in_path) == ".csv":
            return load_csv_as_records(in_path), in_path
        raise ValueError(f"Extensión no soportada: {in_path}")
    if is_dir(in_path):
        cand = pick_input_from_dir(in_path)
        if not cand:
            raise FileNotFoundError(
                f"No se encontró entrada válida en {in_path}. "
                "Se buscan: participantes(.json/_*.json), 03participantes_*.csv, participantes_procesado_*.csv, etc."
            )
        if ext(cand) == ".json":
            return load_json(cand), cand
        if ext(cand) == ".csv":
            return load_csv_as_records(cand), cand
        raise ValueError(f"Extensión no soportada: {cand}")
    raise FileNotFoundError(f"No existe: {in_path}")

def main():
    os.makedirs(os.path.dirname(FINAL_OUT) or ".", exist_ok=True)
    records, src = load_records(IN_PATH)
    if not isinstance(records, list):
        raise ValueError("La entrada no es una lista de registros.")

    meta = {
        "_generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source": os.path.relpath(src, start="."),
        "_count": len(records),
    }
    with open(FINAL_OUT, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"✅ Escrito {FINAL_OUT} con {len(records)} registros")
    print(f"Info: {json.dumps(meta, ensure_ascii=False)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)
