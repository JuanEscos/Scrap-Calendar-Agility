#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, csv, re
from glob import glob
from datetime import datetime
from pathlib import Path

# Entradas/salidas (puedes sobreescribir por args)
OUT_DIR = os.getenv("OUT_DIR", "./output")
FINAL_OUT = "./output/participantes_completos_final.json"

def arg_or_default(i, default):
    return sys.argv[i] if len(sys.argv) > i and sys.argv[i].strip() else default

OUT_DIR   = arg_or_default(1, OUT_DIR)
FINAL_OUT = arg_or_default(2, FINAL_OUT)

def pick_first(*paths):
    for p in paths:
        if p and os.path.isfile(p):
            return p
    return None

def find_candidates(out_dir):
    # 1) Preferido: JSON “último snapshot”
    p1 = os.path.join(out_dir, "participantes.json")
    # 2) Alternativa: JSON versionado por fecha (coge el más reciente)
    dated = sorted(glob(os.path.join(out_dir, "participantes_*.json")))
    p2 = dated[-1] if dated else None
    # 3) Fallback: CSV procesado (coge el más reciente)
    csvs = sorted(glob(os.path.join(out_dir, "participantes_procesado_*.csv")))
    p3 = csvs[-1] if csvs else None
    return p1, p2, p3

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_csv_as_records(path):
    rows=[]
    with open(path, newline="", encoding="utf-8-sig") as f:
        r=csv.DictReader(f)
        for row in r:
            # normaliza espacios
            rows.append({k:(v.strip() if isinstance(v,str) else v) for k,v in row.items()})
    return rows

def main():
    os.makedirs(os.path.dirname(FINAL_OUT) or ".", exist_ok=True)
    out_dir = OUT_DIR
    print(f"=== UNION (estandarización) ===\nOUT_DIR: {out_dir}")

    p1, p2, p3 = find_candidates(out_dir)
    inp = pick_first(p1, p2)
    records = None

    if inp:
        print(f"Usando JSON: {inp}")
        records = load_json(inp)
        if not isinstance(records, list):
            print("El JSON no es una lista; intento leer 'records' si existe…")
            if isinstance(records, dict) and isinstance(records.get("records"), list):
                records = records["records"]
    else:
        if p3:
            print(f"No se encontró JSON. Convirtiendo CSV: {p3}")
            records = load_csv_as_records(p3)
        else:
            print("❌ No se encontró ni participantes.json, ni participantes_*.json, ni participantes_procesado_*.csv")
            sys.exit(1)

    # Sello y guardado
    meta = {
        "_generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source": os.path.relpath(inp or p3, start="."),
        "_count": len(records)
    }
    # Escribimos directamente el array (como antes), y añadimos metadata en comentario JSON si quieres
    with open(FINAL_OUT, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"✅ Escrito {FINAL_OUT} con {len(records)} registros")
    print(f"Info: {json.dumps(meta, ensure_ascii=False)}")

if __name__ == "__main__":
    main()
