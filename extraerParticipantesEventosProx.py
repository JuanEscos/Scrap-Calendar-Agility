#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_eventosproxParticipantes.py (integrado)
==========================================

Propósito
---------
Unificar la salida del paso 03 para que el pipeline SIEMPRE encuentre
'./output/participantes_procesado_YYYY-MM-DD.csv', independientemente
de cómo lo nombre o dónde lo guarde el extractor interno (may/min de carpeta,
nombres distintos, etc.).

Qué hace
--------
1) Normaliza el directorio de salida a ./output (minúsculas en Linux).
2) (Opcional) Ejecuta tu extractor de participantes si defines la variable
   de entorno PARTICIPANTS_CMD (por ejemplo: "python mi_extractor.py all").
3) Busca CSVs candidatos en ./output y ./Output con patrones comunes
   (participants, participantes, procesado, results, etc.).
4) Si encuentra alguno, lo copia/renombra al nombre estándar:
   ./output/participantes_procesado_YYYY-MM-DD.csv
5) Si no encuentra nada, crea un CSV vacío con cabeceras básicas para no
   romper el pipeline (puedes ampliar el schema).

Uso
---
- Sin argumentos: ejecuta el flujo completo descrito arriba.
- Con argumentos, se ignoran (compatibilidad con "all").

Variables de entorno útiles
---------------------------
- OUT_DIR / OUTPUT_DIR : carpeta de salida (se normaliza a ./output)
- PARTICIPANTS_CMD     : comando para lanzar tu extractor real
                         (ej.: "python extractor_real.py all")
- DATE_OVERRIDE        : fija la fecha (YYYY-MM-DD) del nombre destino
                         (por defecto el día actual)
"""

import os
import sys
import glob
import shlex
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
import pandas as pd


def log(msg: str):
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


def normalize_out_dir() -> Path:
    """Resuelve OUT_DIR/OUTPUT_DIR y normaliza a './output'."""
    env_dir = os.getenv("OUT_DIR") or os.getenv("OUTPUT_DIR") or "./output"
    # Si el usuario pasó "Output", lo convertimos a "./output"
    out_dir = Path("./output") if env_dir.lower().strip().replace("\\", "/") in {"output", "./output"} else Path(env_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def run_participants_cmd_if_any() -> int:
    """
    Si existe PARTICIPANTS_CMD en el entorno, lo ejecuta y devuelve su exit code.
    Si no existe, devuelve 0 (no es error).
    """
    cmd = os.getenv("PARTICIPANTS_CMD", "").strip()
    if not cmd:
        log("No hay PARTICIPANTS_CMD definido; salto la ejecución del extractor (solo normalizaré/crear archivo).")
        return 0
    log(f"Ejecutando extractor: {cmd}")
    try:
        # shell=False por seguridad; usar shlex.split
        proc = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
        if proc.stdout:
            print(proc.stdout, end="")
        if proc.stderr:
            print(proc.stderr, end="", file=sys.stderr)
        return proc.returncode
    except Exception as e:
        log(f"ERROR ejecutando PARTICIPANTS_CMD: {e}")
        return 1


def find_candidate_csvs() -> list[Path]:
    """
    Busca CSVs que podrían ser la salida del extractor en ./output y ./Output.
    Retorna una lista ordenada (más reciente primero).
    """
    candidates = []
    # Patrones comunes que hemos visto
    patterns = [
        "./output/*particip*/*.csv",   # por si hay subcarpetas
        "./Output/*particip*/*.csv",
        "./output/*particip*.csv",
        "./Output/*particip*.csv",
        "./output/*procesad*.csv",
        "./Output/*procesad*.csv",
        "./output/*result*.csv",
        "./Output/*result*.csv",
    ]
    for pat in patterns:
        for f in glob.glob(pat):
            try:
                p = Path(f)
                if p.is_file() and p.suffix.lower() == ".csv":
                    candidates.append(p)
            except Exception:
                pass
    # Ordena por fecha de modificación (desc)
    candidates = sorted(set(candidates), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates


def write_empty_csv(dst: Path):
    """
    Crea un CSV vacío con columnas básicas para que el pipeline no falle.
    Ajusta el esquema aquí si quieres más columnas.
    """
    cols = [
        "source_hub_url", "Categoria", "Posicion", "Guia", "Perro",
        "Dorsal_Club", "evento_id", "nombre", "fechas", "organizacion",
        "club", "lugar"
    ]
    df = pd.DataFrame(columns=cols)
    tmp = dst.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(dst)


def main():
    # Argumentos ignorados (compatibilidad con "all")
    _ = sys.argv[1:]  # no se usan

    out_dir = normalize_out_dir()
    date_str = os.getenv("DATE_OVERRIDE") or datetime.now().strftime("%Y-%m-%d")
    final_csv = out_dir / f"participantes_procesado_{date_str}.csv"

    log("=== Normalización de salida (03) ===")
    log(f"Carpeta salida: {out_dir}")
    log(f"Archivo esperado: {final_csv}")

    # 1) Ejecutar comando del extractor si está definido
    rc = run_participants_cmd_if_any()
    if rc != 0:
        log(f"Extractor devolvió código {rc}. Intentaré igualmente localizar/copiar CSV…")

    # 2) Buscar candidatos en ./output y ./Output
    cands = find_candidate_csvs()
    if cands:
        log("Candidatos encontrados (ordenados por más reciente):")
        for i, p in enumerate(cands[:6], 1):  # muestra hasta 6
            log(f"  [{i}] {p}")
        # Copia/renombra el primero
        src = cands[0]
        try:
            tmp = final_csv.with_suffix(".csv.tmp")
            shutil.copy2(src, tmp)
            tmp.replace(final_csv)
            log(f"✅ Normalizado: {src} → {final_csv}")
            return 0
        except Exception as e:
            log(f"ERROR copiando {src} → {final_csv}: {e}")

    # 3) Si no hay candidatos, crea un CSV vacío para no romper el pipeline
    log("No se hallaron CSVs candidatos. Creando archivo vacío para no romper el pipeline…")
    try:
        write_empty_csv(final_csv)
        log(f"✅ Creado vacío: {final_csv}")
        return 0
    except Exception as e:
        log(f"ERROR creando CSV vacío: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
