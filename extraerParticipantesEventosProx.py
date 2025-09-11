#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extraerParticipantesEventosProx.py
Orquesta 01 -> 02 -> 03 -> 04 usando tus scripts y normaliza salidas.

Salidas esperadas en ./output:
- 01events_YYYY-MM-DD.json, 01events_last.json
- 02competiciones_detalladas_YYYY-MM-DD.json, 02info_last.json
- participantes_procesado_YYYY-MM-DD.csv (y alias participantes_procesado_latest.csv)
- participants_completos_final.json

Env obligatorias: FLOW_EMAIL, FLOW_PASS
Env opcionales: HEADLESS, INCOGNITO, LIMIT_INFO, LIMIT_EVENTS, LIMIT_PARTICIPANTS
"""

import os
import sys
import json
import shutil
import glob
import time
import subprocess
from datetime import datetime
from pathlib import Path

ROOT = Path(".").resolve()
OUT  = ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)

HEADLESS = (os.getenv("HEADLESS", "true").strip().lower() in ("1","true","yes","on"))
INCOGNITO = (os.getenv("INCOGNITO", "true").strip().lower() in ("1","true","yes","on"))
DATE_STR = datetime.now().strftime("%Y-%m-%d")

for k in ("FLOW_EMAIL", "FLOW_PASS"):
    if not os.getenv(k):
        print(f"[ERROR] Falta variable de entorno: {k}")
        sys.exit(2)

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def run_py(script: str, args=None, timeout_sec=1800, env=None):
    if args is None:
        args = []
    # localizar el script
    script_path = None
    for p in ROOT.rglob(script):
        if p.is_file() and p.name == script:
            script_path = p
            break
    if not script_path and (ROOT / script).exists():
        script_path = ROOT / script
    if not script_path:
        raise FileNotFoundError(f"No se encontró {script} en el repo")

    cmd = [sys.executable, str(script_path)] + list(args)
    log(f"Ejecutando: {' '.join(cmd)}")
    subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env or os.environ.copy(),
        timeout=timeout_sec,
        check=True,
        text=True,
        capture_output=False
    )

def first_existing(*paths: Path) -> Path | None:
    for p in paths:
        if p and p.exists():
            return p
    return None

def latest_of(pattern: str, base: Path) -> Path | None:
    files = sorted(base.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None

def latest_of_many(patterns, base: Path) -> Path | None:
    files = []
    for pat in patterns:
        files.extend(list(base.glob(pat)))
    if not files:
        return None
    files = sorted(set(files), key=lambda p: p.stat().st_mtime)
    return files[-1]

def list_output_dir():
    print("── Contenido de ./output ──")
    for p in sorted(OUT.glob("*")):
        try:
            print(f" - {p.name:40s}  {p.stat().st_size} bytes")
        except Exception:
            print(f" - {p.name}")
    print("───────────────────────────")

def find_anywhere(patterns):
    """Busca recursivamente en todo el repo."""
    found = []
    for pat in patterns:
        found.extend(ROOT.rglob(pat))
    # ordena por mtime
    found = sorted(set(found), key=lambda p: p.stat().st_mtime)
    return found[-1] if found else None

# -------------------------
# Paso 01
# -------------------------
def step_01(timeout_sec=12*60):
    run_py("01_eventosprox.py", timeout_sec=timeout_sec)

    src = first_existing(
        OUT / "01events.json",
        OUT / "events.json",
        OUT / "01events_last.json",
        ROOT / "01events.json",
        ROOT / "events.json",
    )
    if not src:
        raise FileNotFoundError("01 no generó 01events.json / events.json")

    dated = OUT / f"01events_{DATE_STR}.json"
    last  = OUT / "01events_last.json"
    shutil.copy2(src, dated)
    shutil.copy2(src, last)
    log(f"OK 01 -> {dated.name}, {last.name}")

# -------------------------
# Paso 02
# -------------------------
def step_02(timeout_sec=25*60):
    in_events = first_existing(OUT / "01events_last.json", OUT / "01events.json")
    if not in_events:
        raise FileNotFoundError("Falta 01events_last.json para 02")

    out_info = OUT / "02competiciones_detalladas.json"
    run_py("02_eventosproxINFO.py", args=[str(in_events), str(out_info)], timeout_sec=timeout_sec)

    if not out_info.exists():
        raise FileNotFoundError("02 no generó 02competiciones_detalladas.json")

    dated = OUT / f"02competiciones_detalladas_{DATE_STR}.json"
    last  = OUT / "02info_last.json"
    shutil.copy2(out_info, dated)
    shutil.copy2(out_info, last)
    log(f"OK 02 -> {dated.name}, {last.name}")

# -------------------------
# Paso 03
# -------------------------
def step_03(timeout_sec=60*60):
    """
    Fuerza OUT_DIR=./output al script 03 y busca el CSV resultante
    tanto en ./output como en todo el repo si hiciera falta.
    """
    env3 = os.environ.copy()
    env3.update({
        "OUT_DIR": str(OUT),
        "HEADLESS": "true" if HEADLESS else "false",
        "INCOGNITO": "true" if INCOGNITO else "false",
        # respeta límites si están en el entorno actual
        "LIMIT_EVENTS": os.getenv("LIMIT_EVENTS", "0"),
        "LIMIT_PARTICIPANTS": os.getenv("LIMIT_PARTICIPANTS", "0"),
        "FILE_PREFIX": os.getenv("FILE_PREFIX", "03"),
    })
    run_py("03_eventosproxParticipantes.py", args=["all"], timeout_sec=timeout_sec, env=env3)

    # listar lo que quedó en ./output
    list_output_dir()

    # buscar primero en ./output (preferencia: del día)
    found = latest_of_many([f"participantes_procesado_{DATE_STR}.csv"], OUT)
    if not found:
        # cualquier fecha/prefijo en ./output
        found = latest_of_many(["participantes_procesado_*.csv", "*participantes_procesado*.csv"], OUT)

    if not found:
        # rebuscar por TODO el repo
        found = find_anywhere([f"participantes_procesado_{DATE_STR}.csv",
                               "participantes_procesado_*.csv",
                               "*participantes_procesado*.csv"])

    if not found:
        raise FileNotFoundError(
            "03 no generó participantes_procesado_*.csv. "
            "Se listó arriba el contenido de ./output. Si 03 escribe en otro directorio, ajusta OUT_DIR o mueve el archivo."
        )

    latest_alias = OUT / "participantes_procesado_latest.csv"
    shutil.copy2(found, latest_alias)
    log(f"OK 03 -> {found.name} (alias: {latest_alias.name})")

# -------------------------
# Paso 04
# -------------------------
def step_04(timeout_sec=5*60):
    final_out = OUT / "participants_completos_final.json"
    # Acepta hasta 3 argumentos:  path participantes + (opcional) path eventos
    # Tu 04_beta acepta: IN_PATH OUT_PATH  (y también puede manejar dir)
    run_py("04_eventosproxUnionBeta.py", args=[str(OUT), str(final_out)], timeout_sec=timeout_sec)

    if not final_out.exists():
        raise FileNotFoundError("04 no generó participants_completos_final.json")

    log(f"OK 04 -> {final_out.name}")

def print_summary():
    log("=== RESUMEN DE SALIDAS EN ./output ===")
    wanted = [
        f"01events_{DATE_STR}.json",
        "01events_last.json",
        f"02competiciones_detalladas_{DATE_STR}.json",
        "02info_last.json",
        f"participantes_procesado_{DATE_STR}.csv",
        "participantes_procesado_latest.csv",
        "participants_completos_final.json",
        f"03events_{DATE_STR}.csv",
        f"03participantes_{DATE_STR}.csv",
        "participantes.json",
        f"03participantes_{DATE_STR}.json",
    ]
    for w in wanted:
        for p in OUT.glob(w):
            print(f" - {p.name}  ({p.stat().st_size} bytes)")

def main():
    which = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "all")

    log("=== ENTORNO ===")
    print(f"HEADLESS={HEADLESS}")
    print(f"LIMIT_EVENTS={os.getenv('LIMIT_EVENTS','0')}")
    print(f"LIMIT_PARTICIPANTS={os.getenv('LIMIT_PARTICIPANTS','0')}")
    print("")

    try:
        if which in ("all", "01"): step_01()
        if which in ("all", "02"): step_02()
        if which in ("all", "03"): step_03()
        if which in ("all", "04"): step_04()
        if which == "all": print_summary()
        log("✅ Flujo terminado correctamente.")
    except subprocess.TimeoutExpired as e:
        print(f"[ERROR] Timeout ejecutando: {e.cmd}")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Subproceso falló (exit {e.returncode}): {e.cmd}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
