#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extraerParticipantesEventosProx.py
Orquesta 01 -> 02 -> 03 -> 04 usando tus scripts originales y normaliza salidas.
- Env:
  FLOW_EMAIL / FLOW_PASS   (obligatorias para login)
  HEADLESS=true|false      (por defecto true)
  LIMIT_INFO, LIMIT_EVENTS, LIMIT_PARTICIPANTS (opcional)
- Uso:
  python extraerParticipantesEventosProx.py            # ejecuta all
  python extraerParticipantesEventosProx.py all
  python extraerParticipantesEventosProx.py 01|02|03|04
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

# --- Config base ---
ROOT = Path(".").resolve()
OUT  = ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)

HEADLESS = (os.getenv("HEADLESS", "true").strip().lower() in ("1","true","yes","on"))
DATE_STR = datetime.now().strftime("%Y-%m-%d")

REQ_ENVS = ["FLOW_EMAIL", "FLOW_PASS"]
for k in REQ_ENVS:
    if not os.getenv(k):
        print(f"[ERROR] Falta variable de entorno: {k}")
        sys.exit(2)

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def run_py(script: str, args=None, timeout_sec=1800, env=None):
    """
    Ejecuta un script Python del repo con argumentos y timeout.
    - script: nombre del archivo (ej: '01_eventosprox.py')
    """
    if args is None:
        args = []
    script_path = None
    # Busca el script en el repo
    candidates = [
        p for p in (ROOT.rglob(script))
        if p.is_file() and p.name == script
    ]
    if candidates:
        script_path = candidates[0]
    else:
        # por si está en raíz
        if (ROOT / script).exists():
            script_path = ROOT / script

    if not script_path:
        raise FileNotFoundError(f"No se encontró {script} en el repo")

    cmd = [sys.executable, str(script_path)] + list(args)
    log(f"Ejecutando: {' '.join(cmd)}")
    res = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env or os.environ.copy(),
        timeout=timeout_sec,
        check=True,
        text=True,
        capture_output=False
    )
    return res

def first_existing(*paths: Path) -> Path | None:
    for p in paths:
        if p and p.exists():
            return p
    return None

def copy_if_exists(src: Path, dst: Path):
    if src and src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return True
    return False

def latest_of(pattern: str) -> Path | None:
    files = sorted(OUT.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None

# -------------------------
# Paso 01: eventos base
# -------------------------
def step_01(timeout_sec=12*60):
    """
    Lanza 01_eventosprox.py y normaliza a:
      - output/01events_YYYY-MM-DD.json
      - output/01events_last.json
    Acepta también el caso en el que 01 saque ./output/events.json
    """
    run_py("01_eventosprox.py", timeout_sec=timeout_sec)

    # Detectar salida del 01
    candidates = [
        OUT / "01events.json",
        OUT / "events.json",
        OUT / "01events_last.json",
    ]
    src = first_existing(*candidates)
    if not src:
        # A veces 01 crea en raíz
        src = first_existing(ROOT / "01events.json", ROOT / "events.json")
    if not src or not src.exists():
        raise FileNotFoundError("01 no generó ni 01events.json ni events.json")

    # Normalizar nombres
    dated = OUT / f"01events_{DATE_STR}.json"
    last  = OUT / "01events_last.json"
    shutil.copy2(src, dated)
    shutil.copy2(src, last)
    log(f"OK 01 -> {dated.name}, {last.name}")

# -------------------------
# Paso 02: info detallada
# -------------------------
def step_02(timeout_sec=25*60):
    """
    Lanza 02_eventosproxINFO.py con IN=01events_last.json y OUT=02competiciones_detalladas.json
    y normaliza a:
      - 02competiciones_detalladas_YYYY-MM-DD.json
      - 02info_last.json (copia canónica)
    """
    in_events = first_existing(OUT / "01events_last.json", OUT / "01events.json")
    if not in_events:
        raise FileNotFoundError("Falta 01events_last.json para 02")

    out_info = OUT / "02competiciones_detalladas.json"

    run_py(
        "02_eventosproxINFO.py",
        args=[str(in_events), str(out_info)],
        timeout_sec=timeout_sec
    )

    if not out_info.exists():
        raise FileNotFoundError("02 no generó 02competiciones_detalladas.json")

    dated = OUT / f"02competiciones_detalladas_{DATE_STR}.json"
    last  = OUT / "02info_last.json"
    shutil.copy2(out_info, dated)
    shutil.copy2(out_info, last)
    log(f"OK 02 -> {dated.name}, {last.name}")

# -------------------------
# Paso 03: participantes (scrape + process)
# -------------------------
def step_03(timeout_sec=60*60):
    """
    Lanza 03_eventosproxParticipantes.py modo 'all' (scrape + process).
    Verifica y deja:
      - output/03events_YYYY-MM-DD.csv (si tu 03 lo genera)
      - output/03participantes_YYYY-MM-DD.csv (si tu 03 lo genera)
      - output/participantes_procesado_YYYY-MM-DD.csv (OBLIGATORIO)
      - y snapshots JSON si los saca (03participantes_YYYY-MM-DD.json / 03participantes.json)
    """
    # Pasamos HEADLESS/LIMIT_* al subproceso via env heredado
    run_py("03_eventosproxParticipantes.py", args=["all"], timeout_sec=timeout_sec)

    # Buscar el CSV procesado del día, si no, el último disponible
    proc_today = latest_of(f"participantes_procesado_{DATE_STR}.csv")
    if not proc_today:
        # último cualquiera
        proc_today = latest_of("participantes_procesado_*.csv")
    if not proc_today:
        raise FileNotFoundError("03 no generó participantes_procesado_*.csv")

    log(f"OK 03 -> {proc_today.name}")

# -------------------------
# Paso 04: unión final
# -------------------------
def step_04(timeout_sec=5*60):
    """
    Lanza 04_eventosproxUnionBeta.py para crear:
      - output/participants_completos_final.json
    Toma como IN la propia carpeta ./output (el script 04 detecta el mejor input).
    """
    final_out = OUT / "participants_completos_final.json"
    # Usa la versión Beta que compartiste
    # Arg1: entrada (fichero o carpeta)  Arg2: salida final JSON
    run_py("04_eventosproxUnionBeta.py", args=[str(OUT), str(final_out)], timeout_sec=timeout_sec)

    if not final_out.exists():
        raise FileNotFoundError("04 no generó participants_completos_final.json")

    log(f"OK 04 -> {final_out.name}")

# -------------------------
# Utilidad: resumen al final
# -------------------------
def print_summary():
    log("=== RESUMEN DE SALIDAS EN ./output ===")
    wanted = [
        f"01events_{DATE_STR}.json",
        "01events_last.json",
        f"02competiciones_detalladas_{DATE_STR}.json",
        "02info_last.json",
        f"participantes_procesado_{DATE_STR}.csv",
        "participants_completos_final.json",
        f"03events_{DATE_STR}.csv",
        f"03participantes_{DATE_STR}.csv",
        f"03participantes_{DATE_STR}.json",
        "03participantes.json",
    ]
    for w in wanted:
        for p in OUT.glob(w):
            print(f" - {p.name}  ({p.stat().st_size} bytes)")

# -------------------------
# Main
# -------------------------
def main():
    which = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "all")

    # Cabecera de entorno efectivo
    log("=== ENTORNO ===")
    print(f"HEADLESS={HEADLESS}")
    for k in ("LIMIT_INFO","LIMIT_EVENTS","LIMIT_PARTICIPANTS"):
        if os.getenv(k): print(f"{k}={os.getenv(k)}")
    print("")

    try:
        if which in ("all", "01"):
            step_01()
        if which in ("all", "02"):
            # Si sólo se lanza 02, asegúrate de que 01events exista (por diseño)
            if which == "02" and not (OUT / "01events_last.json").exists():
                raise SystemExit("Para 02 necesitas 01events_last.json en ./output")
            step_02()
        if which in ("all", "03"):
            step_03()
        if which in ("all", "04"):
            step_04()

        if which == "all":
            print_summary()

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
