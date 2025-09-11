#!/usr/bin/env bash
# upload_workflow_outputs.sh
# -----------------------------------------------------------------------------
# Sube al FTP los artefactos generados en ./output:
#   OBLIGATORIOS:
#     - 01events_*.json                   -> 01events_last.json + 01events_${TS}.json
#     - 02competiciones_detalladas_*.json -> 02info_last.json  + 02info_${TS}.json
#   OPCIONALES (no rompen el job si no existen):
#     - participantes_procesado_*.csv     -> participantes_procesado_${TS}.csv
#     - participants_completos_final*.json-> *_last.json y *_${TS}.json
#
# Requisitos: lftp instalado.
#
# Variables de entorno esperadas:
#   FTP_SERVER       (p.ej. ftp.ejemplo.com)
#   FTP_USERNAME
#   FTP_PASSWORD
#   FTP_REMOTE_DIR   (carpeta base remota; se crearán subcarpetas necesarias)
#   OUT_DIR          (opcional, por defecto ./output)
#   DATE_OVERRIDE    (opcional, YYYYMMDDTHHMMSSZ para forzar timestamp)
# -----------------------------------------------------------------------------

set -euo pipefail

OUT_DIR="${OUT_DIR:-./output}"
TS="${DATE_OVERRIDE:-$(date -u +'%Y%m%dT%H%M%SZ')}"
DEST_BASE="$(printf "%s/Competiciones/ListadoEventos/Workflows" "${FTP_REMOTE_DIR%/}")"

echo "[INFO] OUT_DIR=${OUT_DIR}"
echo "[INFO] TS=${TS}"
echo "[INFO] DEST_BASE=${DEST_BASE}"

one_or_fail() {
  local pat="$1"
  local f
  f=$(ls -1t "${OUT_DIR%/}/${pat}" 2>/dev/null | head -n1 || true)
  if [[ -z "${f}" ]]; then
    echo "::error::No encontrado ${OUT_DIR%/}/${pat}"
    exit 1
  fi
  printf "%s" "${f}"
}

one_or_none() {
  local pat="$1"
  ls -1t "${OUT_DIR%/}/${pat}" 2>/dev/null | head -n1 || true
}

# --- Buscar artefactos ---
F01=$(one_or_fail "01events_*.json")
F02=$(one_or_fail "02competiciones_detalladas_*.json")
F03=$(one_or_none "participantes_procesado_*.csv")
F04=$(one_or_none "participants_completos_final*.json")

echo "[INFO] F01=${F01}"
echo "[INFO] F02=${F02}"
[[ -n "${F03:-}" ]] && echo "[INFO] F03=${F03}" || echo "[WARN] F03 no encontrado (opcional)"
[[ -n "${F04:-}" ]] && echo "[INFO] F04=${F04}" || echo "[WARN] F04 no encontrado (opcional)"

# --- Construir comandos lftp (cuidando comillas) ---
LFTP_CMDS="
set cmd:fail-exit true;
set net:timeout 25;
set net:max-retries 1;
set net:persist-retries 0;
set ftp:ssl-force true;
set ftp:ssl-protect-data true;
set ftp:passive-mode true;
set ftp:prefer-epsv false;
set ssl:verify-certificate no;

cd '${FTP_REMOTE_DIR}' || cd '/${FTP_REMOTE_DIR}';
mkdir -f Competiciones; cd Competiciones;
mkdir -f ListadoEventos; cd ListadoEventos;
mkdir -f Workflows; cd Workflows;

put -O . '${F01}' -o '01events_last.json';
put -O . '${F01}' -o '01events_${TS}.json';
put -O . '${F02}' -o '02info_last.json';
put -O . '${F02}' -o '02info_${TS}.json';
"

if [[ -n "${F03:-}" ]]; then
  LFTP_CMDS="${LFTP_CMDS} put -O . '${F03}' -o 'participantes_procesado_${TS}.csv';"
else
  echo "::warning::No se sube participantes_procesado (no existe)."
fi

if [[ -n "${F04:-}" ]]; then
  LFTP_CMDS="${LFTP_CMDS} put -O . '${F04}' -o 'participants_completos_final_last.json';
put -O . '${F04}' -o 'participants_completos_final_${TS}.json';"
else
  echo "::warning::No se sube participants_completos_final (no existe)."
fi

LFTP_CMDS="${LFTP_CMDS}
echo 'Contenido destino:'; pwd; cls -l;
bye"

echo "[INFO] Conectando y subiendo a ${FTP_SERVER} -> ${DEST_BASE}"
lftp -u "${FTP_USERNAME},${FTP_PASSWORD}" "${FTP_SERVER}" -e "${LFTP_CMDS}"
echo "[INFO] Subida completada."
