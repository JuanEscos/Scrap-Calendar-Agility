#Clear All Variables in IPython - No User Confirmation
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Created on agosto 16 2025
    Revisado :20/08/2025
    
    @author: Juan Escos
    
Archivo:
04ResultadosURLsMangas.py 
FUNCIONA: Crea Results/ResultadosPast_{today_str}.csv
    
"""

# **************************************     Descripción    *****************************************
"""

¡Perfecto! Para encadenar 4 workflows separados y pasar los resultados entre ellos, usa artefactos y el trigger workflow_run.
Abajo tienes los 4 .yml completos, listos para pegar. Cada uno:
Descarga código → instala deps → ejecuta su *.py
Sube su salida como artefacto
El siguiente workflow se lanza con workflow_run y descarga ese artefacto usando el run-id del job que lo disparó.
Ajusta si cambias rutas/nombres. He supuesto salidas:

01_eventsProx.py → ./output/01events.json
02_eventosproxINFO.py → ./output/02competiciones_detalladas.json
03_eventosproxParticipantes.py → muchos JSON en ./output/participants/*.json
04_eventosproxUnion.py → ./output/participants_completos_final.json

1) 01_eventos.yml — genera 01events.json
name: 01 - Eventos Prox (JSON base)

on:
  schedule:
    - cron: '1 4 * * *'   # 04:01 UTC
  workflow_dispatch: {}

jobs:
  produce_01events:
    runs-on: ubuntu-latest
    env:
      TZ: Europe/Madrid
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip
          cache-dependency-path: requirements.txt

      - name: Instalar dependencias
        run: |
          python -m pip install --upgrade pip
          if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
          mkdir -p output

      - name: Ejecutar 01_eventsProx.py
        run: |
          python "./01_eventosprox.py"  # genera ./output/01events.json

      - name: Verificar salida
        run: |
          test -f ./output/01events.json
          ls -lh ./output

      - name: Subir artefacto 01events
        uses: actions/upload-artifact@v4
        with:
          name: a01-01events-json
          path: ./output/01events.json
          if-no-files-found: error
          retention-days: 7

2) 02_info.yml — usa 01events.json y genera 02competiciones_detalladas.json
name: 02 - INFO desde 01events

on:
  workflow_run:
    workflows: ["01 - Eventos Prox (JSON base)"]
    types: [completed]

jobs:
  produce_02info:
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
    runs-on: ubuntu-latest
    env:
      TZ: Europe/Madrid
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip
          cache-dependency-path: requirements.txt

      - name: Descargar artefacto 01events
        uses: actions/download-artifact@v4
        with:
          name: a01-01events-json
          path: ./artifacts
          run-id: ${{ github.event.workflow_run.id }}
          github-token: ${{ secrets.GITHUB_TOKEN }}

      - name: Ejecutar 02_eventosproxINFO.py
        run: |
          mkdir -p output
          python "./02_eventosproxINFO.py" "./artifacts/01events.json" "./output/02competiciones_detalladas.json"

      - name: Subir artefacto 02info
        uses: actions/upload-artifact@v4
        with:
          name: a02-02info-json
          path: ./output/02competiciones_detalladas.json
          if-no-files-found: error
          retention-days: 7

3) 03_participantes.yml — usa 02competiciones_detalladas.json y genera N JSON (uno por evento)
name: 3 - Participantes por evento

on:
  workflow_run:
    workflows: ["02 - INFO desde 01events"]
    types: [completed]

jobs:
  produce_participants:
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
    runs-on: ubuntu-latest
    env:
      TZ: Europe/Madrid
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip
          cache-dependency-path: requirements.txt

      - name: Descargar artefacto 02info
        uses: actions/download-artifact@v4
        with:
          name: a02-02info-json
          path: ./artifacts
          run-id: ${{ github.event.workflow_run.id }}
          github-token: ${{ secrets.GITHUB_TOKEN }}

      - name: Ejecutar 03_eventosproxParticipantes.py
        run: |
          mkdir -p output/participants
          python "./03_eventosproxParticipantes.py" "./artifacts/02competiciones_detalladas.json" "./output/participants"

      - name: Comprobar ficheros generados
        run: |
          shopt -s nullglob
          count=(./output/participants/*.json)
          echo "Total participantes JSON: ${#count[@]}"
          test ${#count[@]} -gt 0

      - name: Subir artefacto participantes (carpeta completa)
        uses: actions/upload-artifact@v4
        with:
          name: a03-participants-jsons
          path: |
            ./output/participants/*.json
          if-no-files-found: error
          retention-days: 7

4) 04_union.yml — descarga todos los JSON de participantes y crea participants_completos_final.json
name: 4 - Union participantes

on:
  workflow_run:
    workflows: ["3 - Participantes por evento"]
    types: [completed]

jobs:
  union_participants:
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
    runs-on: ubuntu-latest
    env:
      TZ: Europe/Madrid
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip
          cache-dependency-path: requirements.txt

      - name: Descargar artefacto con todos los participantes
        uses: actions/download-artifact@v4
        with:
          name: a03-participants-jsons
          path: ./participants_jsons
          run-id: ${{ github.event.workflow_run.id }}
          github-token: ${{ secrets.GITHUB_TOKEN }}

      - name: Ejecutar 04_eventosproxUnion.py
        run: |
          mkdir -p output
          python "./04_eventosproxUnion.py" "./participants_jsons" "./output/participants_completos_final.json"

      - name: Verificar resultado final
        run: |
          ls -lh ./output
          test -f ./output/participants_completos_final.json

      - name: Subir artefacto final
        uses: actions/upload-artifact@v4
        with:
          name: a04-participantes-final
          path: ./output/participants_completos_final.json
          if-no-files-found: error
          retention-days: 14

Notas importantes

Nombres coherentes: Los workflows: del trigger workflow_run deben coincidir exactamente con el name: del workflow anterior.

Descarga por run-id: garantiza que el workflow B/C/D usa el artefacto del run específico que lo disparó.

Artefacto múltiple (paso 3): usar un glob ./output/participants/*.json sube todos; al descargar, se restauran con los mismos nombres.

Si prefieres un solo workflow, haz 4 jobs con needs: y usa actions/download-artifact/upload-artifact entre jobs (o comparte workspace). Pero con 4 .yml separados tienes desacoplamiento y logs más limpios.

Si además quieres publicar (FTPS, S3, etc.), añade el paso de subida al final del #4 usando tus secrets.

¿Quieres que adapte los comandos python a la interfaz exacta de tus scripts (argumentos/flags) o que añada la subida FTPS del JSON final en el workflow 4?

Tú dijiste:
Si añade la subida FTPS del JSON final en el workflow 4
ChatGPT Plus
Pensó durante 47s

¡Hecho! Aquí tienes el workflow 4 actualizado con la subida por FTPS del JSON final (incluye listing, subida con nombre fijo y copia con sello temporal, y verificación por hash):

name: 4 - Union participantes

on:
  workflow_run:
    workflows: ["3 - Participantes por evento"]
    types: [completed]

jobs:
  union_participants:
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
    runs-on: ubuntu-latest
    env:
      TZ: Europe/Madrid
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip
          cache-dependency-path: requirements.txt

      - name: Descargar artefacto con todos los participantes
        uses: actions/download-artifact@v4
        with:
          name: a03-participants-jsons
          path: ./participants_jsons
          run-id: ${{ github.event.workflow_run.id }}
          github-token: ${{ secrets.GITHUB_TOKEN }}

      - name: Ejecutar 04_eventosproxUnion.py
        run: |
          set -euo pipefail
          mkdir -p output
          python "./04_eventosproxUnion.py" "./participants_jsons" "./output/participants_completos_final.json"

      - name: Verificar resultado final
        run: |
          set -euo pipefail
          ls -lh ./output
          test -f ./output/participants_completos_final.json
          sha256sum ./output/participants_completos_final.json | awk '{print $1}' > local.sha256
          echo "Hash local: $(cat local.sha256)"

      - name: (FTPS) Listar carpeta remota antes de subir
        env:
          FTP_SERVER:     ${{ secrets.FTP_SERVER }}
          FTP_USERNAME:   ${{ secrets.FTP_USERNAME }}
          FTP_PASSWORD:   ${{ secrets.FTP_PASSWORD }}
          FTP_REMOTE_DIR: ${{ secrets.FTP_REMOTE_DIR }}
        run: |
          set -euo pipefail
          BASE="ftp://${FTP_SERVER}${FTP_REMOTE_DIR}/Competiciones/ListadoParticipantes"
          echo "Listando: ${BASE}/"
          curl --fail --ssl-reqd --user "${FTP_USERNAME}:${FTP_PASSWORD}" "${BASE}/" -l || true

      - name: (FTPS) Subir JSON final (fijo + copia con timestamp)
        env:
          FTP_SERVER:     ${{ secrets.FTP_SERVER }}
          FTP_USERNAME:   ${{ secrets.FTP_USERNAME }}
          FTP_PASSWORD:   ${{ secrets.FTP_PASSWORD }}
          FTP_REMOTE_DIR: ${{ secrets.FTP_REMOTE_DIR }}
        run: |
          set -euo pipefail
          FILE="./output/participants_completos_final.json"
          [ -f "$FILE" ] || { echo "::error::No existe $FILE"; exit 1; }

          BASE="ftp://${FTP_SERVER}${FTP_REMOTE_DIR}/Competiciones/ListadoParticipantes"
          TS="$(date -u +'%Y%m%dT%H%M%SZ')"

          echo "Subiendo (nombre fijo): ${BASE}/participants_completos_final.json"
          curl --fail --ssl-reqd --ftp-create-dirs --disable-epsv --ftp-skip-pasv-ip \
               --user "${FTP_USERNAME}:${FTP_PASSWORD}" \
               --upload-file "$FILE" \
               "${BASE}/participants_completos_final.json"

          echo "Subiendo (backup con timestamp): ${BASE}/participants_completos_final_${TS}.json"
          curl --fail --ssl-reqd --ftp-create-dirs --disable-epsv --ftp-skip-pasv-ip \
               --user "${FTP_USERNAME}:${FTP_PASSWORD}" \
               --upload-file "$FILE" \
               "${BASE}/participants_completos_final_${TS}.json"

      - name: (FTPS) Verificar remoto (descargar y comparar hash)
        env:
          FTP_SERVER:     ${{ secrets.FTP_SERVER }}
          FTP_USERNAME:   ${{ secrets.FTP_USERNAME }}
          FTP_PASSWORD:   ${{ secrets.FTP_PASSWORD }}
          FTP_REMOTE_DIR: ${{ secrets.FTP_REMOTE_DIR }}
        run: |
          set -euo pipefail
          BASE="ftp://${FTP_SERVER}${FTP_REMOTE_DIR}/Competiciones/ListadoParticipantes"
          echo "Descargando para verificación: ${BASE}/participants_completos_final.json"
          curl --fail --ssl-reqd --disable-epsv --ftp-skip-pasv-ip \
               --user "${FTP_USERNAME}:${FTP_PASSWORD}" \
               --output remote.json "${BASE}/participants_completos_final.json"

          sha256sum remote.json | awk '{print $1}' > remote.sha256
          echo "Hash remoto: $(cat remote.sha256)"
          diff -q local.sha256 remote.sha256 && echo "OK: hash coincide"


Notas rápidas:

Ajusta el directorio remoto Competiciones/ListadoParticipantes si usas otro.

Requiere secrets: FTP_SERVER, FTP_USERNAME, FTP_PASSWORD, FTP_REMOTE_DIR.

Sube dos copias: nombre fijo (para consumo web) y backup con timestamp.

Tú dijiste:
Tengo que usar un workflow trigger aparte?
ChatGPT Plus
Pensó durante 43s

Corto y al grano: no necesitas otro trigger extra.

Si mantienes 4 workflows separados, deja solo el primero con schedule/workflow_dispatch.
Los otros tres ya llevan on.workflow_run apuntando al anterior y se encadenan solos cuando el previo termina con éxito.

Opcional (útil para pruebas):

Puedes añadir workflow_dispatch también a los workflows 2-4 para lanzarlos manualmente. En ese caso, pon un fallback para bajar el artefacto “más reciente” cuando no haya run-id del workflow_run:

on:
  workflow_run:
    workflows: ["3 - Participantes por evento"]
    types: [completed]
  workflow_dispatch: {}   # opcional

steps:
  # Ruta normal: viene encadenado
  - name: Descargar artefacto del run que me dispara
    if: ${{ github.event_name == 'workflow_run' }}
    uses: actions/download-artifact@v4
    with:
      name: a03-participants-jsons
      path: ./participants_jsons
      run-id: ${{ github.event.workflow_run.id }}
      github-token: ${{ secrets.GITHUB_TOKEN }}

  # Fallback si lo lanzo a mano
  - name: Descargar último artefacto disponible
    if: ${{ github.event_name == 'workflow_dispatch' }}
    uses: actions/download-artifact@v4
    with:
      name: a03-participants-jsons
      path: ./participants_jsons
      workflow: "3 - Participantes por evento"
      branch: main
      github-token: ${{ secrets.GITHUB_TOKEN }}


Alternativa: si prefieres un solo .yml, crea 4 jobs con needs: y no usas workflow_run:

jobs:
  step01: { ... }
  step02:
    needs: step01
  step03:
    needs: step02
  step04:
    needs: step03


Tip extra: añade concurrency para evitar solapes si el cron se pisa:

concurrency:
  group: participantes-union
  cancel-in-progress: true


¿Te lo dejo ya integrado en tu 04_union.yml con el workflow_dispatch opcional?

Tú dijiste:
si ok
ChatGPT Plus
Pensó durante 15s

¡Listo! Aquí tienes el workflow 4 con:

workflow_run (encadenado desde el #3) y workflow_dispatch (lanzamiento manual).

concurrency para evitar solapes.

Descarga del artefacto usando run-id cuando viene encadenado y fallback al último artefacto del workflow 3 cuando es manual.

Subida FTPS (nombre fijo + copia con timestamp) y verificación por hash.

name: 4 - Union participantes

on:
  workflow_run:
    workflows: ["3 - Participantes por evento"]
    types: [completed]
  workflow_dispatch: {}

concurrency:
  group: participantes-union
  cancel-in-progress: true

jobs:
  union_participants:
    if: ${{ github.event_name == 'workflow_dispatch' || (github.event_name == 'workflow_run' && github.event.workflow_run.conclusion == 'success') }}
    runs-on: ubuntu-latest
    env:
      TZ: Europe/Madrid
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip
          cache-dependency-path: requirements.txt

      # --- Descarga artefactos ---
      - name: Descargar artefacto (modo encadenado workflow_run)
        if: ${{ github.event_name == 'workflow_run' }}
        uses: actions/download-artifact@v4
        with:
          name: a03-participants-jsons
          path: ./participants_jsons
          run-id: ${{ github.event.workflow_run.id }}
          github-token: ${{ secrets.GITHUB_TOKEN }}

      - name: Descargar artefacto más reciente (modo manual)
        if: ${{ github.event_name == 'workflow_dispatch' }}
        uses: actions/download-artifact@v4
        with:
          name: a03-participants-jsons
          path: ./participants_jsons
          workflow: "3 - Participantes por evento"
          branch: main
          github-token: ${{ secrets.GITHUB_TOKEN }}

      # --- Unión ---
      - name: Ejecutar 04_eventosproxUnion.py
        run: |
          set -euo pipefail
          mkdir -p output
          python "./04_eventosproxUnion.py" "./participants_jsons" "./output/participants_completos_final.json"

      - name: Verificar resultado final
        run: |
          set -euo pipefail
          ls -lh ./output
          test -f ./output/participants_completos_final.json
          sha256sum ./output/participants_completos_final.json | awk '{print $1}' > local.sha256
          echo "Hash local: $(cat local.sha256)"

      # --- FTPS: listar, subir (fijo + timestamp), verificar ---
      - name: (FTPS) Listar carpeta remota antes de subir
        env:
          FTP_SERVER:     ${{ secrets.FTP_SERVER }}
          FTP_USERNAME:   ${{ secrets.FTP_USERNAME }}
          FTP_PASSWORD:   ${{ secrets.FTP_PASSWORD }}
          FTP_REMOTE_DIR: ${{ secrets.FTP_REMOTE_DIR }}
        run: |
          set -euo pipefail
          BASE="ftp://${FTP_SERVER}${FTP_REMOTE_DIR}/Competiciones/ListadoParticipantes"
          echo "Listando: ${BASE}/"
          curl --fail --ssl-reqd --user "${FTP_USERNAME}:${FTP_PASSWORD}" "${BASE}/" -l || true

      - name: (FTPS) Subir JSON final (fijo + backup con timestamp)
        env:
          FTP_SERVER:     ${{ secrets.FTP_SERVER }}
          FTP_USERNAME:   ${{ secrets.FTP_USERNAME }}
          FTP_PASSWORD:   ${{ secrets.FTP_PASSWORD }}
          FTP_REMOTE_DIR: ${{ secrets.FTP_REMOTE_DIR }}
        run: |
          set -euo pipefail
          FILE="./output/participants_completos_final.json"
          [ -f "$FILE" ] || { echo "::error::No existe $FILE"; exit 1; }

          BASE="ftp://${FTP_SERVER}${FTP_REMOTE_DIR}/Competiciones/ListadoParticipantes"
          TS="$(date -u +'%Y%m%dT%H%M%SZ')"

          echo "Subiendo (nombre fijo): ${BASE}/participants_completos_final.json"
          curl --fail --ssl-reqd --ftp-create-dirs --disable-epsv --ftp-skip-pasv-ip \
               --user "${FTP_USERNAME}:${FTP_PASSWORD}" \
               --upload-file "$FILE" \
               "${BASE}/participants_completos_final.json"

          echo "Subiendo (backup con timestamp): ${BASE}/participants_completos_final_${TS}.json"
          curl --fail --ssl-reqd --ftp-create-dirs --disable-epsv --ftp-skip-pasv-ip \
               --user "${FTP_USERNAME}:${FTP_PASSWORD}" \
               --upload-file "$FILE" \
               "${BASE}/participants_completos_final_${TS}.json"

      - name: (FTPS) Verificar remoto (descargar y comparar hash)
        env:
          FTP_SERVER:     ${{ secrets.FTP_SERVER }}
          FTP_USERNAME:   ${{ secrets.FTP_USERNAME }}
          FTP_PASSWORD:   ${{ secrets.FTP_PASSWORD }}
          FTP_REMOTE_DIR: ${{ secrets.FTP_REMOTE_DIR }}
        run: |
          set -euo pipefail
          BASE="ftp://${FTP_SERVER}${FTP_REMOTE_DIR}/Competiciones/ListadoParticipantes"
          echo "Descargando para verificación: ${BASE}/participants_completos_final.json"
          curl --fail --ssl-reqd --disable-epsv --ftp-skip-pasv-ip \
               --user "${FTP_USERNAME}:${FTP_PASSWORD}" \
               --output remote.json "${BASE}/participants_completos_final.json"

          sha256sum remote.json | awk '{print $1}' > remote.sha256
          echo "Hash remoto: $(cat remote.sha256)"
          diff -q local.sha256 remote.sha256 && echo "OK: hash coincide"


Si tu rama principal no es main, cámbiala en el paso de descarga “modo manual”. ¿Quieres que te deje también los workflows 1–3 con workflow_dispatch opcional y concurrency?

"""



import os
import re
import time
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup  # Importación añadida aquí

# Configuración
BASE = "https://www.flowagility.com"
EVENTS_URL = "https://www.flowagility.com/zone/events"
FLOW_EMAIL = "pilar1959suarez@gmail.com"
FLOW_PASS = "Seattle1"
HEADLESS = True
INCOGNITO = True
MAX_SCROLLS = 10
SCROLL_WAIT_S = 1.5
OUT_DIR = "./output"
UUID_RE = re.compile(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})")

# Crear directorio de salida si no existe
os.makedirs(OUT_DIR, exist_ok=True)

def log(message):
    """Función de logging"""
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=0.5, max_s=1.2):
    """Pausa aleatoria entre min_s y max_s segundos"""
    time.sleep(max(min_s, max_s))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        JavascriptException, StaleElementReferenceException, NoSuchElementException,
        ElementClickInterceptedException, TimeoutException
    )
    return webdriver, By, Options, WebDriverWait, EC, JavascriptException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException

def _get_driver():
    webdriver, By, Options, *_ = _import_selenium()
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
    
    # Opciones adicionales para evitar problemas de versión
    opts.add_argument("--disable-gpu")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    # Usar ChromeDriverManager para manejar automáticamente la versión
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except ImportError:
        # Fallback si webdriver_manager no está instalado
        log("webdriver_manager no instalado, usando ChromeDriver del sistema")
        return webdriver.Chrome(options=opts)

def _save_screenshot(driver, name):
    try:
        path = os.path.join(OUT_DIR, name)
        driver.save_screenshot(path)
        log(f"Screenshot -> {path}")
    except Exception:
        pass

def _accept_cookies(driver, By):
    try:
        for sel in (
            '[data-testid="uc-accept-all-button"]',
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            'button[mode="primary"]',
        ):
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            if btns:
                btns[0].click()
                slow_pause(0.8, 1.8)
                return
        driver.execute_script("""
            const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
            if(b) b.click();
        """)
        slow_pause(0.2, 0.5)
    except Exception:
        pass

def _is_login_page(driver):
    return "/user/login" in (driver.current_url or "")

def _login(driver, By, WebDriverWait, EC):
    log("Iniciando login...")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL)
    slow_pause(0.2, 0.4)
    pwd.clear();   pwd.send_keys(FLOW_PASS)
    slow_pause(0.2, 0.4)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause()
    log("Login exitoso")

def _ensure_logged_in(driver, max_tries, By, WebDriverWait, EC):
    for _ in range(max_tries):
        if not _is_login_page(driver):
            return True
        log("Sesión caducada. Reintentando login...")
        _login(driver, By, WebDriverWait, EC)
        slow_pause(0.5, 1.2)
        if not _is_login_page(driver):
            return True
    return False

def _full_scroll(driver):
    last_h = 0
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            break
        last_h = h

def extract_event_details(container_html):
    """Extrae detalles específicos de un evento del HTML"""
    soup = BeautifulSoup(container_html, 'html.parser')
    
    event_data = {}
    
    # ID del evento
    event_container = soup.find('div', class_='group mb-6')
    if event_container:
        event_data['id'] = event_container.get('id', '')
    
    # Información básica
    info_div = soup.find('div', class_='relative flex flex-col w-full pt-1 pb-6 mb-4 border-b border-gray-300')
    if info_div:
        # Fechas
        date_elems = info_div.find_all('div', class_='text-xs')
        if date_elems:
            event_data['fechas'] = date_elems[0].get_text(strip=True)
        
        # Organización
        if len(date_elems) > 1:
            event_data['organizacion'] = date_elems[1].get_text(strip=True)
        
        # Nombre del evento
        name_elem = info_div.find('div', class_='font-caption text-lg text-black truncate -mt-1')
        if name_elem:
            event_data['nombre'] = name_elem.get_text(strip=True)
        
        # Club organizador
        club_elem = info_div.find('div', class_='text-xs mb-0.5 mt-0.5')
        if club_elem:
            event_data['club'] = club_elem.get_text(strip=True)
        
        # Lugar - buscar en todos los divs con text-xs
        location_divs = info_div.find_all('div', class_='text-xs')
        for div in location_divs:
            text = div.get_text(strip=True)
            if '/' in text and ('Spain' in text or 'España' in text):
                event_data['lugar'] = text
                break
    
    # Estado del evento
    status_button = soup.find('div', class_='py-1 px-4 border text-white font-bold rounded text-sm')
    if status_button:
        event_data['estado'] = status_button.get_text(strip=True)
        # Determinar tipo de estado
        if 'Inscribirse' in event_data['estado']:
            event_data['estado_tipo'] = 'inscripcion_abierta'
        elif 'En curso' in event_data['estado']:
            event_data['estado_tipo'] = 'en_curso'
        else:
            event_data['estado_tipo'] = 'desconocido'
    
    # Enlaces
    event_data['enlaces'] = {}
    info_link = soup.find('a', href=lambda x: x and '/info/' in x)
    if info_link:
        event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
    
    participants_link = soup.find('a', href=lambda x: x and '/participants_list' in x)
    if participants_link:
        event_data['enlaces']['participantes'] = urljoin(BASE, participants_link['href'])
    
    runs_link = soup.find('a', href=lambda x: x and '/runs' in x)
    if runs_link:
        event_data['enlaces']['runs'] = urljoin(BASE, runs_link['href'])
    
    # Bandera del país
    flag_div = soup.find('div', class_='text-md')
    if flag_div:
        event_data['pais_bandera'] = flag_div.get_text(strip=True)
    
    return event_data

def main():
    """Función principal"""
    log("=== Scraping FlowAgility - Competiciones de Agility ===")
    
    # Importar Selenium
    (webdriver, By, Options, WebDriverWait, EC, 
     JavascriptException, StaleElementReferenceException, 
     NoSuchElementException, ElementClickInterceptedException, 
     TimeoutException) = _import_selenium()
    
    driver = _get_driver()
    
    try:
        # Login
        _login(driver, By, WebDriverWait, EC)
        
        # Navegar a eventos
        log("Navegando a la página de eventos...")
        driver.get(EVENTS_URL)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Aceptar cookies
        _accept_cookies(driver, By)
        
        # Scroll completo para cargar todos los eventos
        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(2, 3)
        
        # Obtener HTML de la página
        page_html = driver.page_source
        
        # Extraer eventos
        log("Extrayendo información de eventos...")
        events = []
        
        # Buscar todos los contenedores de eventos
        soup = BeautifulSoup(page_html, 'html.parser')
        event_containers = soup.find_all('div', class_='group mb-6')
        
        log(f"Encontrados {len(event_containers)} eventos")
        
        for i, container in enumerate(event_containers, 1):
            try:
                event_data = extract_event_details(str(container))
                events.append(event_data)
                log(f"Procesado evento {i}/{len(event_containers)}: {event_data.get('nombre', 'Sin nombre')}")
            except Exception as e:
                log(f"Error procesando evento {i}: {str(e)}")
                continue
        
        # Guardar resultados
        output_file = os.path.join(OUT_DIR, 'events.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Extracción completada. {len(events)} eventos guardados en {output_file}")
        
        # Mostrar resumen
        print(f"\n{'='*80}")
        print("RESUMEN DE COMPETICIONES ENCONTRADAS:")
        print(f"{'='*80}")
        
        for i, event in enumerate(events, 1):
            print(f"\n{i}. {event.get('nombre', 'Sin nombre')}")
            print(f"   📅 {event.get('fechas', 'Fecha no especificada')}")
            print(f"   🏢 {event.get('organizacion', 'Organización no especificada')}")
            print(f"   🏆 {event.get('club', 'Club no especificado')}")
            print(f"   📍 {event.get('lugar', 'Lugar no especificado')}")
            print(f"   🚦 {event.get('estado', 'Estado no especificado')}")
        
        print(f"\n{'='*80}")
        print(f"Total: {len(events)} competiciones de agility")
        
    except Exception as e:
        log(f"Error durante el scraping: {str(e)}")
        _save_screenshot(driver, "error_screenshot.png")
        
    finally:
        driver.quit()
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
