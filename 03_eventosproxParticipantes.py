#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FLOWAGILITY PARTICIPANTS SCRAPER
=================================
Extrae participantes desde FlowAgility:
- Login con .env (FLOW_EMAIL/FLOW_PASS)
- Detecta eventos pendientes desde ./output/events.json
- Clic por fila (booking_details_show) y empareja labels/valores
- Guarda JSON por evento en ./output/participants/participants_{NOMBRE}_{UUID}.json
"""

import os
import re
import time
import json
import random
import unicodedata

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ============================ CONFIGURACIÓN ============================
load_dotenv()

BASE_URL = "https://www.flowagility.com"
LOGIN_URL = f"{BASE_URL}/user/login"
OUTPUT_DIR = os.getenv("OUT_DIR", "./output")
PARTICIPANTS_DIR = os.path.join(OUTPUT_DIR, 'participants')
DEBUG_DIR = os.path.join(OUTPUT_DIR, 'debug_participants')

USERNAME = os.getenv("FLOW_EMAIL")
PASSWORD = os.getenv("FLOW_PASS")

if not USERNAME or not PASSWORD:
    print("❌ ERROR: Falta configurar FLOW_EMAIL o FLOW_PASS en el archivo .env")
    print("   Crea un archivo .env con tus credenciales:")
    print("   FLOW_EMAIL=tu_email@ejemplo.com")
    print("   FLOW_PASS=tu_password")
    exit(1)

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "10"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "2.0"))
SLOW_MIN_S = float(os.getenv("SLOW_MIN_S", "1.0"))
SLOW_MAX_S = float(os.getenv("SLOW_MAX_S", "3.0"))

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(PARTICIPANTS_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.I)

def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def print_header():
    print("\n" + "="*80)
    print("FLOWAGILITY PARTICIPANTS SCRAPER".center(80))
    print("="*80)
    print("• Extrae datos de participantes por competición")
    print("• Hace click fila a fila para abrir el detalle")
    print("• Guarda JSON por evento en ./output/participants")
    print("="*80)
    print(f"📁 Directorio de salida: {OUTPUT_DIR}")
    print(f"🔐 Usuario: {USERNAME}")
    print("="*80 + "\n")

def print_config():
    if os.getenv("SHOW_CONFIG", "false").lower() == "true":
        print("⚙️  CONFIG ACTUAL:")
        print(f"   • Headless: {HEADLESS}")
        print(f"   • Incognito: {INCOGNITO}")
        print(f"   • Max Scrolls: {MAX_SCROLLS}")
        print(f"   • Scroll Wait: {SCROLL_WAIT_S}s")
        print(f"   • Pausa min/max: {SLOW_MIN_S}/{SLOW_MAX_S}s")
        print()

def slow_pause(min_s=None, max_s=None):
    a = SLOW_MIN_S if min_s is None else float(min_s)
    b = SLOW_MAX_S if max_s is None else float(max_s)
    if b < a: a, b = b, a
    time.sleep(random.uniform(a, b))

# ============================ DETECCIÓN DE EVENTOS ============================

def _uuid_from_filename(name):
    m = UUID_RE.search(name)
    return m.group(0).lower() if m else ""

def get_scraped_events():
    """IDs de eventos ya procesados (por archivos en ./output/participants)."""
    processed = set()
    try:
        for filename in os.listdir(PARTICIPANTS_DIR):
            if not filename.endswith(".json"): 
                continue
            # 1) intenta sacar UUID del nombre
            uid = _uuid_from_filename(filename)
            if uid:
                processed.add(uid)
                continue
            # 2) si no hay UUID en nombre, abre y mira 'event_id'
            try:
                with open(os.path.join(PARTICIPANTS_DIR, filename), "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and "event_id" in data:
                    processed.add(str(data["event_id"]).strip())
            except Exception:
                pass
    except Exception as e:
        log(f"⚠️ Error obteniendo eventos procesados: {e}")
    return processed

def get_events_to_scrape():
    """Eventos pendientes desde ./output/events.json (id, nombre, enlaces.participantes)."""
    log("Buscando eventos para procesar…")
    events_to_scrape = []
    events_file = os.path.join(OUTPUT_DIR, 'events.json')
    if not os.path.exists(events_file):
        log(f"❌ ERROR: No se encuentra {events_file}")
        return []

    try:
        with open(events_file, 'r', encoding='utf-8') as f:
            all_events = json.load(f)
        log(f"📊 Total eventos en events.json: {len(all_events)}")

        processed = get_scraped_events()
        log(f"📊 Eventos ya procesados: {len(processed)}")

        for ev in all_events:
            ev_id = ev.get('id')
            if ev_id and ev_id not in processed:
                events_to_scrape.append(ev)

        log(f"🎯 Por procesar: {len(events_to_scrape)}")
        for e in events_to_scrape[:3]:
            log(f"   Pendiente: ID={e.get('id')}, {e.get('nombre')}")
    except Exception as e:
        log(f"❌ Error leyendo events.json: {e}")
    return events_to_scrape

# ============================ NAVEGADOR ============================

def get_driver():
    opts = Options()
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        return driver
    except Exception as e:
        log(f"Error inicializando ChromeDriver: {e}")
        return webdriver.Chrome(options=opts)

def login(driver):
    log("Iniciando sesión…")
    try:
        driver.get(LOGIN_URL)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "user[email]")))
        driver.find_element(By.NAME, "user[email]").send_keys(USERNAME)
        slow_pause(0.6, 1.0)
        driver.find_element(By.NAME, "user[password]").send_keys(PASSWORD)
        slow_pause(0.4, 0.8)
        driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
        WebDriverWait(driver, 20).until(lambda d: "/user/login" not in d.current_url)
        log("✅ Login exitoso")
        return True
    except Exception as e:
        log(f"❌ Error durante login: {e}")
        return False

# ============================ EXTRACCIÓN ============================

def extract_participants_data(driver, participants_url):
    """
    Abre la lista de participantes, hace click en cada fila (booking_details_show),
    empareja etiquetas/valores y extrae fechas de pruebas para construir
    'Competiciones' (usado por el 04).
    """
    log(f"   Accediendo a: {participants_url}")
    participants = []
    try:
        driver.get(participants_url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        slow_pause(1.0, 1.5)

        # scroll para forzar carga lazy
        last_h = 0
        for _ in range(MAX_SCROLLS):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_WAIT_S)
            h = driver.execute_script("return document.body.scrollHeight;")
            if h == last_h:
                break
            last_h = h

        # Obtén la lista de booking_ids (más robusto que mantener referencias de los botones)
        btns = driver.find_elements(By.CSS_SELECTOR, '[phx-click="booking_details_show"]')
        booking_ids = []
        for b in btns:
            bid = b.get_attribute("phx-value-booking_id")
            if bid:
                booking_ids.append(bid)

        log(f"   Filas con detalle detectadas: {len(booking_ids)}")

        for idx, bid in enumerate(booking_ids, 1):
            try:
                # Encuentra el botón por su booking_id actual (evita stale elements)
                sel = f'[phx-click="booking_details_show"][phx-value-booking_id="{bid}"]'
                btn = driver.find_element(By.CSS_SELECTOR, sel)
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(1.0)

                soup = BeautifulSoup(driver.page_source, "lxml")
                detail = soup.find("div", {"id": bid})
                if not detail:
                    # fallback al último detalle expandido
                    detail = soup.find("div", class_=lambda c: c and "break-all" in c)

                p = {
                    "ID": bid or "",
                    "Dorsal": "",
                    "Guia": "",
                    "Perro": "",
                    "Raza": "",
                    "Edad": "",
                    "Genero": "",
                    "Altura_cm": "",
                    "Pedigree": "",
                    "Licencia": "",
                    "Federacion": "",
                    "Club": "",
                    "Competiciones": {}
                }

                if detail:
                    labels = detail.find_all("div", class_="text-gray-500 text-sm")
                    values = detail.find_all("div", class_="font-bold break-all relative text-sm text-black")

                    for lab, val in zip(labels, values):
                        lt = (lab.get_text(strip=True) or "").lower()
                        vt = val.get_text(strip=True)
                        if lt == "dorsal":                   p["Dorsal"] = vt
                        elif lt in ("guía", "guia"):         p["Guia"] = vt
                        elif lt == "perro":                  p["Perro"] = vt
                        elif lt == "raza":                   p["Raza"] = vt
                        elif lt == "edad":                   p["Edad"] = vt
                        elif lt in ("género","genero"):      p["Genero"] = vt
                        elif lt.startswith("altura"):        p["Altura_cm"] = vt
                        elif "pedigree" in lt:               p["Pedigree"] = vt
                        elif lt == "club":                   p["Club"] = vt
                        elif lt == "licencia":               p["Licencia"] = vt
                        elif lt in ("federación","federacion"): p["Federacion"] = vt
                        elif lt in ("grado","level"):        p.setdefault("_grado_hint", vt)
                        elif lt in ("categoría","categoria","size","category"): p.setdefault("_cat_hint", vt)

                    # Fechas de mangas/pruebas (máx 10) – mismos selectores que tu versión previa
                    headers = detail.find_all('div', class_='mt-4 col-span-2 font-bold text-sm border-b mb-1 pb-1 border-gray-400')
                    fechas = []
                    for j, header in enumerate(headers):
                        if j > 9:
                            break
                        fecha_tag = header.find_next_sibling('div', class_='font-bold break-all relative text-sm text-black')
                        if fecha_tag:
                            fechas.append(fecha_tag.get_text(strip=True))

                    compmap = {}
                    for i, fch in enumerate(fechas[:10], 1):
                        compmap[f"comp_{i}"] = {"Fecha": fch}
                    if compmap:
                        first_key = next(iter(compmap))
                        if p.get("_grado_hint"):
                            compmap[first_key]["Grado"] = p.pop("_grado_hint")
                        if p.get("_cat_hint"):
                            compmap[first_key]["Categoria"] = p.pop("_cat_hint")
                        p["Competiciones"] = compmap

                # Normaliza altura (solo número)
                if isinstance(p["Altura_cm"], str):
                    m = re.search(r"(\d{2,3})", p["Altura_cm"])
                    if m: p["Altura_cm"] = m.group(1)

                # Bug típico: "Mi Perro 10" → limpia
                if isinstance(p["Perro"], str) and p["Perro"].lower().startswith("mi perro"):
                    p["Perro"] = p["Perro"].replace("Mi Perro", "").strip()

                participants.append(p)
                if idx % 20 == 0:
                    log(f"   … procesados {idx}/{len(booking_ids)} participantes")

            except Exception as e:
                log(f"   ⚠️ Error en participante #{idx}: {e}")
                continue

        log(f"   Total participantes extraídos: {len(participants)}")
        return participants

    except Exception as e:
        log(f"❌ Error extrayendo participantes: {e}")
        # dump de depuración
        try:
            with open(os.path.join(DEBUG_DIR, "participants_error.html"), "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            log("   (HTML de depuración guardado)")
        except Exception:
            pass
        return []

def save_participants_to_json(participants, event_name, event_id):
    """
    Guarda SOLO la lista (no wrapper) en:
      ./output/participants/participants_{NOMBRE_LIMPIO}_{ID}.json
    Es el formato que el 04 espera para unir sin vacíos.
    """
    safe_name = re.sub(r'[\\/*?:"<>|]', "_", event_name)
    filename  = f"participants_{safe_name}_{event_id}.json"
    filepath  = os.path.join(PARTICIPANTS_DIR, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(participants, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log(f"❌ Error guardando archivo: {e}")
        return False

# ============================ MAIN ============================

def main():
    print_header()
    print_config()

    events_to_scrape = get_events_to_scrape()
    if not events_to_scrape:
        log("🎉 ¡Todos los eventos ya han sido procesados!")
        return

    driver = get_driver()
    try:
        if not login(driver):
            return

        for i, event in enumerate(events_to_scrape, 1):
            event_id = event.get('id', f'event_{i}')
            event_name = event.get('nombre', 'Sin nombre')
            participants_url = event.get('enlaces', {}).get('participantes', '')

            if not participants_url:
                log(f"{i}/{len(events_to_scrape)}: ⚠️ Sin URL de participantes - {event_name}")
                continue

            log(f"{i}/{len(events_to_scrape)}: 📋 Procesando {event_name}")
            participants = extract_participants_data(driver, participants_url)

            if participants:
                save_participants_to_json(participants, event_name, event_id)
                log(f"   ✅ {len(participants)} participantes extraídos")
                # Ejemplo
                sample = participants[0]
                log(f"   👤 Ejemplo: D{sample.get('Dorsal','?')} - {sample.get('Guia','?')} / {sample.get('Perro','?')}")
            else:
                log("   ⚠️ No se encontraron participantes")

            slow_pause(2.5, 4.5)

        log("🎉 ¡Procesamiento completado!")

    except Exception as e:
        log(f"❌ Error durante la extracción: {e}")
    finally:
        driver.quit()
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
