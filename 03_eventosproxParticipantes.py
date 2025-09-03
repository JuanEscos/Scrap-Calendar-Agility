#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FLOWAGILITY PARTICIPANTS SCRAPER
=================================
- Login con .env (FLOW_EMAIL/FLOW_PASS)
- Detecta eventos pendientes desde ./output/events.json
- Hace click por fila (booking_details_show) y extrae pares label→valor de grids
- Rellena fechas/Grado/Categoría (cuando aparecen)
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
PARTICIPANTS_DIR = os.path.join(OUTPUT_DIR, "participants")
DEBUG_DIR = os.path.join(OUTPUT_DIR, "debug_participants")

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

UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)

# ============================ UTILS ============================

def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def print_header():
    print("\n" + "="*80)
    print("FLOWAGILITY PARTICIPANTS SCRAPER".center(80))
    print("="*80)
    print("• Extrae datos de participantes por competición")
    print("• Click en cada fila de la lista para abrir el detalle")
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

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

# Etiquetas aceptadas -> clave destino en ES (sin acentos)
LABELS = {
    "dorsal":        "Dorsal",
    "bib":           "Dorsal",
    "start":         "Dorsal",

    "guia":          "Guia",
    "guía":          "Guia",
    "handler":       "Guia",
    "guide":         "Guia",

    "perro":         "Perro",
    "dog":           "Perro",

    "raza":          "Raza",
    "breed":         "Raza",

    "edad":          "Edad",
    "age":           "Edad",

    "genero":        "Genero",
    "género":        "Genero",
    "gender":        "Genero",
    "sex":           "Genero",

    "altura":        "Altura_cm",
    "height":        "Altura_cm",
    "height (cm)":   "Altura_cm",

    "pedigree":      "Pedigree",

    "licencia":      "Licencia",
    "license":       "Licencia",
    "licence":       "Licencia",

    "federacion":    "Federacion",
    "federación":    "Federacion",
    "federation":    "Federacion",

    "club":          "Club",

    "grado":         "Grado",
    "level":         "Grado",

    "categoria":     "Categoria",
    "categoría":     "Categoria",
    "category":      "Categoria",
    "size":          "Categoria",
}

def _map_label_to_key(txt: str):
    t = strip_accents((txt or "").strip().lower())
    if t.endswith(":"):
        t = t[:-1].strip()
    return LABELS.get(t, None)

def _uuid_from_filename(name: str) -> str:
    m = UUID_RE.search(name)
    return m.group(0).lower() if m else ""

# ============================ DETECCIÓN DE EVENTOS ============================

def get_scraped_events():
    """IDs de eventos ya procesados (por archivos en ./output/participants)."""
    processed = set()
    try:
        for filename in os.listdir(PARTICIPANTS_DIR):
            if not filename.endswith(".json"):
                continue
            uid = _uuid_from_filename(filename)
            if uid:
                processed.add(uid)
                continue
            # Compatibilidad: si antiguamente se guardaba wrapper con event_id:
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
    events_file = os.path.join(OUTPUT_DIR, "events.json")
    if not os.path.exists(events_file):
        log(f"❌ ERROR: No se encuentra {events_file}")
        return []

    try:
        with open(events_file, "r", encoding="utf-8") as f:
            all_events = json.load(f)
        log(f"📊 Total eventos en events.json: {len(all_events)}")

        processed = get_scraped_events()
        log(f"📊 Eventos ya procesados: {len(processed)}")

        for ev in all_events:
            ev_id = ev.get("id")
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
    opts.add_experimental_option("useAutomationExtension", False)
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
    Versión robusta: tras cada click, recorre bloques grid-cols-2 y toma pares (label,value)
    sin depender de clases. También extrae fechas de mangas y rellena 'Competiciones'.
    """
    log(f"   Accediendo a: {participants_url}")
    participants = []
    try:
        driver.get(participants_url)
        WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        slow_pause(1.0, 1.5)

        # Scroll para carga lazy
        last_h = 0
        for _ in range(MAX_SCROLLS):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_WAIT_S)
            h = driver.execute_script("return document.body.scrollHeight;")
            if h == last_h:
                break
            last_h = h

        # Recoge booking_ids (y se reutilizan para encontrar cada fila al vuelo)
        btns = driver.find_elements(By.CSS_SELECTOR, '[phx-click="booking_details_show"]')
        booking_ids = []
        for b in btns:
            bid = b.get_attribute("phx-value-booking_id")
            if bid:
                booking_ids.append(bid)

        log(f"   Filas con detalle detectadas: {len(booking_ids)}")

        for idx, bid in enumerate(booking_ids, 1):
            try:
                # Click por selector con el booking_id actual (evita stale references)
                sel = f'[phx-click="booking_details_show"][phx-value-booking_id="{bid}"]'
                btn = driver.find_element(By.CSS_SELECTOR, sel)
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", btn)

                # Espera panel de detalle asociado (o usa grid más reciente como fallback)
                root = None
                try:
                    root = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, f"//*[@id='{bid}']"))
                    )
                except Exception:
                    grids = driver.find_elements(By.XPATH, "//div[contains(@class,'grid') and contains(@class,'grid-cols-2')]")
                    root = grids[-1] if grids else None

                # Estructura base del participante
                p = {
                    "ID": bid,
                    "Dorsal": "", "Guia": "", "Perro": "", "Raza": "",
                    "Edad": "", "Genero": "", "Altura_cm": "", "Pedigree": "",
                    "Licencia": "", "Federacion": "", "Club": "",
                    "Competiciones": {}
                }

                # 1) Parse por pares label→valor en grids
                if root:
                    if root.get_attribute("id") == bid:
                        blocks = driver.find_elements(By.XPATH, f"//*[@id='{bid}']//div[contains(@class,'grid') and contains(@class,'grid-cols-2')]")
                    else:
                        blocks = root.find_elements(By.XPATH, ".//div[contains(@class,'grid') and contains(@class,'grid-cols-2')]")
                    if not blocks:
                        # fallback: busca grids globales cercanos
                        blocks = driver.find_elements(By.XPATH, "//div[contains(@class,'grid') and contains(@class,'grid-cols-2')]")

                    for block in blocks:
                        cells = block.find_elements(By.XPATH, "./div")
                        i = 0
                        while i + 1 < len(cells):
                            lab = cells[i].text.strip()
                            val = cells[i+1].text.strip()
                            key = _map_label_to_key(lab)
                            if key:
                                if key == "Altura_cm":
                                    m = re.search(r"(\d{2,3})", val)
                                    if m: val = m.group(1)
                                p[key] = val
                            i += 2

                # 2) Fallback: clases antiguas (por si siguen presentes)
                if not any(p[k] for k in ("Dorsal","Guia","Perro","Raza","Edad","Genero","Altura_cm","Licencia","Federacion","Club")):
                    soup = BeautifulSoup(driver.page_source, "lxml")
                    detail = soup.find("div", {"id": bid})
                    if not detail:
                        detail = soup.find("div", class_=lambda c: c and "break-all" in c)
                    if detail:
                        labels = detail.find_all("div", class_="text-gray-500 text-sm")
                        values = detail.find_all("div", class_="font-bold break-all relative text-sm text-black")
                        for lab, val in zip(labels, values):
                            lt = (lab.get_text(strip=True) or "")
                            key = _map_label_to_key(lt)
                            if key:
                                vt = val.get_text(strip=True)
                                if key == "Altura_cm":
                                    m = re.search(r"(\d{2,3})", vt)
                                    if m: vt = m.group(1)
                                p[key] = vt

                # 3) Fechas/Grado/Categoría
                fechas = []
                date_headers = driver.find_elements(By.XPATH, f"//*[@id='{bid}']//div[contains(@class,'font-bold') and contains(@class,'border-b')]") \
                               or driver.find_elements(By.XPATH, "//div[contains(@class,'font-bold') and contains(@class,'border-b')]")
                for hdr in date_headers[:10]:
                    try:
                        val = hdr.find_element(By.XPATH, "following-sibling::div[1]").text.strip()
                        if val:
                            fechas.append(val)
                    except Exception:
                        pass

                compmap = {}
                for i, fch in enumerate(fechas[:10], 1):
                    compmap[f"comp_{i}"] = {"Fecha": fch}

                # Inferencias básicas si no vinieron explícitas
                all_txt = ""
                try:
                    cont = driver.find_element(By.XPATH, f"//*[@id='{bid}']")
                    all_txt = cont.text
                except Exception:
                    pass
                if not p.get("Grado"):
                    m = re.search(r"\b(?:pre|g?\s*[123])\b", strip_accents(all_txt).lower())
                    if m: p["Grado"] = m.group(0).upper().replace("G", "").strip()
                if not p.get("Categoria"):
                    m = re.search(r"\b(xs|s|m|i|l|20|30|40|50)\b", all_txt.upper())
                    if m: p["Categoria"] = m.group(1)

                if compmap:
                    first_key = next(iter(compmap))
                    if p.get("Grado"):     compmap[first_key]["Grado"] = p["Grado"]
                    if p.get("Categoria"): compmap[first_key]["Categoria"] = p["Categoria"]
                    p["Competiciones"] = compmap

                # Limpieza típica “Mi Perro 10”
                if isinstance(p["Perro"], str) and p["Perro"].lower().startswith("mi perro"):
                    p["Perro"] = p["Perro"].replace("Mi Perro", "").strip()

                participants.append(p)

                if idx % 25 == 0:
                    log(f"   … procesados {idx}/{len(booking_ids)} participantes")

            except Exception as e:
                log(f"   ⚠️ Error en participante #{idx}: {e}")
                # Dump rápido para depurar
                try:
                    with open(os.path.join(DEBUG_DIR, f"p_{bid}.html"), "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                except Exception:
                    pass
                continue

        log(f"   Total participantes extraídos: {len(participants)}")
        return participants

    except Exception as e:
        log(f"❌ Error extrayendo participantes: {e}")
        # Dump global
        try:
            with open(os.path.join(DEBUG_DIR, "participants_error.html"), "w", encoding="utf-8") as f:
                f.write(driver.page_source)
        except Exception:
            pass
        return []

# ============================ SAVE ============================

def save_participants_to_json(participants, event_name, event_id):
    """
    Guarda SOLO la lista (no wrapper) en:
      ./output/participants/participants_{NOMBRE_LIMPIO}_{ID}.json
    Es el formato que el 04 espera para unir sin vacíos.
    """
    safe_name = re.sub(r'[\\/*?:"<>|]', "_", event_name)
    filename = f"participants_{safe_name}_{event_id}.json"
    filepath = os.path.join(PARTICIPANTS_DIR, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
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

        for i, event in enumerate(events_to_scrape[:3], 1):
            event_id = event.get("id", f"event_{i}")
            event_name = event.get("nombre", "Sin nombre")
            participants_url = event.get("enlaces", {}).get("participantes", "")

            if not participants_url:
                log(f"{i}/{len(events_to_scrape)}: ⚠️ Sin URL de participantes - {event_name}")
                continue

            log(f"{i}/{len(events_to_scrape)}: 📋 Procesando {event_name}")
            participants = extract_participants_data(driver, participants_url)

            if participants:
                save_participants_to_json(participants, event_name, event_id)
                log(f"   ✅ {len(participants)} participantes extraídos")
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
