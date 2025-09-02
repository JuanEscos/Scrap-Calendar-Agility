#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FLOWAGILITY PARTICIPANTS SCRAPER
=================================

Script automatizado para extraer información de participantes de competiciones caninas
de FlowAgility.com. Este sistema:

1. ✅ Inicia sesión automáticamente con credenciales seguras (.env)
2. ✅ Detecta eventos no procesados previamente
3. ✅ Extrae datos completos de participantes (guía, perro, raza, edad, etc.)
4. ✅ Procesa competiciones separadas con grado y categoría
5. ✅ Genera archivos JSON estructurados
6. ✅ Reanuda automáticamente donde se quedó

Características especiales:
- Corrección automática del problema "Mi Perro 10"
- Extracción inteligente de grado (G1, G2, G3, PRE, etc.)
- Detección de categoría (I, L, M, S, XS, 20, 30, etc.)
- Sistema anti-detection con pausas aleatorias
- Manejo robusto de errores

Requisitos:
- pip install selenium webdriver-manager beautifulsoup4

Uso:
- Configure las credenciales en el archivo .env
- Ejecute: python flowagility_scraper.py

Autor: Sistema Automatizado
Versión: 2.0
"""

import os
import re
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import unicodedata
from dotenv import load_dotenv
import random

# ============================ CONFIGURACIÓN ============================
# Cargar variables de entorno
load_dotenv()

# Configuración desde .env
BASE_URL = "https://www.flowagility.com"
LOGIN_URL = f"{BASE_URL}/user/login"
OUTPUT_DIR = os.getenv("OUT_DIR", "./output")
PARTICIPANTS_DIR = os.path.join(OUTPUT_DIR, 'participants')
DEBUG_DIR = os.path.join(OUTPUT_DIR, 'debug_participants')

# Credenciales desde .env
USERNAME = os.getenv("FLOW_EMAIL")
PASSWORD = os.getenv("FLOW_PASS")

# Validar credenciales
if not USERNAME or not PASSWORD:
    print("❌ ERROR: Falta configurar FLOW_EMAIL o FLOW_PASS en el archivo .env")
    print("   Crea un archivo .env con tus credenciales:")
    print("   FLOW_EMAIL=tu_email@ejemplo.com")
    print("   FLOW_PASS=tu_password")
    exit(1)

# Configuración de scraping
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "10"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "2.0"))
SLOW_MIN_S = float(os.getenv("SLOW_MIN_S", "1.0"))
SLOW_MAX_S = float(os.getenv("SLOW_MAX_S", "3.0"))

# Crear directorios
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(PARTICIPANTS_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

def log(message):
    """Función de logging con timestamp"""
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def print_header():
    """Imprime la cabecera descriptiva del script"""
    print("\n" + "="*80)
    print("FLOWAGILITY PARTICIPANTS SCRAPER".center(80))
    print("="*80)
    print("• Extracción automatizada de datos de participantes")
    print("• Detección inteligente de eventos no procesados")
    print("• Generación de JSON estructurados")
    print("• Sistema de reanudación automática")
    print("="*80)
    print(f"📁 Directorio de salida: {OUTPUT_DIR}")
    print(f"🔐 Usuario: {USERNAME}")
    print(f"📊 Eventos a procesar: Detectados automáticamente")
    print("="*80 + "\n")

def print_config():
    """Muestra la configuración cargada"""
    if os.getenv("SHOW_CONFIG", "false").lower() == "true":
        print("⚙️  CONFIGURACIÓN ACTUAL:")
        print(f"   • Headless: {HEADLESS}")
        print(f"   • Incognito: {INCOGNITO}")
        print(f"   • Max Scrolls: {MAX_SCROLLS}")
        print(f"   • Scroll Wait: {SCROLL_WAIT_S}s")
        print(f"   • Pausa mínima: {SLOW_MIN_S}s")
        print(f"   • Pausa máxima: {SLOW_MAX_S}s")
        print()

def slow_pause(min_s=None, max_s=None):
    """Pausa aleatoria para no saturar"""
    a = SLOW_MIN_S if min_s is None else float(min_s)
    b = SLOW_MAX_S if max_s is None else float(max_s)
    if b < a: a, b = b, a
    time.sleep(random.uniform(a, b))

# ============================ FUNCIONES DE DETECCIÓN DE EVENTOS ============================

def get_scraped_events():
    """
    Obtiene lista de IDs de eventos que ya han sido procesados
    revisando los archivos JSON existentes en el directorio de participantes
    """
    processed_events = set()
    
    try:
        if not os.path.exists(PARTICIPANTS_DIR):
            os.makedirs(PARTICIPANTS_DIR, exist_ok=True)
            return processed_events
        
        # Buscar archivos JSON en el directorio de participantes
        for filename in os.listdir(PARTICIPANTS_DIR):
            if filename.endswith('.json'):
                # Múltiples patrones posibles para extraer el ID
                patterns = [
                    r'participants_(.+)\.json',
                    r'(.+)_participants\.json',
                    r'(.+)\.json'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, filename)
                    if match:
                        processed_events.add(match.group(1))
                        break
        
    except Exception as e:
        log(f"⚠️ Error obteniendo eventos procesados: {e}")
    
    return processed_events

def get_events_to_scrape():
    """
    Obtiene la lista de eventos que necesitan ser procesados (no scraped previamente)
    Retorna lista de diccionarios con información de eventos
    """
    log("Buscando eventos para procesar...")
    
    events_to_scrape = []
    events_file = os.path.join(OUTPUT_DIR, 'events.json')
    
    # Verificar si el archivo existe
    if not os.path.exists(events_file):
        log(f"❌ ERROR: No se encuentra el archivo {events_file}")
        log("💡 Debes ejecutar primero el script de extracción de eventos")
        log("💡 O crear manualmente el archivo events.json con la estructura adecuada")
        return []
    
    try:
        with open(events_file, 'r', encoding='utf-8') as f:
            all_events = json.load(f)
        
        log(f"📊 Total eventos en events.json: {len(all_events)}")
        
        # Mostrar primeros 3 eventos para debug
        for i, event in enumerate(all_events[:3]):
            log(f"   Evento {i+1}: ID={event.get('id')}, Nombre={event.get('nombre')}")
        
        # Obtener eventos ya procesados
        processed_events = get_scraped_events()
        log(f"📊 Eventos ya procesados: {len(processed_events)}")
        if processed_events:
            log(f"   IDs procesados: {list(processed_events)[:5]}{'...' if len(processed_events) > 5 else ''}")
        
        # Filtrar eventos no procesados
        for event in all_events:
            event_id = event.get('id')
            if event_id and event_id not in processed_events:
                events_to_scrape.append(event)
        
        log(f"🎯 Eventos por procesar: {len(events_to_scrape)}")
        
        # Mostrar eventos pendientes
        for event in events_to_scrape[:3]:
            log(f"   Pendiente: ID={event.get('id')}, {event.get('nombre')}")
        
    except Exception as e:
        log(f"❌ Error leyendo events.json: {e}")
        import traceback
        traceback.print_exc()
    
    return events_to_scrape

# ============================ FUNCIONES PRINCIPALES ============================

def get_driver():
    """Configura y retorna el driver de Selenium"""
    opts = Options()
    if HEADLESS: opts.add_argument("--headless=new")
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
    """Inicia sesión en FlowAgility"""
    log("Iniciando sesión...")
    
    try:
        driver.get(LOGIN_URL)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "user[email]")))
        
        email_field = driver.find_element(By.NAME, "user[email]")
        password_field = driver.find_element(By.NAME, "user[password]")
        
        email_field.clear()
        email_field.send_keys(USERNAME)
        time.sleep(1)
        
        password_field.clear()
        password_field.send_keys(PASSWORD)
        time.sleep(1)
        
        login_button = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
        login_button.click()
        
        WebDriverWait(driver, 20).until(lambda d: "/user/login" not in d.current_url)
        log("✅ Login exitoso")
        return True
        
    except Exception as e:
        log(f"❌ Error durante login: {str(e)}")
        return False

# --- Helpers para limpieza/normalización ---
def _t(x):
    return unicodedata.normalize("NFKC", x).strip() if isinstance(x, str) else x

def _clean_spaces(s):
    return re.sub(r"\s+", " ", s).strip() if isinstance(s, str) else s

def _text(el):
    try:
        return _clean_spaces(el.text)
    except Exception:
        return ""

GRADE_PAT = re.compile(r"\b(?:grado\s*|g)\s*(pre|1|2|3)\b", re.I)
CAT_PAT   = re.compile(r"\b(?:xs|s|m|i|l|20|30|40|50)\b", re.I)
UUID_PAT  = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)

FIELD_MAP = {
    # cabeceras -> clave normalizada
    "dorsal": "Dorsal", "bib": "Dorsal", "start": "Dorsal",
    "guia": "Guia", "guide": "Guia", "handler": "Guia", "handler name": "Guia",
    "perro": "Perro", "dog": "Perro", "dog name": "Perro",
    "raza": "Raza", "breed": "Raza",
    "edad": "Edad", "age": "Edad",
    "sexo": "Genero", "gender": "Genero", "genero": "Genero", "sex": "Genero",
    "altura": "Altura_cm", "height": "Altura_cm", "height (cm)": "Altura_cm", "height_cm": "Altura_cm",
    "pedigree": "Pedigree",
    "licencia": "Licencia", "license": "Licencia", "licence": "Licencia",
    "federacion": "Federacion", "federation": "Federacion",
    "club": "Club",
    "grado": "Grado", "grade": "Grado", "level": "Grado",
    "categoria": "Categoria", "category": "Categoria", "size": "Categoria", "size class": "Categoria",
}

def _header_to_key(h):
    h = _t(h).lower()
    h = h.replace(":", "").replace(".", "")
    return FIELD_MAP.get(h)

def _infer_grade(text):
    m = GRADE_PAT.search(text or "")
    if not m: return ""
    val = m.group(1).upper()
    return "PRE" if val == "PRE" else f"{val}"

def _infer_cat(text):
    m = CAT_PAT.search((text or "").upper())
    return m.group(0) if m else ""

def _maybe_click_cookies(driver):
    # intenta cerrar banners típicos
    sels = [
        '[data-testid="uc-accept-all-button"]',
        'button[aria-label="Accept all"]',
        'button[aria-label="Aceptar todo"]',
        'button:has(svg+span), button:has(span:contains("Accept"))',
    ]
    for s in sels:
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, s)
            if btns:
                btns[0].click()
                slow_pause(0.4, 0.8)
                return
        except Exception:
            pass

def _infinite_scroll(driver, max_scrolls=10, wait_s=1.2):
    last_h = driver.execute_script("return document.body.scrollHeight;")
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(wait_s)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            break
        last_h = h

def _normalize_row(row):
    """row puede venir de tabla o tarjeta (dict libre); devuelve dict con tus claves."""
    out = {
        "ID": row.get("ID",""),
        "Dorsal": row.get("Dorsal",""),
        "Guia": row.get("Guia",""),
        "Perro": row.get("Perro",""),
        "Raza": row.get("Raza",""),
        "Edad": row.get("Edad",""),
        "Genero": row.get("Genero",""),
        "Altura_cm": row.get("Altura_cm",""),
        "Pedigree": row.get("Pedigree",""),
        "Licencia": row.get("Licencia",""),
        "Federacion": row.get("Federacion",""),
        "Club": row.get("Club",""),
        "Grado": row.get("Grado",""),
        "Categoria": row.get("Categoria",""),
        # estructura para 04:
        "Competiciones": {}  # la llenamos luego si detectamos fechas
    }
    # Correcciones frecuentes
    if isinstance(out["Altura_cm"], str):
        m = re.search(r"(\d{2,3})", out["Altura_cm"])
        if m: out["Altura_cm"] = m.group(1)
    # Mi Perro 10 (bug típico): si dorsal=10 y nombre=Mi Perro 10 -> limpia
    if out["Perro"].lower().startswith("mi perro"):
        out["Perro"] = out["Perro"].replace("Mi Perro","").strip()
    return out

def _parse_table(soup):
    """Intenta parsear tabla de participantes <table>."""
    table = soup.find("table")
    if not table: 
        return []

    # cabeceras
    headers = []
    thead = table.find("thead")
    if thead:
        ths = thead.find_all(["th","td"])
        headers = [_t(th.get_text()) for th in ths]
    if not headers:
        # intenta primera fila del tbody
        first = table.find("tr")
        if first:
            headers = [_t(td.get_text()) for td in first.find_all(["th","td"])]

    # cuerpo
    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds: 
            continue
        r = {}
        for i, td in enumerate(tds):
            hkey = _header_to_key(headers[i]) if i < len(headers) else None
            if not hkey:
                # heurística por texto del td
                text = _text(td)
                # asignaciones por patrón
                if "@" in text and "." in text and not r.get("Email"):
                    hkey = "Email"  # no usado, pero por si acaso
                else:
                    # intenta mapear por keywords
                    m = _header_to_key(text.lower().split()[0])
            val = _text(td)
            if not hkey:
                # fallback: intenta por data-label (responsive tables)
                lab = td.get("data-label")
                if lab:
                    hkey = _header_to_key(lab)
            if hkey:
                r[hkey] = val
        if r:
            rows.append(_normalize_row(r))
    return rows

def _parse_cards(soup):
    """Intenta parsear tarjetas/list-items de participantes."""
    cards = []
    # Heurísticas: bloques con muchas etiquetas <li>, <div class*="participant|card|row">
    candidates = soup.select('div[class*="participant"], div[class*="card"], li[class*="participant"], div[class*="row"]')
    if not candidates:
        # fallback: cualquier li con varios spans
        candidates = soup.select("li:has(span)")
    for c in candidates:
        txt = _text(c)
        # filtro rápido (evita contenedores muy grandes sin datos)
        if len(txt) < 10 or len(txt) > 3000:
            continue
        r = {}
        # Busca pares label: value
        for lab in ["Dorsal","Bib","Start","Guia","Guide","Handler","Perro","Dog","Breed","Raza","Edad","Age","Sexo","Gender","Altura","Height","Licencia","License","Federacion","Federation","Club","Pedigree","Categoria","Category","Grado","Grade","Level"]:
            # lab:
            m = re.search(rf"{lab}\s*[:\-]\s*([^\n|•]+)", txt, re.I)
            if m:
                key = _header_to_key(lab.lower()) or lab
                r[key] = _clean_spaces(m.group(1))
        # También intenta detectar grado/categoría en el bloque entero
        r.setdefault("Grado", _infer_grade(txt))
        r.setdefault("Categoria", _infer_cat(txt))

        # dorsal aislado
        if not r.get("Dorsal"):
            m = re.search(r"\b(?:dorsal|bib|start)\s*(\d{1,4})\b", txt, re.I)
            if m: r["Dorsal"] = m.group(1)

        # nombre perro y guía heurísticos si faltan
        if not r.get("Perro"):
            m = re.search(r"Perro\s*[:\-]\s*([^\n|•]+)", txt, re.I)
            if m: r["Perro"] = _clean_spaces(m.group(1))
        if not r.get("Guia"):
            m = re.search(r"(?:Gu[ií]a|Handler|Guide)\s*[:\-]\s*([^\n|•]+)", txt, re.I)
            if m: r["Guia"] = _clean_spaces(m.group(1))

        if r:
            cards.append(_normalize_row(r))
    return cards

def _extract_competition_dates(soup):
    """Devuelve lista de fechas (strings) detectadas en la página (cabeceras, badges...)."""
    texts = []
    # típicos lugares donde aparecen fechas
    for sel in ["h1","h2","h3",".text-xs",".badge",".tag","header",".subtitle",".caption"]:
        for el in soup.select(sel):
            t = _text(el)
            if re.search(r"\b(20\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|Ene|Feb|Mar|Abr|May|Jun|Jul|Ago|Sep|Oct|Nov|Dic)\b", t, re.I):
                texts.append(t)
    # dedup conservando orden
    seen, out = set(), []
    for t in texts:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out[:10]

def extract_participants_data(driver, participants_url):
    """Extrae datos de participantes desde la URL proporcionada."""
    log(f"   Accediendo a: {participants_url}")
    try:
        driver.get(participants_url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        _maybe_click_cookies(driver)
        # si la lista es lazy, scrollea para asegurar carga
        _infinite_scroll(driver, MAX_SCROLLS, SCROLL_WAIT_S)
        slow_pause(1.0, 1.5)

        # a veces hay pestañas o botones "Participants"/"Inscritos"
        for text in ("Participants","Inscritos","Listado","Entrants"):
            try:
                btn = driver.find_element(By.XPATH, f"//button[contains(., '{text}') or //a[contains(., '{text}')]]")
                btn.click(); slow_pause(0.6, 1.2)
            except Exception:
                pass

        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")

        # 1) prueba tabla
        rows = _parse_table(soup)

        # 2) si no hay tabla, prueba tarjetas
        if not rows:
            rows = _parse_cards(soup)

        # 3) si aún vacío, intenta “list items” simples
        if not rows:
            items = soup.select("li")
            for li in items:
                t = _text(li)
                if len(t) < 10 or len(t) > 1200:
                    continue
                r = {
                    "Dorsal": (re.search(r"\b\d{1,4}\b", t).group(0) if re.search(r"\b\d{1,4}\b", t) else ""),
                    "Guia": "",
                    "Perro": "",
                    "Raza": "",
                }
                # heurísticos
                m = re.search(r"(?:Gu[ií]a|Handler|Guide)\s*[:\-]\s*([^\n|•]+)", t, re.I)
                if m: r["Guia"] = _clean_spaces(m.group(1))
                m = re.search(r"(?:Perro|Dog)\s*[:\-]\s*([^\n|•]+)", t, re.I)
                if m: r["Perro"] = _clean_spaces(m.group(1))
                m = re.search(r"(?:Raza|Breed)\s*[:\-]\s*([^\n|•]+)", t, re.I)
                if m: r["Raza"] = _clean_spaces(m.group(1))
                r["Grado"] = _infer_grade(t)
                r["Categoria"] = _infer_cat(t)
                rows.append(_normalize_row(r))

        # 4) intento de fechas/competitions para poblar "Competiciones"
        fechas_detectadas = _extract_competition_dates(soup)
        for r in rows:
            compmap = {}
            # si la página muestra múltiples días/rondas, usa las fechas detectadas
            for i, f in enumerate(fechas_detectadas, 1):
                compmap[f"comp_{i}"] = {"Fecha": f}
            # añade grado/categoría deducidos a la primera
            if compmap:
                first_key = next(iter(compmap))
                if r.get("Grado"): compmap[first_key]["Grado"] = r["Grado"]
                if r.get("Categoria"): compmap[first_key]["Categoria"] = r["Categoria"]
            r["Competiciones"] = compmap

        return rows

    except Exception as e:
        log(f"❌ Error extrayendo participantes: {e}")
        return []

def save_participants_to_json(participants, event_name, event_id):
    safe_name = re.sub(r'[\\/*?:"<>|]', "_", event_name)
    filepath  = os.path.join(PARTICIPANTS_DIR, f"participants_{safe_name}_{event_id}.json")
    
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            # Ideal: guardar SOLO la lista (como antes funcionaba el 04)
            json.dump(participants, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log(f"❌ Error guardando archivo: {e}")
        return False

# ============================ FUNCIÓN PRINCIPAL ============================

def main():
    """Función principal"""
    print_header()
    print_config()
    
    # Obtener eventos que necesitan scraping
    events_to_scrape = get_events_to_scrape()
    
    if not events_to_scrape:
        log("🎉 ¡Todos los eventos ya han sido procesados!")
        return
    
    driver = get_driver()
    
    try:
        if not login(driver):
            return
        
        # Procesar solo eventos no scraped
        for i, event in enumerate(events_to_scrape, 1):
            event_id = event.get('id', f'event_{i}')
            event_name = event.get('nombre', 'Sin nombre')
            participants_url = event.get('enlaces', {}).get('participantes', '')
            
            if not participants_url:
                log(f"{i}/{len(events_to_scrape)}: ⚠️ Sin URL de participantes - {event_name}")
                continue
            
            log(f"{i}/{len(events_to_scrape)}: 📋 Procesando {event_name}")
            
            # Extraer participantes
            participants = extract_participants_data(driver, participants_url)
            
            if participants:
                # Guardar resultados en JSON
                save_participants_to_json(participants, event_name, event_id)
                log(f"   ✅ {len(participants)} participantes extraídos")
                
                # Mostrar resumen rápido
                sample = participants[0]
                log(f"   👤 Ejemplo: D{sample['dorsal']} - {sample['guide']}")
                if sample.get('competitions'):
                    comp = sample['competitions'][0]
                    log(f"   🏆 {comp.get('nombre', '')} - G{comp.get('grado', '')}/{comp.get('categoria', '')}")
            else:
                log("   ⚠️ No se encontraron participantes")
            
            slow_pause(3, 5)
        
        log("🎉 ¡Procesamiento completado! Todos los eventos han sido scraped.")
        
    except Exception as e:
        log(f"❌ Error durante la extracción: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        driver.quit()
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
