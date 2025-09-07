#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_eventosproxINFO.py
- Lee ./output/01events.json (o el que pases por CLI) y visita el enlace /info de cada evento.
- Extrae información básica (título, fechas, ubicación, precios, pruebas, contacto…).
- Escribe ./output/02competiciones_detalladas.json (o el que pases por CLI).
- Usa credenciales de ENV: FLOW_EMAIL / FLOW_PASS.
- Guarda screenshot en caso de error para depuración.
"""

import os, sys, json, time, re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://www.flowagility.com"
HEADLESS  = os.getenv("HEADLESS", "true").strip().lower() in ("1","true","yes","on")
INCOGNITO = os.getenv("INCOGNITO","true").strip().lower() in ("1","true","yes","on")

FLOW_EMAIL = os.getenv("FLOW_EMAIL", "").strip()
FLOW_PASS  = os.getenv("FLOW_PASS", "").strip()

OUT_DIR = "./output"
os.makedirs(OUT_DIR, exist_ok=True)

# Entradas/salidas (CLI o por defecto)
IN_PATH  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(OUT_DIR, "01events.json")
OUT_PATH = sys.argv[2] if len(sys.argv) > 2 else os.path.join(OUT_DIR, "02competiciones_detalladas.json")

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
def slow_pause(a=0.8, b=1.8):
    import random, time as _t; _t.sleep(random.uniform(a,b))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
    from selenium.webdriver.chrome.service import Service
    return webdriver, By, Options, WebDriverWait, EC, TimeoutException, NoSuchElementException, WebDriverException, Service

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
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except Exception:
        return webdriver.Chrome(options=opts)

def _save_screenshot(driver, name="02_error.png"):
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
                btns[0].click(); slow_pause(0.4,0.8)
                return
        driver.execute_script("""
            const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
            if(b) b.click();
        """)
        slow_pause(0.2,0.6)
    except Exception:
        pass

def _login(driver, By, WebDriverWait, EC):
    if not FLOW_EMAIL or not FLOW_PASS:
        raise RuntimeError("Faltan FLOW_EMAIL/FLOW_PASS en el entorno.")
    log("Login…")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL); slow_pause(0.2,0.4)
    pwd.clear();   pwd.send_keys(FLOW_PASS);    slow_pause(0.2,0.4)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause(0.6,1.0)
    log("Login OK.")

def _extract_info_from_html(html, base_event):
    """Parsea la página /info y devuelve un dict extendiendo el evento base."""
    soup = BeautifulSoup(html, "html.parser")
    out = dict(base_event) if isinstance(base_event, dict) else {}

    # Título
    h1 = soup.find('h1') or soup.find(['h2','h3'])
    if h1: out['titulo'] = h1.get_text(strip=True)

    # Fechas y ubicación (heurística simple, flexible)
    text_blocks = soup.find_all(['div','span','p'])
    fecha_txt = None; ubic_txt = None
    for el in text_blocks:
        t = el.get_text(" ", strip=True)
        if not t: continue
        if not fecha_txt and re.search(r'\b(\d{1,2}\s*[/-]\s*\d{1,2}|\b(Ene|Feb|Mar|Abr|May|Jun|Jul|Ago|Sep|Oct|Nov|Dic|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b)', t, re.I):
            if len(t) < 90: fecha_txt = t
        if not ubic_txt and re.search(r'\b(Spain|España|Portugal|France|Italia|Germany|Belgium|Netherlands|UK|Ireland)\b', t, re.I):
            if len(t) < 90: ubic_txt = t
        if fecha_txt and ubic_txt: break

    out.setdefault('informacion_general', {})
    if fecha_txt: out['informacion_general']['fechas_completas'] = fecha_txt
    if ubic_txt:  out['informacion_general']['ubicacion_completa'] = ubic_txt

    # Inscripción: precios/periodos
    reg = {}
    for el in text_blocks:
        t = el.get_text(" ", strip=True)
        if not t: continue
        if re.search(r'(inscrip|regist|registration)', t, re.I) and len(t) < 140:
            reg['periodo_inscripcion'] = t
        if ('€' in t or 'EUR' in t.upper()) and len(t) < 140:
            reg['precios'] = t
    if reg: out['inscripcion'] = reg

    # Pruebas (muy laxo; lista simples de bloques con palabras clave)
    pruebas = []
    for sec in soup.find_all(['div','section']):
        txt = sec.get_text(" ", strip=True)
        if not txt: continue
        if re.search(r'(prueba|manga|round|agility|jumping|event)', txt, re.I):
            tit = None
            for h in sec.find_all(['h2','h3','h4','strong']):
                ht = h.get_text(" ", strip=True)
                if ht and 3 <= len(ht) <= 120:
                    tit = ht; break
            pruebas.append({"nombre": tit or txt[:60]})
    if pruebas: out['pruebas'] = pruebas

    # Contacto
    contacto = {}
    # email
    for a in soup.find_all(text=re.compile(r'@.*\.', re.I)):
        t = a.strip()
        if len(t) < 120:
            contacto['email'] = t; break
    # teléfono
    for a in soup.find_all(text=re.compile(r'\+?\d[\d\s-]{6,}', re.I)):
        t = a.strip()
        if len(t) < 40:
            contacto['telefono'] = t; break
    if contacto: out['contacto'] = contacto

    return out

def _get_info_url(ev):
    """Intenta obtener la URL /info desde varias estructuras posibles."""
    if isinstance(ev, dict):
        enlaces = ev.get("enlaces") or {}
        if isinstance(enlaces, dict) and enlaces.get("info"):
            return enlaces["info"]
        # A veces viene 'event_url' base => derivar /info
        if ev.get("event_url"):
            base = ev["event_url"].rstrip("/")
            return base + "/info"
    return None

def main():
    # Cargar base de eventos
    if not os.path.exists(IN_PATH):
        log(f"❌ No existe input: {IN_PATH}")
        # Para no romper la cadena, graba salida vacía
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        sys.exit(1)

    with open(IN_PATH, "r", encoding="utf-8") as f:
        base_events = json.load(f)
    if not isinstance(base_events, list):
        log("⚠️ El input no es una lista. Escribo salida vacía.")
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        sys.exit(1)

    (webdriver, By, Options, WebDriverWait, EC,
     TimeoutException, NoSuchElementException, WebDriverException, Service) = _import_selenium()

    driver = _get_driver()
    detailed = []
    try:
        _login(driver, By, WebDriverWait, EC)

        for i, ev in enumerate(base_events, 1):
            try:
                info_url = _get_info_url(ev)
                if not info_url:
                    # sin /info, añade el evento tal cual
                    detailed.append(ev); continue

                log(f"[{i}/{len(base_events)}] INFO -> {info_url}")
                driver.get(info_url)
                WebDriverWait(driver, 20).until(lambda d: d.find_element(By.TAG_NAME, "body"))
                _accept_cookies(driver, By)
                slow_pause()

                html = driver.page_source
                d = _extract_info_from_html(html, ev)
                d['url_detalle'] = info_url
                d['timestamp_extraccion'] = time.strftime('%Y-%m-%d %H:%M:%S')
                detailed.append(d)
                slow_pause(0.6, 1.4)

            except Exception as e:
                log(f"[WARN] Falló {ev.get('event_url','<sin url>')}: {e}")
                detailed.append(ev)  # conserva base
                continue

    except Exception as e:
        log(f"ERROR general 02: {e}")
        _save_screenshot(driver, "02_error.png")
    finally:
        try: driver.quit()
        except Exception: pass
        log("Navegador cerrado.")

    # Siempre escribe salida (aunque venga sin enriquecer)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(detailed, f, ensure_ascii=False, indent=2)
    log(f"✅ Guardado {len(detailed)} items en {OUT_PATH}")

if __name__ == "__main__":
    main()
