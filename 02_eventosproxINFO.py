# -*- coding: utf-8 -*-
"""
02_eventosproxINFO.py
- Enriquecer eventos (de 01) con información detallada (página /info).
- Credenciales por ENV: FLOW_EMAIL, FLOW_PASS
- Flags por ENV:
    HEADLESS=true|false (default: true)
    LIMIT_INFO=0 (0 = sin límite; >0 limita eventos a procesar)
- Entradas/salidas:
    python 02_eventosproxINFO.py [IN_EVENTS_JSON] [OUT_DETAILED_JSON]
    * Si no se pasan argumentos, el script busca IN en:
        ./output/01events.json
        ./output/01events_last.json
        ./artifacts/01events.json
        ./01events.json
    * OUT por defecto: ./output/02competiciones_detalladas.json
    * También genera/actualiza: ./output/02info_last.json
"""

import os
import re
import sys
import json
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# ========================= Config =========================
BASE = "https://www.flowagility.com"
OUT_DIR = "./output"
os.makedirs(OUT_DIR, exist_ok=True)

FLOW_EMAIL = os.getenv("FLOW_EMAIL") or os.getenv("FLOW_USER_EMAIL")
FLOW_PASS  = os.getenv("FLOW_PASS")  or os.getenv("FLOW_USER_PASSWORD")
if not FLOW_EMAIL or not FLOW_PASS:
    print("[ERROR] Faltan variables de entorno FLOW_EMAIL/FLOW_PASS")
    sys.exit(2)

HEADLESS = (os.getenv("HEADLESS", "true").strip().lower() in ("1","true","yes","on"))
LIMIT_INFO = int(os.getenv("LIMIT_INFO", "0"))  # 0 = sin límite

# === Resuelve rutas de entrada/salida ===
def _first_existing(paths):
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None

IN_PATH  = sys.argv[1] if len(sys.argv) > 1 else _first_existing([
    os.path.join(OUT_DIR, "01events.json"),
    os.path.join(OUT_DIR, "01events_last.json"),
    os.path.join("./artifacts", "01events.json"),
    "./01events.json",
])
OUT_PATH = sys.argv[2] if len(sys.argv) > 2 else os.path.join(OUT_DIR, "02competiciones_detalladas.json")
OUT_PATH_LAST = os.path.join(OUT_DIR, "02info_last.json")  # copia canónica para FTP

# ========================= Utils =========================
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def slow_pause(a=0.8, b=1.8):
    import random, time as _t
    _t.sleep(random.uniform(a, b))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, WebDriverException,
        StaleElementReferenceException, JavascriptException, ElementClickInterceptedException
    )
    from selenium.webdriver.chrome.service import Service
    return (webdriver, By, Options, WebDriverWait, EC,
            TimeoutException, NoSuchElementException, WebDriverException,
            StaleElementReferenceException, JavascriptException, ElementClickInterceptedException, Service)

def _get_driver(headless=True):
    (webdriver, By, Options, *_rest) = _import_selenium()
    from selenium.webdriver.chrome.service import Service
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
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

def _accept_cookies(driver, By):
    try:
        sels = [
            '[data-testid="uc-accept-all-button"]',
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            'button[mode="primary"]',
        ]
        for sel in sels:
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            if btns:
                driver.execute_script("arguments[0].click();", btns[0])
                slow_pause(0.3, 0.7)
                return
        # intento genérico
        driver.execute_script("""
          const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent||''));
          if(b) b.click();
        """)
        slow_pause(0.2, 0.4)
    except Exception:
        pass

def _login(driver, By, WebDriverWait, EC):
    log("Login…")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    # <-- FIX: sin paréntesis extra
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL); slow_pause(0.2, 0.6)
    pwd.clear();   pwd.send_keys(FLOW_PASS);    slow_pause(0.2, 0.6)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause(0.8, 1.2)
    log("Login OK.")

def _extract_info_from_info_page(driver, info_url):
    """
    Extrae información útil de la página /info de un evento.
    Devuelve un dict con secciones 'informacion_general', 'inscripcion', 'pruebas', 'contacto', 'enlaces_adicionales'.
    """
    data = {
        "informacion_general": {},
        "inscripcion": {},
        "pruebas": [],
        "contacto": {},
        "enlaces_adicionales": {},
        "url_detalle": info_url,
    }
    driver.get(info_url)
    slow_pause(1.2, 2.0)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    def txt(el):
        if not el: return ""
        return (el.get_text(separator=" ", strip=True) or "").strip()

    # ------ Información general ------
    title = soup.find(["h1","h2"], string=True) or soup.find("h1")
    if title:
        data["informacion_general"]["titulo"] = txt(title)

    blocks = soup.find_all(["div","span","p"])
    for b in blocks:
        s = txt(b)
        if not s:
            continue
        if " - " in s and len(s) < 80 and re.search(r"\b(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", s, re.I):
            data["informacion_general"].setdefault("fechas_completas", s)
        if any(k in s.lower() for k in ["spain","españa","france","italy","portugal","madrid","barcelona","valencia","sevilla","lisboa","roma","paris"]):
            data["informacion_general"].setdefault("ubicacion_completa", s)

    # ------ Inscripción ------
    for b in blocks:
        s = txt(b).lower()
        if not s: continue
        if "inscrip" in s or "registration" in s:
            data["inscripcion"].setdefault("periodo_inscripcion", b.get_text(" ", strip=True))
        if ("€" in s) or ("eur" in s) or ("precio" in s) or ("price" in s) or ("coste" in s):
            if "precios" not in data["inscripcion"]:
                data["inscripcion"]["precios"] = b.get_text(" ", strip=True)

    # ------ Pruebas (secciones) ------
    sections = soup.find_all(["section","div"], class_=lambda x: x and any(t in str(x).lower() for t in ["prueba","competition","event","round"]))
    for sec in sections:
        pr = {}
        name = sec.find(["h2","h3","strong"])
        if name:
            pr["nombre"] = txt(name)
        # horarios / schedule
        sched = []
        for el in sec.find_all(["div","p","li"]):
            s = txt(el)
            if re.search(r"\b(hora|horario|schedule|time)\b", s, re.I):
                sched.append(s)
        if sched:
            pr["horarios"] = " | ".join(sorted(set(sched)))
        # categorías / niveles
        cats = []
        for el in sec.find_all(["div","p","li"]):
            s = txt(el)
            if re.search(r"\b(categor|nivel|level|class)\b", s, re.I):
                cats.append(s)
        if cats:
            pr["categorias"] = " | ".join(sorted(set(cats)))
        if pr:
            data["pruebas"].append(pr)

    # ------ Contacto ------
    emails = set()
    phones = set()
    for a in soup.find_all(text=re.compile(r"@")):
        s = str(a).strip()
        if "@" in s and "." in s:
            emails.add(s)
    for el in soup.find_all(text=True):
        s = str(el)
        if re.search(r"(\+?\d[\d\s\-()]{6,})", s) and any(tok in s.lower() for tok in ["tel", "phone", "+34"]):
            phones.add(s.strip())
    if emails:
        data["contacto"]["email"] = " | ".join(sorted(emails))
    if phones:
        data["contacto"]["telefono"] = " | ".join(sorted(phones))

    # ------ Enlaces adicionales ------
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        low = href.lower()
        if any(k in low for k in ["reglamento","regulation","rules","normas"]):
            data["enlaces_adicionales"]["reglamento"] = urljoin(BASE, href)
        if any(k in low for k in ["map","ubicacion","location","google"]):
            data["enlaces_adicionales"]["mapa"] = urljoin(BASE, href)

    return data

# ========================= Main =========================
def main():
    if not IN_PATH:
        print(f"[ERROR] No se encontró archivo de entrada 01events (prueba a pasar ruta por argumento).")
        sys.exit(1)

    log(f"Entrada 01events: {IN_PATH}")
    try:
        with open(IN_PATH, "r", encoding="utf-8") as f:
            base_events = json.load(f)
    except Exception as e:
        print(f"[ERROR] No se pudo leer/parsear {IN_PATH}: {e}")
        sys.exit(1)

    if not isinstance(base_events, list) or not base_events:
        print("[WARN] El 01events está vacío o no es una lista. Se volcará tal cual a OUT (sin enriquecer).")
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(base_events, f, ensure_ascii=False, indent=2)
        with open(OUT_PATH_LAST, "w", encoding="utf-8") as f:
            json.dump(base_events, f, ensure_ascii=False, indent=2)
        print(f"OK -> {OUT_PATH} y {OUT_PATH_LAST}")
        return

    (webdriver, By, Options, WebDriverWait, EC,
     TimeoutException, NoSuchElementException, WebDriverException,
     StaleElementReferenceException, JavascriptException, ElementClickInterceptedException, Service) = _import_selenium()

    driver = _get_driver(headless=HEADLESS)
    try:
        _login(driver, By, WebDriverWait, EC)
        slow_pause(0.5, 1.0)

        detailed = []
        iterable = base_events[:LIMIT_INFO] if (LIMIT_INFO and LIMIT_INFO > 0) else base_events

        for i, ev in enumerate(iterable, 1):
            nombre = ev.get("nombre") or ev.get("title") or "Sin nombre"
            enlaces = ev.get("enlaces") or {}
            info_url = enlaces.get("info") or ""
            log(f"[{i}/{len(iterable)}] {nombre}")

            merged = dict(ev)
            try:
                if info_url:
                    data = _extract_info_from_info_page(driver, info_url)
                    merged["informacion_general"] = data.get("informacion_general", {})
                    merged["inscripcion"] = data.get("inscripcion", {})
                    merged["pruebas"] = data.get("pruebas", [])
                    merged["contacto"] = data.get("contacto", {})
                    merged["enlaces_adicionales"] = data.get("enlaces_adicionales", {})
                    merged["url_detalle"] = data.get("url_detalle", info_url)
                else:
                    log("  - Sin enlace de información; se deja base intacta.")
            except Exception as e:
                log(f"  - [WARN] Error extrayendo info: {e}. Dejando base intacta.")

            detailed.append(merged)
            slow_pause(0.8, 1.6)

        if LIMIT_INFO and LIMIT_INFO > 0 and len(base_events) > len(iterable):
            detailed.extend(base_events[len(iterable):])

        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(detailed, f, ensure_ascii=False, indent=2)
        with open(OUT_PATH_LAST, "w", encoding="utf-8") as f:
            json.dump(detailed, f, ensure_ascii=False, indent=2)

        log(f"✅ Guardado: {OUT_PATH}")
        log(f"✅ Guardado: {OUT_PATH_LAST}")

        comp_con_info = sum(1 for c in detailed if c.get("informacion_general"))
        comp_con_precios = sum(1 for c in detailed if c.get("inscripcion", {}).get("precios"))
        comp_con_pruebas = sum(1 for c in detailed if c.get("pruebas"))
        print("\n" + "="*80)
        print("RESUMEN DE INFORMACIÓN EXTRAÍDA (02):")
        print("="*80)
        print(f"Eventos base            : {len(base_events)}")
        print(f"Eventos enriquecidos    : {len(detailed)}")
        print(f"Con información general : {comp_con_info}")
        print(f"Con precios             : {comp_con_precios}")
        print(f"Con pruebas detalladas  : {comp_con_pruebas}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
