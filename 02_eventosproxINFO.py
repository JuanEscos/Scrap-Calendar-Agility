# -*- coding: utf-8 -*-
"""
02_eventosproxINFO.py — robusto
- Enriquecer eventos (de 01) con datos de /info.
- NUNCA devuelve lista vacía si la entrada tiene elementos: ante fallos, vuelca la base.
- Credenciales por ENV: FLOW_EMAIL, FLOW_PASS (alias FLOW_USER_EMAIL, FLOW_USER_PASSWORD)
- Uso:
    python 02_eventosproxINFO.py [IN_EVENTS_JSON] [OUT_DETAILED_JSON]
    * IN por defecto: ./output/01events_last.json (o variantes)
    * OUT por defecto: ./output/02competiciones_detalladas.json
    * Copia canónica adicional: ./output/02info_last.json
"""

import os, sys, re, json, time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://www.flowagility.com"
OUT_DIR = "./output"
os.makedirs(OUT_DIR, exist_ok=True)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def slow(a=0.3, b=0.8):
    import random, time as _t
    _t.sleep(random.uniform(a, b))

# ---------- Selenium helpers ----------
def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
    from selenium.webdriver.chrome.service import Service
    return webdriver, By, Options, WebDriverWait, EC, TimeoutException, NoSuchElementException, WebDriverException, Service

def _get_driver(headless=True):
    webdriver, By, Options, *_ = _import_selenium()
    from selenium.webdriver.chrome.service import Service
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")
    # webdriver_manager fallback
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except Exception:
        return webdriver.Chrome(options=opts)

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
                driver.execute_script("arguments[0].click();", btns[0]); slow()
                return
        driver.execute_script("""
        const b=[...document.querySelectorAll('button')]
          .find(x=>/acept|accept|consent|de acuerdo/i.test((x.textContent||'')));
        if(b) b.click();
        """); slow()
    except Exception:
        pass

def _login(driver, By, WebDriverWait, EC, email, password):
    driver.get(f"{BASE}/user/login")
    _accept_cookies(driver, By)
    wait = WebDriverWait(driver, 25)
    email_el = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd_el   = driver.find_element(By.NAME, "user[password]")
    email_el.clear(); email_el.send_keys(email); slow()
    pwd_el.clear();    pwd_el.send_keys(password); slow()
    driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
    # éxito = URL cambia o aparece navbar ya logado
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow(0.6, 1.0)

# ---------- Info page parser ----------
def _parse_info_page(html):
    soup = BeautifulSoup(html, "html.parser")
    def txt(el): 
        return (el.get_text(" ", strip=True) if el else "").strip()
    data = {
        "informacion_general": {},
        "inscripcion": {},
        "pruebas": [],
        "contacto": {},
        "enlaces_adicionales": {}
    }

    # Título
    h = soup.find(["h1","h2"], string=True) or soup.find("h1")
    if h: data["informacion_general"]["titulo"] = txt(h)

    # bloques
    blocks = soup.find_all(["div","span","p","li"])
    for b in blocks:
        s = txt(b)
        if not s: 
            continue
        # Fechas comunes
        if " - " in s and re.search(r"\b(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", s, re.I):
            data["informacion_general"].setdefault("fechas_completas", s)
        if any(k in s.lower() for k in ["spain","españa","portugal","france","italy","germany","madrid","barcelona","valencia","lisboa","roma","paris"]):
            data["informacion_general"].setdefault("ubicacion_completa", s)
        if ("€" in s) or any(w in s.lower() for w in ["price","precio","eur","coste"]):
            data["inscripcion"].setdefault("precios", s)
        if any(w in s.lower() for w in ["inscrip", "registration"]):
            data["inscripcion"].setdefault("periodo_inscripcion", s)

    # Pruebas/categorías/horarios
    sections = soup.find_all(["section","div"], class_=lambda x: x and any(t in str(x).lower() for t in ["prueba","competition","event","round"]))
    for sec in sections:
        pr={}
        name = sec.find(["h2","h3","strong"])
        if name: pr["nombre"] = txt(name)
        texts = " ".join(txt(x) for x in sec.find_all(["div","li","p"]))
        if re.search(r"\b(hora|horario|schedule|time)\b", texts, re.I):
            pr["horarios"] = texts
        if re.search(r"\b(categor|nivel|level|class)\b", texts, re.I):
            pr["categorias"] = texts
        if pr: data["pruebas"].append(pr)

    # Contacto
    emails = sorted(set([t for t in soup.get_text(" ").split() if "@" in t and "." in t]))
    if emails: data["contacto"]["email"] = " | ".join(emails)

    for a in soup.find_all("a", href=True):
        href = a["href"].strip().lower()
        full = urljoin(BASE, a["href"])
        if any(k in href for k in ["reglamento","regulation","rules","normas"]):
            data["enlaces_adicionales"]["reglamento"] = full
        if any(k in href for k in ["map","ubicacion","location","google"]):
            data["enlaces_adicionales"]["mapa"] = full

    return data

# ---------- Main ----------
def main():
    FLOW_EMAIL = os.getenv("FLOW_EMAIL") or os.getenv("FLOW_USER_EMAIL")
    FLOW_PASS  = os.getenv("FLOW_PASS")  or os.getenv("FLOW_USER_PASSWORD")
    if not FLOW_EMAIL or not FLOW_PASS:
        print("[ERROR] Faltan FLOW_EMAIL/FLOW_PASS en el entorno."); sys.exit(2)
    HEADLESS = os.getenv("HEADLESS","true").lower() in ("1","true","yes","on")
    LIMIT_INFO = int(os.getenv("LIMIT_INFO","0") or "0")

    # Entradas/salidas
    def _first_existing(paths):
        for p in paths:
            if p and os.path.exists(p): 
                return p
        return None

    IN_PATH = sys.argv[1] if len(sys.argv) > 1 else _first_existing([
        os.path.join(OUT_DIR,"01events_last.json"),
        os.path.join(OUT_DIR,"01events.json"),
        "./01events_last.json", "./01events.json"
    ])
    OUT_PATH = sys.argv[2] if len(sys.argv) > 2 else os.path.join(OUT_DIR,"02competiciones_detalladas.json")
    OUT_PATH_LAST = os.path.join(OUT_DIR,"02info_last.json")

    if not IN_PATH:
        print("[ERROR] No se encontró el 01events JSON de entrada."); sys.exit(1)

    log(f"Entrada 01events: {IN_PATH}")
    try:
        base_events = json.load(open(IN_PATH, "r", encoding="utf-8"))
    except Exception as e:
        print(f"[ERROR] No se pudo leer {IN_PATH}: {e}"); sys.exit(1)

    if not isinstance(base_events, list):
        print("[ERROR] El archivo de entrada no es una lista JSON."); sys.exit(1)
    if len(base_events) == 0:
        print("[WARN] 01events está vacío; se volcará vacío (sin enriquecer).")
        json.dump(base_events, open(OUT_PATH,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
        json.dump(base_events, open(OUT_PATH_LAST,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
        print(f"OK -> {OUT_PATH} y {OUT_PATH_LAST} (vacío)"); return

    webdriver, By, Options, WebDriverWait, EC, TimeoutException, NoSuchElementException, WebDriverException, Service = _import_selenium()
    driver = _get_driver(headless=HEADLESS)

    detailed = []
    try:
        log("Login…")
        _login(driver, By, WebDriverWait, EC, FLOW_EMAIL, FLOW_PASS)
        log("Login OK.")

        iterable = base_events[:LIMIT_INFO] if (LIMIT_INFO and LIMIT_INFO > 0) else base_events
        for i, ev in enumerate(iterable, start=1):
            name = ev.get("nombre") or ev.get("title") or "Sin nombre"
            info_url = (ev.get("enlaces") or {}).get("info") or ""
            log(f"[{i}/{len(iterable)}] {name}")

            merged = dict(ev)
            if info_url:
                try:
                    driver.get(info_url); _accept_cookies(driver, By); slow(0.6, 1.0)
                    html = driver.page_source
                    extra = _parse_info_page(html)
                    merged.update({
                        "informacion_general": extra.get("informacion_general", {}),
                        "inscripcion": extra.get("inscripcion", {}),
                        "pruebas": extra.get("pruebas", []),
                        "contacto": extra.get("contacto", {}),
                        "enlaces_adicionales": extra.get("enlaces_adicionales", {}),
                        "url_detalle": info_url
                    })
                except Exception as e:
                    log(f"  - [WARN] Error leyendo info: {e}. Conservo base.")
            else:
                log("  - Sin enlace /info; conservo base.")
            detailed.append(merged)
            slow(0.4, 0.9)

    except Exception as e:
        log(f"[WARN] Fallo global durante scraping: {e}. Se devolverá base sin enriquecer.")
        detailed = base_events[:]  # Nunca vacío si base tenía contenido
    finally:
        try: driver.quit()
        except Exception: pass
        log("Navegador cerrado")

    # Guardar
    json.dump(detailed, open(OUT_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    json.dump(detailed, open(OUT_PATH_LAST, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    log(f"✅ Guardado: {OUT_PATH}")
    log(f"✅ Guardado: {OUT_PATH_LAST}")

    # Resumen
    comp_con_info = sum(1 for c in detailed if c.get("informacion_general"))
    comp_con_precios = sum(1 for c in detailed if (c.get("inscripcion") or {}).get("precios"))
    comp_con_pruebas = sum(1 for c in detailed if c.get("pruebas"))
    print("\n" + "="*70)
    print("RESUMEN 02:")
    print("="*70)
    print(f"Eventos base         : {len(base_events)}")
    print(f"Eventos enriquecidos : {len(detailed)}")
    print(f"Con info general     : {comp_con_info}")
    print(f"Con precios          : {comp_con_precios}")
    print(f"Con pruebas          : {comp_con_pruebas}")

    # Si por alguna razón quedó vacío, considerarlo fallo
    if len(detailed) == 0:
        print("[ERROR] La salida quedó vacía; esto no es esperado.")
        sys.exit(3)

if __name__ == "__main__":
    main()
