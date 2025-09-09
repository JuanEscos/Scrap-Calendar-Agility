# -*- coding: utf-8 -*-
"""
02_eventosproxINFO.py (robusto)
- Enriquecer eventos (de 01) con información de la página /info.
- Login opcional/tolerante: no falla en timeout; intenta continuar sin login.
- Si falta enlaces.info, lo deduce desde event_url o id (uuid).
- Salidas: ./output/02competiciones_detalladas.json y ./output/02info_last.json
Env:
  FLOW_EMAIL / FLOW_USER_EMAIL
  FLOW_PASS  / FLOW_USER_PASSWORD
  HEADLESS=true|false (default true)
  LIMIT_INFO=0 (0 = sin límite)
CLI:
  python 02_eventosproxINFO.py [IN_EVENTS_JSON] [OUT_DETAILED_JSON]
"""

import os, re, sys, json, time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://www.flowagility.com"
OUT_DIR = "./output"
os.makedirs(OUT_DIR, exist_ok=True)

# ---- Credenciales (opcionales) ----
FLOW_EMAIL = os.getenv("FLOW_EMAIL") or os.getenv("FLOW_USER_EMAIL")
FLOW_PASS  = os.getenv("FLOW_PASS")  or os.getenv("FLOW_USER_PASSWORD")

HEADLESS = os.getenv("HEADLESS", "true").strip().lower() in ("1","true","yes","on")
LIMIT_INFO = int(os.getenv("LIMIT_INFO", "0") or "0")

def _first_existing(paths):
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None

IN_PATH  = sys.argv[1] if len(sys.argv) > 1 else _first_existing([
    os.path.join(OUT_DIR, "01events_last.json"),
    os.path.join(OUT_DIR, "01events.json"),
    "./01events.json",
])
OUT_PATH = sys.argv[2] if len(sys.argv) > 2 else os.path.join(OUT_DIR, "02competiciones_detalladas.json")
OUT_PATH_LAST = os.path.join(OUT_DIR, "02info_last.json")

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def slow(a=0.6, b=1.4):
    import random, time as _t
    _t.sleep(random.uniform(a, b))

# ---------------- Selenium helpers ----------------
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
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except Exception:
        return webdriver.Chrome(options=opts)

def _screenshot(driver, name):
    try:
        driver.save_screenshot(os.path.join(OUT_DIR, name))
    except Exception:
        pass

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
                driver.execute_script("arguments[0].click();", btns[0]); slow(0.2,0.5); return
        driver.execute_script("""
            const b=[...document.querySelectorAll('button')]
              .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent||''));
            if(b) b.click();
        """)
        slow(0.2,0.4)
    except Exception:
        pass

def _is_login_url(url: str) -> bool:
    return "/user/login" in (url or "")

def _looks_like_login_page(driver, By) -> bool:
    try:
        return bool(driver.find_elements(By.NAME, "user[email]"))
    except Exception:
        return False

def _login_if_needed(driver, By, WebDriverWait, EC) -> bool:
    """Intenta loguearse si hay credenciales; NO lanza excepciones en timeout."""
    if not (FLOW_EMAIL and FLOW_PASS):
        return False
    try:
        # Si ya no estamos en login, no hacemos nada
        if not _is_login_url(driver.current_url) and not _looks_like_login_page(driver, By):
            return False

        log("Intentando login…")
        wait = WebDriverWait(driver, 20)
        # A veces hay diálogo de cookies en login:
        _accept_cookies(driver, By)
        # Asegura estar en la URL de login
        if not _is_login_url(driver.current_url):
            driver.get(f"{BASE}/user/login")
            slow(0.5, 1.0)
            _accept_cookies(driver, By)

        email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
        pwd   = driver.find_element(By.NAME, "user[password]")
        email.clear(); email.send_keys(FLOW_EMAIL); slow(0.2,0.5)
        pwd.clear();   pwd.send_keys(FLOW_PASS);    slow(0.2,0.5)
        driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()

        # Espera NO estricta: si en 25s seguimos en login, no error
        t0 = time.time()
        while time.time() - t0 < 25:
            if not _is_login_url(driver.current_url):
                log("Login OK.")
                return True
            slow(0.3,0.6)

        log("Login timeout, sigo sin romper (modo tolerante).")
        _screenshot(driver, "02_login_timeout.png")
        return False
    except Exception as e:
        log(f"[WARN] Login falló: {e}. Continúo en modo anónimo.")
        _screenshot(driver, "02_login_error.png")
        return False

# -------------- Parse /info --------------
def _extract_info_from_info_page(driver, info_url):
    data = {
        "informacion_general": {},
        "inscripcion": {},
        "pruebas": [],
        "contacto": {},
        "enlaces_adicionales": {},
        "url_detalle": info_url,
    }
    driver.get(info_url); slow(0.8,1.4)
    _accept_cookies(driver, By)  # importado más abajo al llamar
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    def txt(el): return (el.get_text(" ", strip=True) if el else "").strip()

    # Título
    h = soup.find(["h1","h2"], string=True) or soup.find("h1")
    if h: data["informacion_general"]["titulo"] = txt(h)

    # Bloques para fechas/ubicación/precios
    blocks = soup.find_all(["div","span","p","li","section"])
    for b in blocks:
        s = txt(b)
        if not s: continue
        low = s.lower()
        # Fechas formateadas tipo "Sep 5 - 7"
        if " - " in s and re.search(r"\b(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", s, re.I):
            data["informacion_general"].setdefault("fechas_completas", s)
        # Ubicación
        if any(k in low for k in ["spain","españa","portugal","france","italy","madrid","barcelona","valencia","sevilla","lisboa","roma","paris"]):
            data["informacion_general"].setdefault("ubicacion_completa", s)
        # Inscripción / precios
        if "inscrip" in low or "registration" in low:
            data["inscripcion"].setdefault("periodo_inscripcion", s)
        if "€" in s or " eur" in low or "precio" in low or "price" in low or "coste" in low:
            data["inscripcion"].setdefault("precios", s)

    # Secciones de pruebas
    secs = soup.find_all(["section","div"], class_=lambda x: x and any(t in str(x).lower() for t in ["prueba","competition","event","round"]))
    for sec in secs:
        pr = {}
        name = sec.find(["h2","h3","strong"])
        if name: pr["nombre"] = txt(name)
        # horarios
        sched = []
        for el in sec.find_all(["div","p","li","span"]):
            s = txt(el)
            if re.search(r"\b(hora|horario|schedule|time)\b", s, re.I):
                sched.append(s)
        if sched: pr["horarios"] = " | ".join(sorted(set(sched)))
        # categorías
        cats = []
        for el in sec.find_all(["div","p","li","span"]):
            s = txt(el)
            if re.search(r"\b(categor|nivel|level|class)\b", s, re.I):
                cats.append(s)
        if cats: pr["categorias"] = " | ".join(sorted(set(cats)))
        if pr: data["pruebas"].append(pr)

    # Contacto
    emails=set(); phones=set()
    for t in soup.find_all(string=re.compile(r"@")):
        s = str(t)
        if "@" in s and "." in s:
            emails.add(s.strip())
    for t in soup.find_all(string=True):
        s = str(t)
        if re.search(r"(\+?\d[\d\s\-()]{6,})", s) and any(tok in s.lower() for tok in ["tel", "phone", "+34"]):
            phones.add(s.strip())
    if emails: data["contacto"]["email"] = " | ".join(sorted(emails))
    if phones: data["contacto"]["telefono"] = " | ".join(sorted(phones))

    # Enlaces adicionales
    for a in soup.find_all("a", href=True):
        href = a["href"].strip().lower()
        if any(k in href for k in ["reglamento","regulation","rules","normas"]):
            data["enlaces_adicionales"]["reglamento"] = urljoin(BASE, a["href"])
        if any(k in href for k in ["map","ubicacion","location","google"]):
            data["enlaces_adicionales"]["mapa"] = urljoin(BASE, a["href"])

    return data

# -------------- Main --------------
def main():
    if not IN_PATH:
        log("ERROR: No se encontró el JSON de entrada del 01.")
        sys.exit(1)

    try:
        with open(IN_PATH, "r", encoding="utf-8") as f:
            base_events = json.load(f)
    except Exception as e:
        log(f"ERROR leyendo {IN_PATH}: {e}")
        sys.exit(1)

    if not isinstance(base_events, list) or not base_events:
        log("WARN: Entrada vacía o no lista; vuelco tal cual a salida.")
        for p in (OUT_PATH, OUT_PATH_LAST):
            with open(p, "w", encoding="utf-8") as f: json.dump(base_events, f, ensure_ascii=False, indent=2)
        return

    webdriver, By, Options, WebDriverWait, EC, TimeoutException, NoSuchElementException, WebDriverException, Service = _import_selenium()
    driver = _get_driver(headless=HEADLESS)

    try:
        # Intento de login (no obligatorio)
        # Entramos en /user/login sólo si queremos forzar cookie de sesión; si falla, continuamos.
        driver.get(BASE + "/user/login"); slow(0.6,1.0)
        _accept_cookies(driver, By)
        _login_if_needed(driver, By, WebDriverWait, EC)

        # Iteración
        detailed = []
        iterable = base_events[:LIMIT_INFO] if (LIMIT_INFO and LIMIT_INFO > 0) else base_events

        for i, ev in enumerate(iterable, 1):
            nombre = ev.get("nombre") or ev.get("title") or "Sin nombre"
            info_url = (ev.get("enlaces") or {}).get("info") or ""

            # Deducción de info_url si falta
            if not info_url:
                uuid = None
                # intenta desde event_url
                ev_url = ev.get("event_url") or (ev.get("enlaces") or {}).get("evento") or ""
                m = re.search(r"/zone/events/([0-9a-fA-F-]{36})", ev_url or "")
                if m: uuid = m.group(1)
                # intenta desde id
                if not uuid:
                    cand = ev.get("id") or ""
                    if re.fullmatch(r"[0-9a-fA-F-]{36}", str(cand)): uuid = cand
                if uuid:
                    info_url = f"{BASE}/zone/events/{uuid}/info"

            merged = dict(ev)
            log(f"[{i}/{len(iterable)}] {nombre}  |  info={info_url or 'N/D'}")

            try:
                if info_url:
                    driver.get(info_url); slow(0.6,1.0)
                    _accept_cookies(driver, By)
                    # Si nos ha llevado al login, intenta loguear y vuelve
                    if _is_login_url(driver.current_url) or _looks_like_login_page(driver, By):
                        _login_if_needed(driver, By, WebDriverWait, EC)
                        driver.get(info_url); slow(0.6,1.0)
                        _accept_cookies(driver, By)

                    data = _extract_info_from_info_page(driver, info_url)
                    merged["informacion_general"]   = data.get("informacion_general", {})
                    merged["inscripcion"]           = data.get("inscripcion", {})
                    merged["pruebas"]               = data.get("pruebas", [])
                    merged["contacto"]              = data.get("contacto", {})
                    merged["enlaces_adicionales"]   = data.get("enlaces_adicionales", {})
                    merged["url_detalle"]           = data.get("url_detalle", info_url)
                else:
                    log("  - Sin enlace /info (ni deducible). Se deja base intacta.")
            except Exception as e:
                log(f"  - [WARN] Error extrayendo info: {e}. Dejo base intacta.")
                _screenshot(driver, f"02_info_error_{i}.png")

            detailed.append(merged)
            slow()

        if LIMIT_INFO and LIMIT_INFO > 0 and len(base_events) > len(iterable):
            detailed.extend(base_events[len(iterable):])

        for p in (OUT_PATH, OUT_PATH_LAST):
            with open(p, "w", encoding="utf-8") as f:
                json.dump(detailed, f, ensure_ascii=False, indent=2)
        log(f"✅ Guardado: {OUT_PATH}")
        log(f"✅ Guardado: {OUT_PATH_LAST}")

        comp_con_info = sum(1 for c in detailed if c.get("informacion_general"))
        comp_con_precios = sum(1 for c in detailed if c.get("inscripcion", {}).get("precios"))
        comp_con_pruebas = sum(1 for c in detailed if c.get("pruebas"))
        print("\n" + "="*80)
        print("RESUMEN 02:")
        print("="*80)
        print(f"Eventos base         : {len(base_events)}")
        print(f"Eventos enriquecidos : {len(detailed)}")
        print(f"Con info general     : {comp_con_info}")
        print(f"Con precios          : {comp_con_precios}")
        print(f"Con pruebas          : {comp_con_pruebas}")

    finally:
        try: driver.quit()
        except Exception: pass
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
