# -*- coding: utf-8 -*-
"""
02_eventosproxINFO.py (adaptado para GitHub Actions)
- No chdir a rutas Windows
- Credenciales vía variables de entorno (FLOW_EMAIL/FLOW_PASS o FLOW_USER_EMAIL/FLOW_USER_PASSWORD)
- HEADLESS configurable con env
- Argumentos: [input_json] [output_json]
"""

import os, json, time, re, sys, random
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://www.flowagility.com"
OUT_DIR = "./output"
COMPETITIONS_FILE = os.path.join(OUT_DIR, 'competiciones_agility.json')
DETAILED_FILE     = os.path.join(OUT_DIR, 'competiciones_detalladas.json')
os.makedirs(OUT_DIR, exist_ok=True)

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
def slow_pause(a=1.0, b=2.0): time.sleep(random.uniform(a, b))

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
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except ImportError:
        log("webdriver_manager no instalado; usando chromedriver del sistema")
        return webdriver.Chrome(options=opts)

def _accept_cookies(driver, By):
    try:
        for sel in ('[data-testid="uc-accept-all-button"]','button[aria-label="Accept all"]','button[aria-label="Aceptar todo"]','button[mode="primary"]'):
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            if btns:
                driver.execute_script("arguments[0].click();", btns[0])
                slow_pause(0.8, 1.6)
                return
        driver.execute_script("""
          const b=[...document.querySelectorAll('button')].find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
          if(b) b.click();
        """)
        slow_pause(0.2, 0.5)
    except Exception:
        pass

def _login(driver, By, WebDriverWait, EC):
    # admite dos juegos de nombres por si en el repo existen distintos secrets
    email = os.getenv("FLOW_EMAIL") or os.getenv("FLOW_USER_EMAIL") or ""
    pwd   = os.getenv("FLOW_PASS")  or os.getenv("FLOW_USER_PASSWORD") or ""
    if not email or not pwd:
        raise RuntimeError("Faltan credenciales (FLOW_EMAIL/FLOW_PASS o FLOW_USER_EMAIL/FLOW_USER_PASSWORD).")
    log("Iniciando login…")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email_el = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd_el   = driver.find_element(By.NAME, "user[password]")
    email_el.clear(); email_el.send_keys(email); slow_pause(0.3, 0.6)
    pwd_el.clear();   pwd_el.send_keys(pwd);     slow_pause(0.3, 0.6)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause(1.0, 1.8)
    log("Login OK")

def extract_detailed_info(driver, info_url, event_base_info):
    """Extrae información detallada desde /info del evento"""
    try:
        log(f"Info: {info_url}")
        driver.get(info_url)
        _accept_cookies(driver, driver.__class__.By if hasattr(driver.__class__, 'By') else __import__('selenium').webdriver.common.by.By)  # no rompe si ya aceptaste
        slow_pause(1.5, 2.5)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        detailed = event_base_info.copy()

        # ---- INFO GENERAL ----
        general = {}
        title_elem = soup.find('h1') or soup.find(['h2','h3'])
        if title_elem: general['titulo'] = title_elem.get_text(strip=True)

        for elem in soup.find_all('div', class_=lambda x: x and 'text' in x.lower()):
            text = elem.get_text(strip=True)
            if ' - ' in text and len(text) < 60:
                general['fechas_completas'] = text
            elif any(w in text.lower() for w in ['spain','españa','madrid','barcelona']):
                general['ubicacion_completa'] = text

        # ---- INSCRIPCION ----
        insc = {}
        for elem in soup.find_all('div', class_=lambda x: x and any(w in str(x).lower() for w in ['date','fecha','inscrip'])):
            t = elem.get_text(strip=True)
            if 'inscrip' in t.lower() or 'registration' in t.lower():
                insc['periodo_inscripcion'] = t
        for elem in soup.find_all(lambda tag: tag.name in ['div','span'] and any(w in tag.get_text().lower() for w in ['€','euro','precio','price','coste'])):
            t = elem.get_text(strip=True)
            if '€' in t: insc['precios'] = t

        # ---- PRUEBAS ----
        pruebas = []
        sections = soup.find_all(['div','section'], class_=lambda x: x and any(w in str(x).lower() for w in ['prueba','competition','event','round']))
        for s in sections:
            item = {}
            t = s.find(['h2','h3','h4','strong'])
            if t: item['nombre'] = t.get_text(strip=True)
            for e in s.find_all(lambda tag: any(w in tag.get_text().lower() for w in ['hora','time','horario','schedule'])):
                item['horarios'] = e.get_text(strip=True)
            for e in s.find_all(lambda tag: any(w in tag.get_text().lower() for w in ['categor','level','nivel','class'])):
                item['categorias'] = e.get_text(strip=True)
            if item: pruebas.append(item)

        # ---- CONTACTO ----
        contacto = {}
        for e in soup.find_all(lambda tag: '@' in tag.get_text() and '.' in tag.get_text()):
            contacto['email'] = e.get_text(strip=True)
        for e in soup.find_all(lambda tag: any(w in tag.get_text() for w in ['+34','tel:','phone','tlf'])):
            contacto['telefono'] = e.get_text(strip=True)

        # ---- ENLACES ADICIONALES ----
        extra = {}
        for a in soup.find_all('a', href=lambda x: x and any(w in x.lower() for w in ['reglamento','regulation','normas','rules'])):
            extra['reglamento'] = urljoin(BASE, a['href'])
        for a in soup.find_all('a', href=lambda x: x and any(w in x.lower() for w in ['map','ubicacion','location','google'])):
            extra['mapa'] = urljoin(BASE, a['href'])

        detailed.update({
            'informacion_general': general,
            'inscripcion': insc,
            'pruebas': pruebas,
            'contacto': contacto,
            'enlaces_adicionales': extra,
            'url_detalle': info_url,
            'timestamp_extraccion': time.strftime('%Y-%m-%d %H:%M:%S')
        })
        return detailed
    except Exception as e:
        log(f"Error en {info_url}: {e}")
        return event_base_info

def main():
    log("=== EXTRACCIÓN DE INFORMACIÓN DETALLADA DE COMPETICIONES ===")
    in_path  = sys.argv[1] if len(sys.argv) >= 2 else COMPETITIONS_FILE
    out_path = sys.argv[2] if len(sys.argv) >= 3 else DETAILED_FILE

    if not os.path.exists(in_path):
        log(f"Error: No existe {in_path}")
        sys.exit(1)

    with open(in_path, 'r', encoding='utf-8') as f:
        comps = json.load(f)

    log(f"Cargadas {len(comps)} competiciones desde {in_path}")

    webdriver, By, Options, WebDriverWait, EC, *_ = _import_selenium()
    headless = os.getenv("HEADLESS", "true").lower() == "true"
    driver = _get_driver(headless=headless)

    try:
        _login(driver, By, WebDriverWait, EC)

        out = []
        for i, c in enumerate(comps, 1):
            try:
                info_url = c.get('enlaces', {}).get('info', '')
                if info_url:
                    log(f"[{i}/{len(comps)}] {c.get('nombre','(sin nombre)')}")
                    out.append(extract_detailed_info(driver, info_url, c))
                    slow_pause(1.2, 2.4)
                else:
                    log(f"[{i}] Sin enlace de info → se conserva base")
                    out.append(c)
            except Exception as e:
                log(f"Error procesando {i}: {e}"); out.append(c)

        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        log(f"✅ Guardado: {out_path}")

        comp_con_info   = sum(1 for c in out if 'informacion_general' in c)
        comp_con_precios= sum(1 for c in out if 'inscripcion' in c and 'precios' in c['inscripcion'])
        comp_con_pruebas= sum(1 for c in out if 'pruebas' in c and c['pruebas'])
        print("\n" + "="*80)
        print("RESUMEN DE INFORMACIÓN EXTRAÍDA:")
        print("="*80)
        print(f"Competiciones procesadas: {len(out)}")
        print(f"Con información general: {comp_con_info}")
        print(f"Con precios: {comp_con_precios}")
        print(f"Con pruebas detalladas: {comp_con_pruebas}")
    finally:
        try: driver.quit()
        except Exception: pass
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
