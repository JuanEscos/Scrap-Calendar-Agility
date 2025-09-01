# -*- coding: utf-8 -*-
"""
02_eventosproxINFO.py (adaptado para GitHub Actions)
- Evita chdir a Windows en Linux
- Credenciales vía variables de entorno
- HEADLESS configurable con env
- Soporta argumentos: [input_json] [output_json]
"""

import os, json, time, re, sys
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# === Paths ===
BASE = "https://www.flowagility.com"
OUT_DIR = "./output"
COMPETITIONS_FILE = os.path.join(OUT_DIR, 'competiciones_agility.json')
DETAILED_FILE = os.path.join(OUT_DIR, 'competiciones_detalladas.json')

os.makedirs(OUT_DIR, exist_ok=True)

# Evitar chdir roto en Linux
parent_dir = r"c:\Jescos 25.07.07\Agility\Pythonscrap\ListaEventos"
try:
    if os.name == "nt" and os.path.isdir(parent_dir):
        os.chdir(parent_dir)
except Exception:
    pass

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
def slow_pause(a=1, b=2):
    import random, time as _t
    _t.sleep(random.uniform(a, b))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, WebDriverException
    )
    from selenium.webdriver.chrome.service import Service
    return webdriver, By, Options, WebDriverWait, EC, TimeoutException, NoSuchElementException, WebDriverException, Service

def _get_driver(headless=True):
    webdriver, By, Options, *_rest = _import_selenium()
    from selenium.webdriver.chrome.service import Service
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except ImportError:
        log("webdriver_manager no instalado, usando ChromeDriver del sistema")
        return webdriver.Chrome(options=opts)

def _login(driver, By, WebDriverWait, EC):
    """Login con credenciales desde ENV (fallback: valores hardcodeados si existen)"""
    email_env = os.getenv("FLOW_USER_EMAIL") or "pilar1959suarez@gmail.com"
    pass_env  = os.getenv("FLOW_USER_PASSWORD") or "Seattle1"
    log("Iniciando login...")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(email_env); slow_pause(0.5, 1)
    pwd.clear();   pwd.send_keys(pass_env);    slow_pause(0.5, 1)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause(1.5, 2.5)
    log("Login exitoso")

def extract_detailed_info(driver, info_url, event_base_info):
    """Extrae información detallada desde la página /info del evento"""
    try:
        log(f"Accediendo a información detallada: {info_url}")
        driver.get(info_url)
        slow_pause(2, 3)
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')

        detailed_info = event_base_info.copy()

        # ===== INFO GENERAL =====
        general_info = {}
        title_elem = soup.find('h1', class_=lambda x: x and 'text' in x.lower()) or soup.find('h1')
        if title_elem:
            general_info['titulo'] = title_elem.get_text(strip=True)

        date_location_elems = soup.find_all('div', class_=lambda x: x and 'text' in x.lower())
        for elem in date_location_elems:
            text = elem.get_text(strip=True)
            if ' - ' in text and len(text) < 60:
                general_info['fechas_completas'] = text
            elif any(w in text.lower() for w in ['spain', 'españa', 'madrid', 'barcelona']):
                general_info['ubicacion_completa'] = text

        # ===== INSCRIPCIÓN =====
        registration_info = {}
        registration_dates = soup.find_all('div', class_=lambda x: x and any(w in str(x).lower() for w in ['date', 'fecha', 'inscrip']))
        for elem in registration_dates:
            text = elem.get_text(strip=True)
            if 'inscrip' in text.lower() or 'registration' in text.lower():
                registration_info['periodo_inscripcion'] = text

        price_elems = soup.find_all(lambda tag: tag.name in ['div', 'span'] and any(w in tag.get_text().lower() for w in ['€', 'euro', 'precio', 'price', 'coste']))
        for elem in price_elems:
            text = elem.get_text(strip=True)
            if '€' in text:
                registration_info['precios'] = text

        # ===== PRUEBAS =====
        pruebas_info = []
        prueba_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(w in str(x).lower() for w in ['prueba', 'competition', 'event', 'round']))
        for section in prueba_sections:
            prueba = {}
            name_elem = section.find(['h2', 'h3', 'h4', 'strong'])
            if name_elem:
                prueba['nombre'] = name_elem.get_text(strip=True)
            time_elems = section.find_all(lambda tag: any(w in tag.get_text().lower() for w in ['hora', 'time', 'horario', 'schedule']))
            for elem in time_elems:
                prueba['horarios'] = elem.get_text(strip=True)
            category_elems = section.find_all(lambda tag: any(w in tag.get_text().lower() for w in ['categor', 'level', 'nivel', 'class']))
            for elem in category_elems:
                prueba['categorias'] = elem.get_text(strip=True)
            if prueba:
                pruebas_info.append(prueba)

        # ===== CONTACTO =====
        contact_info = {}
        email_elems = soup.find_all(lambda tag: '@' in tag.get_text() and '.' in tag.get_text())
        for elem in email_elems:
            contact_info['email'] = elem.get_text(strip=True)
        phone_elems = soup.find_all(lambda tag: any(w in tag.get_text() for w in ['+34', 'tel:', 'phone', 'tlf']))
        for elem in phone_elems:
            contact_info['telefono'] = elem.get_text(strip=True)

        # ===== ENLACES ADICIONALES =====
        additional_links = {}
        reglamento_links = soup.find_all('a', href=lambda x: x and any(w in x.lower() for w in ['reglamento', 'regulation', 'normas', 'rules']))
        for link in reglamento_links:
            additional_links['reglamento'] = urljoin(BASE, link['href'])
        mapa_links = soup.find_all('a', href=lambda x: x and any(w in x.lower() for w in ['map', 'ubicacion', 'location', 'google']))
        for link in mapa_links:
            additional_links['mapa'] = urljoin(BASE, link['href'])

        detailed_info.update({
            'informacion_general': general_info,
            'inscripcion': registration_info,
            'pruebas': pruebas_info,
            'contacto': contact_info,
            'enlaces_adicionales': additional_links,
            'url_detalle': info_url,
            'timestamp_extraccion': time.strftime('%Y-%m-%d %H:%M:%S')
        })
        return detailed_info
    except Exception as e:
        log(f"Error extrayendo información de {info_url}: {str(e)}")
        return event_base_info

def main():
    log("=== EXTRACCIÓN DE INFORMACIÓN DETALLADA DE COMPETICIONES ===")

    # Permitir override por argumentos
    in_path  = sys.argv[1] if len(sys.argv) >= 2 else COMPETITIONS_FILE
    out_path = sys.argv[2] if len(sys.argv) >= 3 else DETAILED_FILE

    # Cargar competiciones
    if not os.path.exists(in_path):
        log(f"Error: No se encuentra el archivo {in_path}")
        sys.exit(1)

    with open(in_path, 'r', encoding='utf-8') as f:
        competiciones = json.load(f)

    log(f"Cargadas {len(competiciones)} competiciones desde {in_path}")

    (webdriver, By, Options, WebDriverWait, EC,
     TimeoutException, NoSuchElementException, WebDriverException, Service) = _import_selenium()

    headless = os.getenv("HEADLESS", "true").lower() == "true"
    driver = _get_driver(headless=headless)

    try:
        _login(driver, By, WebDriverWait, EC)

        competiciones_detalladas = []
        for i, competicion in enumerate(competiciones, 1):
            try:
                if 'enlaces' in competicion and 'info' in competicion['enlaces']:
                    info_url = competicion['enlaces']['info']
                    log(f"Procesando competición {i}/{len(competiciones)}: {competicion.get('nombre', 'Sin nombre')}")
                    competicion_detallada = extract_detailed_info(driver, info_url, competicion)
                    competiciones_detalladas.append(competicion_detallada)
                    slow_pause(1.5, 3.0)
                else:
                    log(f"Competición {i} sin enlace de información, saltando…")
                    competiciones_detalladas.append(competicion)
            except Exception as e:
                log(f"Error procesando competición {i}: {e}")
                competiciones_detalladas.append(competicion)
                continue

        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(competiciones_detalladas, f, ensure_ascii=False, indent=2)
        log(f"✅ Información detallada guardada en {out_path}")

        # Resumen
        comp_con_info = sum(1 for c in competiciones_detalladas if 'informacion_general' in c)
        comp_con_precios = sum(1 for c in competiciones_detalladas if 'inscripcion' in c and 'precios' in c['inscripcion'])
        comp_con_pruebas = sum(1 for c in competiciones_detalladas if 'pruebas' in c and c['pruebas'])
        print("\n" + "="*80)
        print("RESUMEN DE INFORMACIÓN EXTRAÍDA:")
        print("="*80)
        print(f"Competiciones procesadas: {len(competiciones_detalladas)}")
        print(f"Con información general: {comp_con_info}")
        print(f"Con precios: {comp_con_precios}")
        print(f"Con pruebas detalladas: {comp_con_pruebas}")

    finally:
        driver.quit()
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
