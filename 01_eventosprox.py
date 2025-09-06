#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, time, json, sys
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"
OUT_DIR = "./output"
OUT_FILE = os.path.join(OUT_DIR, "events.json")

FLOW_EMAIL = os.getenv("FLOW_EMAIL", "").strip()
FLOW_PASS  = os.getenv("FLOW_PASS", "").strip()
HEADLESS   = (os.getenv("HEADLESS", "true").lower() in ("1","true","yes","on"))
INCOGNITO  = (os.getenv("INCOGNITO", "true").lower() in ("1","true","yes","on"))
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "10"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "1.5"))

UUID_RE = re.compile(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})")

os.makedirs(OUT_DIR, exist_ok=True)

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)
def slow_pause(a=0.5, b=1.2): time.sleep(max(a, b))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import JavascriptException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException
    from selenium.webdriver.chrome.service import Service
    return webdriver, By, Options, WebDriverWait, EC, JavascriptException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException, Service

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
                btns[0].click(); slow_pause()
                return
        driver.execute_script("""
            const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
            if(b) b.click();
        """)
        slow_pause()
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
    slow_pause()
    log("Login OK.")

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
    soup = BeautifulSoup(container_html, 'html.parser')
    event_data = {}
    event_container = soup.find('div', class_='group mb-6')
    if event_container:
        event_data['id'] = event_container.get('id', '')
    info_div = soup.find('div', class_='relative flex flex-col w-full pt-1 pb-6 mb-4 border-b border-gray-300')
    if info_div:
        date_elems = info_div.find_all('div', class_='text-xs')
        if date_elems: event_data['fechas'] = date_elems[0].get_text(strip=True)
        if len(date_elems)>1: event_data['organizacion'] = date_elems[1].get_text(strip=True)
        name_elem = info_div.find('div', class_='font-caption text-lg text-black truncate -mt-1')
        if name_elem: event_data['nombre'] = name_elem.get_text(strip=True)
        club_elem = info_div.find('div', class_='text-xs mb-0.5 mt-0.5')
        if club_elem: event_data['club'] = club_elem.get_text(strip=True)
        for div in info_div.find_all('div', class_='text-xs'):
            t = div.get_text(strip=True)
            if '/' in t and ('Spain' in t or 'España' in t):
                event_data['lugar'] = t; break
    status_button = soup.find('div', class_='py-1 px-4 border text-white font-bold rounded text-sm')
    if status_button:
        event_data['estado'] = status_button.get_text(strip=True)
    event_data['enlaces'] = {}
    info_link = soup.find('a', href=lambda x: x and '/info/' in x)
    if info_link: event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
    participants_link = soup.find('a', href=lambda x: x and '/participants_list' in x)
    if participants_link: event_data['enlaces']['participantes'] = urljoin(BASE, participants_link['href'])
    runs_link = soup.find('a', href=lambda x: x and '/runs' in x)
    if runs_link: event_data['enlaces']['runs'] = urljoin(BASE, runs_link['href'])
    return event_data

def main():
    events = []
    wrote = False
    try:
        (webdriver, By, Options, WebDriverWait, EC,
         JavascriptException, StaleElementReferenceException,
         NoSuchElementException, ElementClickInterceptedException, TimeoutException, Service) = _import_selenium()

        driver = _get_driver()
        try:
            _login(driver, By, WebDriverWait, EC)
            log("Abriendo listado…")
            driver.get(EVENTS_URL)
            WebDriverWait(driver, 25).until(lambda d: d.find_element(By.TAG_NAME, "body"))
            _accept_cookies(driver, By)
            _full_scroll(driver)
            page_html = driver.page_source

            soup = BeautifulSoup(page_html, 'html.parser')
            containers = soup.find_all('div', class_='group mb-6')
            log(f"Contenedores de eventos encontrados: {len(containers)}")

            for i, container in enumerate(containers, 1):
                try:
                    ev = extract_event_details(str(container))
                    # intenta sacar uuid si se ve
                    h = container.find('a', href=True)
                    if h and h['href']:
                        m = UUID_RE.search(h['href'])
                        if m:
                            ev['uuid'] = m.group(1)
                            ev['event_url'] = f"{BASE}/zone/events/{ev['uuid']}"
                except Exception as e:
                    log(f"Error en evento {i}: {e}")
                    continue
                events.append(ev)

            with open(OUT_FILE, "w", encoding="utf-8") as f:
                json.dump(events, f, ensure_ascii=False, indent=2)
            wrote = True
            log(f"✅ Guardado {len(events)} eventos en {OUT_FILE}")

        finally:
            try: driver.quit()
            except Exception: pass
            log("Navegador cerrado.")

    except Exception as e:
        log(f"ERROR durante el scraping: {e}")
        # intenta screenshot si hay driver en locals
        try:
            if 'driver' in locals():
                _save_screenshot(driver, "01_error.png")
        except Exception:
            pass

    # Garantiza que exista el fichero (aunque vacío) para facilitar debug del workflow
    if not wrote:
        try:
            with open(OUT_FILE, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            log(f"⚠️ No se extrajeron eventos, pero se creó {OUT_FILE} vacío para diagnóstico.")
        except Exception as e:
            log(f"❌ No pude crear {OUT_FILE}: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()
