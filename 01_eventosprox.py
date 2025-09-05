#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import random
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"

# 🔒 Credenciales desde entorno (no hardcode)
FLOW_EMAIL = os.getenv("FLOW_EMAIL", "").strip()
FLOW_PASS  = os.getenv("FLOW_PASS", "").strip()
if not FLOW_EMAIL or not FLOW_PASS:
    raise RuntimeError("Faltan FLOW_EMAIL/FLOW_PASS en el entorno.")

HEADLESS = True
INCOGNITO = True
MAX_SCROLLS = 10
SCROLL_WAIT_S = 1.5
OUT_DIR = "./output"
UUID_RE = re.compile(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})")

os.makedirs(OUT_DIR, exist_ok=True)

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def slow_pause(a=0.5, b=1.2): time.sleep(random.uniform(a, b))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        JavascriptException, StaleElementReferenceException, NoSuchElementException,
        ElementClickInterceptedException, TimeoutException
    )
    return webdriver, By, Options, WebDriverWait, EC, JavascriptException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, TimeoutException

def _get_driver():
    webdriver, By, Options, *_ = _import_selenium()
    from selenium.webdriver.chrome.service import Service
    opts = Options()
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except ImportError:
        return webdriver.Chrome(options=opts)

def _save_screenshot(driver, name):
    try:
        driver.save_screenshot(os.path.join(OUT_DIR, name))
    except Exception:
        pass

def _accept_cookies(driver, By):
    try:
        for sel in ('[data-testid="uc-accept-all-button"]','button[aria-label="Accept all"]','button[aria-label="Aceptar todo"]','button[mode="primary"]'):
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            if btns: btns[0].click(); slow_pause(0.8,1.8); return
        driver.execute_script("const b=[...document.querySelectorAll('button')].find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent)); if(b) b.click();")
        slow_pause(0.2,0.5)
    except Exception:
        pass

def _is_login_page(driver): return "/user/login" in (driver.current_url or "")

def _login(driver, By, WebDriverWait, EC):
    log("Iniciando login...")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL); slow_pause(0.2,0.4)
    pwd.clear();   pwd.send_keys(FLOW_PASS);   slow_pause(0.2,0.4)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause(); log("Login exitoso")

def _full_scroll(driver):
    last_h = 0
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h: break
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
        if len(date_elems) > 1: event_data['organizacion'] = date_elems[1].get_text(strip=True)
        name_elem = info_div.find('div', class_='font-caption text-lg text-black truncate -mt-1')
        if name_elem: event_data['nombre'] = name_elem.get_text(strip=True)
        club_elem = info_div.find('div', class_='text-xs mb-0.5 mt-0.5')
        if club_elem: event_data['club'] = club_elem.get_text(strip=True)
        for div in info_div.find_all('div', class_='text-xs'):
            text = div.get_text(strip=True)
            if '/' in text and ('Spain' in text or 'España' in text):
                event_data['lugar'] = text; break
    status_button = soup.find('div', class_='py-1 px-4 border text-white font-bold rounded text-sm')
    if status_button:
        event_data['estado'] = status_button.get_text(strip=True)
        if 'Inscribirse' in event_data['estado']: event_data['estado_tipo'] = 'inscripcion_abierta'
        elif 'En curso' in event_data['estado']:   event_data['estado_tipo'] = 'en_curso'
        else:                                      event_data['estado_tipo'] = 'desconocido'
    event_data['enlaces'] = {}
    info_link = soup.find('a', href=lambda x: x and '/info/' in x)
    if info_link: event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
    participants_link = soup.find('a', href=lambda x: x and '/participants_list' in x)
    if participants_link: event_data['enlaces']['participantes'] = urljoin(BASE, participants_link['href'])
    runs_link = soup.find('a', href=lambda x: x and '/runs' in x)
    if runs_link: event_data['enlaces']['runs'] = urljoin(BASE, runs_link['href'])
    flag_div = soup.find('div', class_='text-md')
    if flag_div: event_data['pais_bandera'] = flag_div.get_text(strip=True)
    return event_data

def main():
    log("=== Scraping FlowAgility - Competiciones de Agility ===")
    webdriver, By, Options, WebDriverWait, EC, *_ = _import_selenium()
    driver = _get_driver()
    try:
        _login(driver, By, WebDriverWait, EC)
        log("Navegando a la página de eventos...")
        driver.get(EVENTS_URL)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        _accept_cookies(driver, By)
        log("Cargando todos los eventos...")
        _full_scroll(driver); slow_pause(2, 3)
        page_html = driver.page_source
        log("Extrayendo información de eventos...")
        soup = BeautifulSoup(page_html, 'html.parser')
        containers = soup.find_all('div', class_='group mb-6')
        log(f"Encontrados {len(containers)} eventos")
        events = []
        for i, c in enumerate(containers, 1):
            try:
                d = extract_event_details(str(c))
                events.append(d)
                log(f"Procesado evento {i}/{len(containers)}: {d.get('nombre','Sin nombre')}")
            except Exception as e:
                log(f"Error procesando evento {i}: {e}")
        out_file = os.path.join(OUT_DIR, '01events.json')  # 🔄 nombre final directo
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        log(f"✅ {len(events)} eventos guardados en {out_file}")
    except Exception as e:
        log(f"Error durante el scraping: {e}")
        _save_screenshot(driver, "error_screenshot.png")
        raise
    finally:
        driver.quit(); log("Navegador cerrado")

if __name__ == "__main__":
    main()
