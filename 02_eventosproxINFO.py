#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_eventosproxINFO.py
---------------------
Enriquece la lista de eventos con detalles por UUID a partir del JSON del paso 01.

Uso:
  python 02_eventosproxINFO.py [ruta_01events.json] [ruta_salida_detallada.json]

Entradas:
  - ruta_01events.json (opcional): JSON del workflow 01 con la lista base de eventos.
    Si no se pasa, el script intentará extraer UUIDs de la página de eventos directamente.

Salidas:
  - JSON enriquecido: por defecto ./output/02competiciones_detalladas.json (o el 2º argumento).
  - CSV compat: ./Results/events_past_YYYY-MM-DD.csv (para mantener pipelines existentes).

Requisitos:
  pip install selenium webdriver-manager beautifulsoup4 lxml

Variables de entorno:
  FLOW_USER_EMAIL, FLOW_USER_PASSWORD
  HEADLESS=true|false (default: true)
  INCOGNITO=true|false (default: true)
  BASE=https://www.flowagility.com (por si cambia)
"""

import os
import re
import sys
import csv
import json
import time
import datetime as dt
from urllib.parse import urljoin

# ---------------------------
# Configuración (env + defaults)
# ---------------------------
BASE = os.getenv("BASE", "https://www.flowagility.com")
FLOW_EMAIL = os.getenv("FLOW_USER_EMAIL") or os.getenv("FLOW_EMAIL") or ""
FLOW_PASS  = os.getenv("FLOW_USER_PASSWORD") or os.getenv("FLOW_PASS") or ""
HEADLESS   = (os.getenv("HEADLESS", "true").lower() == "true")
INCOGNITO  = (os.getenv("INCOGNITO", "true").lower() == "true")

OUT_DIR      = "./output"
RESULTS_DIR  = "./Results"
DEFAULT_OUT  = os.path.join(OUT_DIR, "02competiciones_detalladas.json")
EVENTS_URL   = f"{BASE}/zone/events"

UUID_RE = re.compile(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", re.I)

# ---------------------------
# Utils
# ---------------------------
def log(msg):  # log con hora
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def sleep(a=0.2, b=0.6):  # pausa simple
    time.sleep(max(a, b))

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

def today_str():
    return dt.datetime.now().strftime("%Y-%m-%d")

# ---------------------------
# Selenium setup
# ---------------------------
def import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        JavascriptException, StaleElementReferenceException, NoSuchElementException,
        ElementClickInterceptedException, TimeoutException, WebDriverException
    )
    from selenium.webdriver.chrome.service import Service
    return (webdriver, By, Options, WebDriverWait, EC, JavascriptException,
            StaleElementReferenceException, NoSuchElementException,
            ElementClickInterceptedException, TimeoutException, WebDriverException, Service)

def get_driver():
    (webdriver, By, Options, *_), Service = import_selenium()[:4], import_selenium()[-1]
    opts = Options()
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    # webdriver-manager
    from webdriver_manager.chrome import ChromeDriverManager
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

# ---------------------------
# Login + helpers
# ---------------------------
def accept_cookies(driver, By):
    try:
        for sel in (
            '[data-testid="uc-accept-all-button"]',
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            'button[mode="primary"]',
        ):
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            if btns:
                btns[0].click()
                sleep(0.3, 0.6)
                return
        driver.execute_script("""
            const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
            if(b) b.click();
        """)
        sleep(0.2, 0.4)
    except Exception:
        pass

def login(driver, By, WebDriverWait, EC):
    if not FLOW_EMAIL or not FLOW_PASS:
        raise RuntimeError("Credenciales no encontradas. Define FLOW_USER_EMAIL y FLOW_USER_PASSWORD en el entorno.")
    log("Login…")
    driver.get(f"{BASE}/user/login")
    w = WebDriverWait(driver, 25)
    email = w.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL); sleep(0.1, 0.2)
    pwd.clear();   pwd.send_keys(FLOW_PASS);    sleep(0.1, 0.2)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    w.until(lambda d: "/user/login" not in d.current_url)
    sleep(0.3, 0.6)
    log("Login OK.")

def full_scroll(driver):
    last_h = 0
    for _ in range(16):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.0)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            break
        last_h = h

# ---------------------------
# Parse helpers (BeautifulSoup)
# ---------------------------
from bs4 import BeautifulSoup

def text_or(el):
    return el.get_text(strip=True) if el else ""

def parse_info_html(html):
    """
    Intenta extraer campos básicos del detalle del evento (página /info).
    Devuelve dict con campos 'title', 'dates', 'organizer', 'club', 'place', etc.
    """
    soup = BeautifulSoup(html, "lxml")

    def find_first(*selectors):
        for sel in selectors:
            el = soup.select_one(sel)
            if el: return el
        return None

    title = text_or(find_first("h1", ".font-caption.text-lg", "h2"))
    # fechas
    dates = None
    # varias zonas con 'text-xs' suelen contener fechas / organización
    xs = soup.select(".text-xs")
    if xs:
        # heuristic: primera 'text-xs' con patrón de fecha
        for div in xs:
            t = div.get_text(" ", strip=True)
            if any(k in t.lower() for k in ("202", "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic", "jan", "feb", "apr", "aug", "sept", "oct", "nov", "dec", "-")):
                dates = t
                break

    # club/organizador (heurístico)
    organizer = None
    club = None
    # A veces el club aparece como siguiente .text-xs
    if xs:
        # coge un par de candidatos (ordena por longitud corta)
        cands = [x.get_text(" ", strip=True) for x in xs]
        if len(cands) >= 2:
            organizer = cands[1]
        if len(cands) >= 3:
            club = cands[2]

    # lugar
    place = None
    for div in xs:
        t = div.get_text(" ", strip=True)
        if "/" in t and any(x in t for x in ("Spain", "España", "Portugal", "France", "Andorra", "Italia")):
            place = t
            break

    # jueces (si aparece grid o bloque de reglas)
    judges = []
    # buscar por keywords
    for table in soup.find_all(["table", "div", "section"]):
        txt = table.get_text(" ", strip=True)
        if re.search(r"juez|jueces|judge", txt, re.I):
            # intenta extraer nombres capitalizados separados por coma
            parts = re.split(r"[:;\n]", txt)
            for p in parts:
                if re.search(r"juez|jueces|judge", p, re.I):
                    # lo que siga pueden ser nombres
                    continue
                # filtro de nombres (heurístico)
                if len(p.strip()) >= 3 and any(c.isalpha() for c in p):
                    judges.extend([x.strip() for x in re.split(r",|·|\|", p) if x.strip()])
    judges = sorted(list({j for j in judges if j and len(j) < 80}))[:8]  # dedup + corta

    return {
        "title": title or None,
        "dates": dates or None,
        "organizer": organizer or None,
        "club": club or None,
        "place": place or None,
        "judges": judges or None
    }

def build_urls(uuid):
    base = f"{BASE}/zone/event/{uuid}"
    return {
        "info": urljoin(base + "/", "info"),
        "runs": urljoin(base + "/", "runs"),
        "participants": urljoin(base + "/", "participants_list"),
        "combined_results": urljoin(base + "/", "combined_results"),
    }

def extract_uuids_from_01json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    uuids = set()
    def maybe_add(s):
        if not s: return
        m = UUID_RE.search(s)
        if m: uuids.add(m.group(1).lower())

    for item in data:
        # enlaces
        enlaces = item.get("enlaces") or {}
        maybe_add(enlaces.get("info"))
        maybe_add(enlaces.get("runs"))
        maybe_add(enlaces.get("participantes"))
        # id por si acaso fuera el uuid
        maybe_add(item.get("id"))
        # cualquier otro texto
        for v in item.values():
            if isinstance(v, str):
                maybe_add(v)
            elif isinstance(v, dict):
                for vv in v.values():
                    if isinstance(vv, str):
                        maybe_add(vv)
    return sorted(uuids)

# ---------------------------
# Main
# ---------------------------
def main():
    ensure_dirs()
    in_json  = sys.argv[1] if len(sys.argv) >= 2 else None
    out_json = sys.argv[2] if len(sys.argv) >= 3 else DEFAULT_OUT

    (webdriver, By, Options, WebDriverWait, EC, JavascriptException,
     StaleElementReferenceException, NoSuchElementException,
     ElementClickInterceptedException, TimeoutException, WebDriverException, Service) = import_selenium()

    driver = get_driver()
    try:
        log("—— INICIO PAST ——")
        driver.get(BASE)
        accept_cookies(driver, By)
        login(driver, By, WebDriverWait, EC)

        # === preparar lista de UUIDs ===
        if in_json and os.path.isfile(in_json):
            uuids = extract_uuids_from_01json(in_json)
        else:
            # fallback: escanear /zone/events y sacar uuids de los enlaces
            log("No se pasó JSON del paso 01; extrayendo UUIDs desde la página de eventos.")
            driver.get(EVENTS_URL)
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            accept_cookies(driver, By)
            full_scroll(driver)
            html = driver.page_source
            uuids = sorted(set(m.group(1).lower() for m in UUID_RE.finditer(html)))

        log(f"UUIDs detectados: {len(uuids)}")

        detailed = []
        rows_csv = []

        for i, uid in enumerate(uuids, 1):
            log(f"[{i}/{len(uuids)}] {uid} -> detalle")
            urls = build_urls(uid)
            ev = {
                "uuid": uid,
                "urls": urls,
                "source": "02_eventosproxINFO.py",
                "scraped_at": dt.datetime.now().isoformat(timespec="seconds")
            }

            # INFO page
            try:
                driver.get(urls["info"])
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                info_dict = parse_info_html(driver.page_source)
                ev.update(info_dict)
            except Exception as e:
                ev.setdefault("errors", []).append(f"info_fail:{type(e).__name__}")

            # RUNS page (opcional: marcar si existe)
            try:
                driver.get(urls["runs"])
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                # ejemplo de chequeo mínimo
                ev["has_runs_page"] = True
            except Exception:
                ev["has_runs_page"] = False

            # PARTICIPANTS page (para que el 03 use el enlace)
            try:
                driver.get(urls["participants"])
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                ev["has_participants_page"] = True
            except Exception:
                ev["has_participants_page"] = False

            detailed.append(ev)

            # fila CSV compat
            rows_csv.append({
                "uuid": uid,
                "title": ev.get("title") or "",
                "dates": ev.get("dates") or "",
                "organizer": ev.get("organizer") or "",
                "club": ev.get("club") or "",
                "place": ev.get("place") or "",
                "participants_url": urls.get("participants") or "",
                "runs_url": urls.get("runs") or "",
                "info_url": urls.get("info") or "",
            })

        # === Guardar JSON enriquecido ===
        os.makedirs(os.path.dirname(out_json), exist_ok=True)
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(detailed, f, ensure_ascii=False, indent=2)

        # === Guardar CSV compat ===
        csv_path = os.path.join(RESULTS_DIR, f"events_past_{today_str()}.csv")
        fieldnames = ["uuid", "title", "dates", "organizer", "club", "place",
                      "participants_url", "runs_url", "info_url"]
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows_csv:
                w.writerow(r)

        log(f"Guardado: {csv_path} | filas={len(rows_csv)}")
        log("—— FIN PAST ——")

    except Exception as e:
        log(f"ERROR general: {type(e).__name__}: {e}")
        try:
            driver.save_screenshot(os.path.join(OUT_DIR, "02_error.png"))
            log("Screenshot guardada en ./output/02_error.png")
        except Exception:
            pass
        raise
    finally:
        driver.quit()
        log("Navegador cerrado.")

if __name__ == "__main__":
    main()
