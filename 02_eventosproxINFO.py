# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 18:59:25 2025

@author: Juan
"""

#1) Scraper de eventos pasados → Results/events_past_<YYYY-MM-DD>.csv

# -*- coding: utf-8 -*-
# FlowAgility · Scraper de eventos pasados (/zone/events/past)
# Salida: Results/events_past_<YYYY-MM-DD>.csv con:
#   event_uuid, events_url(/zone/event/<uuid>/runs), title, organizer, location, dates

import os, re, csv, time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    StaleElementReferenceException, TimeoutException, NoSuchElementException,
    ElementClickInterceptedException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib3.exceptions import ReadTimeoutError

BASE      = "https://www.flowagility.com"
PAST_URL  = f"{BASE}/zone/events/past"

FLOW_EMAIL = os.environ.get("FLOW_EMAIL", "jescosq@gmail.com")
FLOW_PASS  = os.environ.get("FLOW_PASS",  "Seattle1")

HEADLESS  = True
INCOGNITO = True

MAX_SCROLLS   = 24
SCROLL_WAIT_S = 0.5

OUT_DIR   = os.path.abspath("./Results")
os.makedirs(OUT_DIR, exist_ok=True)
DATE_STR  = datetime.now().strftime("%Y-%m-%d")
DESTINATION_FILE = os.path.join(OUT_DIR, f"events_past_{DATE_STR}.csv")

UUID_RE   = re.compile(r"/zone/events/([0-9a-fA-F-]{36})(?:/.*)?$")

def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}")

def get_driver():
    opts = Options()
    opts.page_load_strategy = "eager"
    if HEADLESS:  opts.add_argument("--headless=new")
    if INCOGNITO: opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu"); opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--proxy-server=direct://"); opts.add_argument("--proxy-bypass-list=*")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")
    d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(240); d.set_script_timeout(90)
    return d

def wait_dom_interactive(d, timeout=25):
    WebDriverWait(d, timeout).until(lambda x: x.execute_script("return document.readyState") in ("interactive","complete"))

def go(d, url, tries=3):
    last = None
    for _ in range(tries):
        try:
            d.get(url)
            wait_dom_interactive(d, 30)
            return True
        except (TimeoutException, ReadTimeoutError) as e:
            last = e
            try:
                d.execute_script("window.stop();")
                wait_dom_interactive(d, 10)
                return True
            except Exception:
                pass
            try: d.get("about:blank")
            except Exception: pass
            time.sleep(1.0)
    if last: raise last
    raise TimeoutException("Navigation timeout")

def accept_cookies(d):
    try:
        for sel in (
            '[data-testid="uc-accept-all-button"]',
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            'button[mode="primary"]',
        ):
            btns = d.find_elements(By.CSS_SELECTOR, sel)
            if btns:
                d.execute_script("arguments[0].click();", btns[0]); time.sleep(0.2); return
        d.execute_script("""
            const b=[...document.querySelectorAll('button')]
              .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent));
            if(b) b.click();
        """)
        time.sleep(0.2)
    except Exception:
        pass

def is_login_page(d):
    return "/user/login" in (d.current_url or "")

def login(d):
    log("Login…")
    go(d, f"{BASE}/user/login")
    wait = WebDriverWait(d, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = d.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL)
    pwd.clear();   pwd.send_keys(FLOW_PASS)
    d.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    WebDriverWait(d, 25).until(lambda x: "/user/login" not in x.current_url)
    time.sleep(0.5)
    log("Login OK.")

def click_ver_todos_hasta_agotar(d, max_clicks=50):
    xp = ("//button[normalize-space()='Ver todos los eventos'] | "
          "//a[normalize-space()='Ver todos los eventos'] | "
          "//*[self::button or self::a][contains(normalize-space(.), 'Ver todos los eventos')]")
    clicks = 0; loaded = False
    while clicks < max_clicks:
        try:
            btn = WebDriverWait(d, 2).until(EC.presence_of_element_located((By.XPATH, xp)))
        except TimeoutException:
            break
        d.execute_script("arguments[0].click();", btn)
        time.sleep(SCROLL_WAIT_S + 0.5)
        clicks += 1; loaded = True
    return loaded

def try_infinite_scroll(d, max_scrolls=MAX_SCROLLS, pause=SCROLL_WAIT_S):
    last_h = d.execute_script("return document.body.scrollHeight"); grew = False
    for _ in range(max_scrolls):
        d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_h = d.execute_script("return document.body.scrollHeight")
        if new_h > last_h: grew = True; last_h = new_h
        else: break
    return grew

def text_or_empty(el):
    try: return (el.text or "").strip()
    except Exception: return ""

def first_nonempty_text(els):
    for e in els:
        t = text_or_empty(e)
        if len(t) >= 2: return t
    return ""

MONTH_RE = re.compile(r"(Ene|Feb|Mar|Abr|May|Jun|Jul|Ago|Sep|Oct|Nov|Dic|\d{1,2}/\d{1,2}/\d{2,4})", re.I)

def extract_event_detail(d, uuid):
    title = organizer = location = dates = ""
    for url in (f"{BASE}/zone/events/{uuid}", f"{BASE}/zone/event/{uuid}"):
        try:
            go(d, url); accept_cookies(d)
            WebDriverWait(d, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(0.2)

            t_candidates = d.find_elements(By.CSS_SELECTOR, "#event_header h1, h1.text-2xl, h1.text-3xl, h1, .prose h1")
            if not t_candidates:
                t_candidates = d.find_elements(By.CSS_SELECTOR, "h2.text-2xl, h2")
            title = first_nonempty_text(t_candidates)
            if not title:
                try:
                    ot = d.find_element(By.CSS_SELECTOR, "meta[property='og:title']")
                    title = (ot.get_attribute("content") or "").strip()
                except NoSuchElementException:
                    pass
            if not title:
                try:
                    dt = d.title or ""
                    title = re.sub(r"\s*\|\s*FlowAgility.*$", "", dt).strip()
                except Exception:
                    pass

            try:
                header = d.find_element(By.CSS_SELECTOR, "#event_header")
            except NoSuchElementException:
                header = None

            if header:
                smalls = header.find_elements(By.CSS_SELECTOR, ".text-sm, .text-xs, .text-gray-600, .text-gray-500, .opacity-70")
                lines = [text_or_empty(x) for x in smalls if text_or_empty(x)]
                if lines:
                    if len(lines) >= 1 and not organizer: organizer = lines[0]
                    if len(lines) >= 2 and not location:  location  = lines[1]
                    if len(lines) >= 3 and not dates:     dates     = lines[2]
                if dates and not MONTH_RE.search(dates):
                    for ln in lines:
                        if MONTH_RE.search(ln):
                            dates = ln; break

            if not organizer:
                try:
                    el = d.find_element(By.XPATH, "//*[contains(translate(., 'ORGANIZA', 'organiza'),'organiza')]/following::*[(self::div or self::span)][1]")
                    organizer = text_or_empty(el)
                except NoSuchElementException:
                    pass
            if not location:
                try:
                    el = d.find_element(By.XPATH, "//*[contains(translate(., 'LOCALIZACIÓN', 'localización'),'localiz') or contains(translate(., 'LOCATION', 'location'),'location')]/following::*[(self::div or self::span)][1]")
                    location = text_or_empty(el)
                except NoSuchElementException:
                    pass
            if not dates:
                try:
                    el = d.find_element(By.XPATH, "//*[contains(translate(., 'FECHA', 'fecha'),'fecha') or contains(., 'Date')]/following::*[(self::div or self::span)][1]")
                    cand = text_or_empty(el)
                    if MONTH_RE.search(cand): dates = cand
                except NoSuchElementException:
                    pass

            if title: break
        except Exception:
            continue

    for k in ("title","organizer","location","dates"):
        v = locals()[k]
        if isinstance(v, str):
            locals()[k] = re.sub(r"\s+", " ", v).strip()

    return {"uuid": uuid, "title": title, "organizer": organizer, "location": location, "dates": dates}

def collect_uuids_from_past(d):
    uuids, seen = [], set()
    anchors = d.find_elements(By.CSS_SELECTOR, "a[href*='/zone/events/']")
    for a in anchors:
        try: href = a.get_attribute("href") or ""
        except StaleElementReferenceException: continue
        m = UUID_RE.search(href)
        if not m: continue
        uuid = m.group(1).lower()
        if uuid not in seen:
            seen.add(uuid); uuids.append(uuid)
    return uuids

def main():
    log("—— INICIO PAST ——")
    d = get_driver()
    rows = []
    try:
        go(d, PAST_URL); accept_cookies(d)
        if is_login_page(d):
            login(d); go(d, PAST_URL); accept_cookies(d)

        WebDriverWait(d, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.5)

        click_ver_todos_hasta_agotar(d)
        try_infinite_scroll(d)

        uuids = collect_uuids_from_past(d)
        log(f"UUIDs detectados: {len(uuids)}")

        for i, uuid in enumerate(uuids, 1):
            log(f"[{i}/{len(uuids)}] {uuid} -> detalle")
            info = extract_event_detail(d, uuid)
            events_url = f"{BASE}/zone/event/{uuid}/runs"  # salida estándar
            rows.append([uuid, events_url, info["title"], info["organizer"], info["location"], info["dates"]])
            time.sleep(0.15)

        with open(DESTINATION_FILE, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["event_uuid", "events_url", "title", "organizer", "location", "dates"])
            w.writerows(rows)

        log(f"Guardado: {DESTINATION_FILE} | filas={len(rows)}")
        log("—— FIN PAST ——")
    finally:
        try: d.quit()
        except Exception: pass

if __name__ == "__main__":
    main()
