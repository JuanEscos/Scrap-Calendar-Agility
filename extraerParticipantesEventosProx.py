#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extraerParticipantesEventosProx.py
Unificado 01+02+03+04

ENV requeridas:
  FLOW_EMAIL, FLOW_PASS

ENV opcionales:
  OUT_DIR=./output
  HEADLESS=true
  INCOGNITO=true
  SHOW_CONFIG=true

  # Límites y tiempos (para controlar presión sobre la web):
  LIMIT_EVENTS=0              # 0 = sin límite
  LIMIT_PARTICIPANTS=0        # 0 = sin límite
  MAX_SCROLLS=24
  SCROLL_WAIT_S=2.0
  SLOW_MIN_S=1.0
  SLOW_MAX_S=3.0
  PER_PART_TIMEOUT_S=10
  MAX_EVENT_SECONDS=1800

Salidas que siempre intenta generar:
  ./output/01events_YYYY-MM-DD.json         y   ./output/01events_last.json
  ./output/02competiciones_detalladas_YYYY-MM-DD.json   y   ./output/02info_last.json
  ./output/participantes_procesado_YYYY-MM-DD.csv       y   ./output/participantes.json
  ./output/participants_completos_final.json

Uso:
  python extraerParticipantesEventosProx.py            # hace todo
  python extraerParticipantesEventosProx.py --skip-02  # salta enriquecido
  python extraerParticipantesEventosProx.py --skip-03  # salta participantes
  python extraerParticipantesEventosProx.py --skip-04  # salta unión
"""

import os, re, sys, json, csv, time, random, unicodedata, argparse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# =========================== Config/ENV ===========================
OUT_DIR = os.path.abspath(os.getenv("OUT_DIR", "./output"))
os.makedirs(OUT_DIR, exist_ok=True)

DATE_STR = datetime.now().strftime("%Y-%m-%d")
BASE = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"
UUID_RE = re.compile(r"/zone/events/([0-9a-fA-F-]{36})(?:/.*)?$")

FLOW_EMAIL = os.getenv("FLOW_EMAIL") or os.getenv("FLOW_USER_EMAIL")
FLOW_PASS  = os.getenv("FLOW_PASS")  or os.getenv("FLOW_USER_PASSWORD")
if not FLOW_EMAIL or not FLOW_PASS:
    print("[ERROR] Faltan FLOW_EMAIL/FLOW_PASS en entorno.")
    sys.exit(2)

HEADLESS           = str(os.getenv("HEADLESS","true")).lower() in ("1","true","yes","on")
INCOGNITO          = str(os.getenv("INCOGNITO","true")).lower() in ("1","true","yes","on")
SHOW_CONFIG        = str(os.getenv("SHOW_CONFIG","false")).lower() in ("1","true","yes","on")
MAX_SCROLLS        = int(os.getenv("MAX_SCROLLS", "24"))
SCROLL_WAIT_S      = float(os.getenv("SCROLL_WAIT_S", "2.0"))
SLOW_MIN_S         = float(os.getenv("SLOW_MIN_S", "1.0"))
SLOW_MAX_S         = float(os.getenv("SLOW_MAX_S", "3.0"))
PER_PART_TIMEOUT_S = float(os.getenv("PER_PART_TIMEOUT_S", "10"))
MAX_EVENT_SECONDS  = int(os.getenv("MAX_EVENT_SECONDS", "1800"))
LIMIT_EVENTS       = int(os.getenv("LIMIT_EVENTS", "0"))
LIMIT_PARTICIPANTS = int(os.getenv("LIMIT_PARTICIPANTS", "0"))

def _print_effective_config():
    if not SHOW_CONFIG: return
    print("=== Config efectiva ===")
    print(f"OUT_DIR              = {OUT_DIR}")
    print(f"HEADLESS             = {HEADLESS}")
    print(f"INCOGNITO            = {INCOGNITO}")
    print(f"MAX_SCROLLS          = {MAX_SCROLLS}")
    print(f"SCROLL_WAIT_S        = {SCROLL_WAIT_S}")
    print(f"SLOW_MIN_S           = {SLOW_MIN_S}")
    print(f"SLOW_MAX_S           = {SLOW_MAX_S}")
    print(f"PER_PART_TIMEOUT_S   = {PER_PART_TIMEOUT_S}")
    print(f"MAX_EVENT_SECONDS    = {MAX_EVENT_SECONDS}")
    print(f"LIMIT_EVENTS         = {LIMIT_EVENTS}")
    print(f"LIMIT_PARTICIPANTS   = {LIMIT_PARTICIPANTS}")
    print("=======================")

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
def slow_pause(a=None,b=None):
    a = SLOW_MIN_S if a is None else float(a)
    b = SLOW_MAX_S if b is None else float(b)
    if b < a: a,b = b,a
    time.sleep(random.uniform(a,b))

# ======================= Selenium Utils =======================
def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, StaleElementReferenceException, NoSuchElementException,
        ElementClickInterceptedException, JavascriptException, WebDriverException
    )
    from selenium.webdriver.chrome.service import Service
    return (webdriver, By, Options, WebDriverWait, EC,
            TimeoutException, StaleElementReferenceException, NoSuchElementException,
            ElementClickInterceptedException, JavascriptException, WebDriverException, Service)

def _get_driver():
    (webdriver, By, Options, *_rest) = _import_selenium()
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
        from selenium.webdriver.chrome.service import Service
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
                driver.execute_script("arguments[0].click();", btns[0])
                slow_pause(0.3,0.7)
                return
        driver.execute_script("""
          const b=[...document.querySelectorAll('button')]
            .find(x=>/acept|accept|consent|de acuerdo/i.test(x.textContent||''));
          if(b) b.click();
        """)
        slow_pause(0.2,0.5)
    except Exception:
        pass

def _login(driver, By, WebDriverWait, EC):
    log("Login…")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd   = driver.find_element(By.NAME, "user[password]")
    email.clear(); email.send_keys(FLOW_EMAIL); slow_pause(0.2,0.5)
    pwd.clear();   pwd.send_keys(FLOW_PASS);    slow_pause(0.2,0.5)
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause(0.8,1.2)
    log("Login OK.")

def _full_scroll(driver):
    last_h = 0
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h: break
        last_h = h

# ======================= 01: Eventos Base =======================
def collect_events_base(driver, By, WebDriverWait, EC):
    """
    Emula 01: recorre /zone/events y saca lista con campos mínimos + enlaces.
    """
    driver.get(EVENTS_URL)
    WebDriverWait(driver, 25).until(lambda d: d.find_element(By.TAG_NAME, "body"))
    _accept_cookies(driver, By)
    _full_scroll(driver)
    slow_pause()

    by_uuid = {}
    anchors = driver.find_elements(By.TAG_NAME, "a")
    for a in anchors:
        href = a.get_attribute("href") or ""
        if not href: continue
        if href.startswith("/"): href = urljoin(BASE, href)
        if "flowagility.com/zone/events/" not in href: continue
        m = UUID_RE.search(href)
        if not m: continue
        uuid = m.group(1)
        is_plist = href.rstrip("/").endswith("participants_list")
        base_url = f"{BASE}/zone/events/{uuid}"
        d = by_uuid.get(uuid, {"base": base_url, "plist": None})
        if is_plist: d["plist"] = href
        else:        d["base"]  = base_url
        by_uuid[uuid] = d

    # Modelado de eventos base (mínimo)
    out=[]
    for uuid, pair in by_uuid.items():
        base = pair["base"]
        info = base.rstrip("/") + "/info"
        rec = {
            "uuid": uuid,
            "nombre": uuid,          # se rellena mejor en 02 o 03
            "enlaces": {
                "base": base,
                "info": info,
                "participants_list": pair["plist"] or (base.rstrip("/") + "/participants_list")
            }
        }
        out.append(rec)

    if LIMIT_EVENTS > 0:
        out = out[:LIMIT_EVENTS]
    return out

def write_01events(events):
    p_last = os.path.join(OUT_DIR, "01events_last.json")
    p_dated = os.path.join(OUT_DIR, f"01events_{DATE_STR}.json")
    with open(p_last, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    with open(p_dated, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    log(f"01 -> {p_last}  (y fechado)")

# ======================= 02: Enriquecido INFO =======================
def _extract_info_from_info_page(driver, info_url):
    data = {
        "informacion_general": {},
        "inscripcion": {},
        "pruebas": [],
        "contacto": {},
        "enlaces_adicionales": {},
        "url_detalle": info_url,
    }
    driver.get(info_url)
    slow_pause(1.0,1.8)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    def txt(el): return (el.get_text(" ", strip=True) or "").strip() if el else ""

    title = soup.find(["h1","h2"], string=True) or soup.find("h1")
    if title:
        data["informacion_general"]["titulo"] = txt(title)

    blocks = soup.find_all(["div","span","p","li"])
    for b in blocks:
        s = txt(b)
        if not s: continue
        if " - " in s and len(s) < 80 and re.search(r"\b(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", s, re.I):
            data["informacion_general"].setdefault("fechas_completas", s)
        if any(k in s.lower() for k in ["spain","españa","france","italy","portugal","madrid","barcelona","valencia","sevilla","lisboa","roma","paris"]):
            data["informacion_general"].setdefault("ubicacion_completa", s)

    for b in blocks:
        s = txt(b).lower()
        if "inscrip" in s or "registration" in s:
            data["inscripcion"].setdefault("periodo_inscripcion", txt(b))
        if ("€" in s) or ("eur" in s) or ("precio" in s) or ("price" in s) or ("coste" in s):
            if "precios" not in data["inscripcion"]:
                data["inscripcion"]["precios"] = txt(b)

    for a in soup.find_all("a", href=True):
        href = a["href"].strip().lower()
        full = urljoin(BASE, a["href"].strip())
        if any(k in href for k in ["reglamento","regulation","rules","normas"]):
            data["enlaces_adicionales"]["reglamento"] = full
        if any(k in href for k in ["map","ubicacion","location","google"]):
            data["enlaces_adicionales"]["mapa"] = full

    # contacto relativo
    emails=set(); phones=set()
    for t in soup.find_all(string=re.compile(r"@")):
        s = str(t).strip()
        if "@" in s and "." in s: emails.add(s)
    if emails:
        data["contacto"]["email"] = " | ".join(sorted(emails))

    return data

def run_02_enrich(driver, By, WebDriverWait, EC, base_events):
    detailed=[]
    for i, ev in enumerate(base_events, 1):
        nombre = ev.get("nombre") or ev.get("uuid")
        info_url = (ev.get("enlaces") or {}).get("info","")
        log(f"[02] {i}/{len(base_events)} -> {nombre}")
        merged = dict(ev)
        if info_url:
            try:
                data = _extract_info_from_info_page(driver, info_url)
                merged.update({
                    "informacion_general": data.get("informacion_general", {}),
                    "inscripcion": data.get("inscripcion", {}),
                    "pruebas": data.get("pruebas", []),
                    "contacto": data.get("contacto", {}),
                    "enlaces_adicionales": data.get("enlaces_adicionales", {}),
                    "url_detalle": data.get("url_detalle", info_url),
                })
            except Exception as e:
                log(f"    WARN: {e}")
        detailed.append(merged)
        slow_pause(0.8,1.6)
    # escribir
    p_last  = os.path.join(OUT_DIR, "02info_last.json")
    p_dated = os.path.join(OUT_DIR, f"02competiciones_detalladas_{DATE_STR}.json")
    with open(p_last, "w", encoding="utf-8") as f: json.dump(detailed, f, ensure_ascii=False, indent=2)
    with open(p_dated,"w", encoding="utf-8") as f: json.dump(detailed, f, ensure_ascii=False, indent=2)
    log(f"02 -> {p_last}  (y fechado)")
    return detailed

# ======================= 03: Participantes (scrape + process) =======================
EMOJI_RE = re.compile(
    "[\U0001F1E6-\U0001F1FF\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+"
)
def _clean(s:str)->str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = EMOJI_RE.sub("", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:")

def _collect_booking_ids(driver):
    try:
        ids = driver.execute_script("""
            return Array.from(
              document.querySelectorAll("[phx-click='booking_details_show']")
            ).map(el => el.getAttribute("phx-value-booking_id")).filter(Boolean);
        """) or []
    except Exception:
        ids = []
    seen, out = set(), []
    for x in ids:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _click_toggle_by_pid(driver, pid, By, WebDriverWait, EC, TimeoutException):
    sel = f"[phx-click='booking_details_show'][phx-value-booking_id='{pid}']"
    for _ in range(5):
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            WebDriverWait(driver, 8).until(lambda d: d.find_element(By.ID, pid))
            return driver.find_element(By.ID, pid)
        except TimeoutException:
            time.sleep(0.25)
    return None

JS_MAP_PARTICIPANT_RICH = r"""
const pid = arguments[0];
const root = document.getElementById(pid);
if (!root) return null;

const txt = el => (el && el.textContent) ? el.textContent.trim() : null;

function classListArray(el){
  if (!el) return [];
  const cn = el.className;
  if (!cn) return [];
  if (typeof cn === 'string') return cn.trim().split(/\s+/);
  if (typeof cn === 'object' && 'baseVal' in cn) return String(cn.baseVal).trim().split(/\s+/);
  return String(cn).trim().split(/\s+/);
}
function isHeader(el){
  const arr = classListArray(el);
  return (arr.includes('border-b') && arr.includes('border-gray-400'))
      || (arr.includes('font-bold') && arr.includes('text-sm') && arr.some(c => /^mt-/.test(c)));
}
function isLabel(el){ return (classListArray(el).includes('text-gray-500') && classListArray(el).includes('text-sm')); }
function isStrong(el){
  const arr = classListArray(el);
  return (arr.includes('font-bold') && arr.includes('text-sm'));
}
function nextStrong(el){
  let cur = el;
  for (let i=0;i<8;i++){
    cur = cur && cur.nextElementSibling;
    if (!cur) break;
    if (isStrong(cur)) return cur;
  }
  return null;
}

const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, null);
let node = walker.currentNode;
let currentDay = null;
let tmpFecha = null;
let tmpMangas = null;

const fields = {};
const schedule = [];
const simpleFieldLabels = new Set([
  "Dorsal","Guía","Guia","Perro","Raza","Edad","Género","Genero",
  "Altura (cm)","Altura","Nombre de Pedigree","Nombre de Pedrigree",
  "País","Pais","Licencia","Equipo","Club","Federación","Federacion"
]);

while (node){
  if (isHeader(node)){
    const t = txt(node); if (t) currentDay = t;
  } else if (isLabel(node)){
    const label = (txt(node) || "");
    const valueEl = nextStrong(node);
    const value = txt(valueEl) || "";

    const l = label.toLowerCase();
    if (l.startsWith("fecha"))       { tmpFecha  = value; }
    else if (l.startsWith("mangas")) { tmpMangas = value; }
    else if (simpleFieldLabels.has(label) && value && (fields[label] == null || fields[label] === "")) {
      fields[label] = value;
    }

    if (tmpFecha !== null && tmpMangas !== null){
      schedule.push({ day: currentDay || "", fecha: tmpFecha, mangas: tmpMangas });
      tmpFecha = null; tmpMangas = null;
    }
  }
  node = walker.nextNode();
}
return { fields, schedule };
"""

def _fallback_map_participant(driver, pid, By):
    labels = driver.find_elements(By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'text-gray-500') and contains(@class,'text-sm')]")
    values = driver.find_elements(By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'font-bold') and contains(@class,'text-sm')]")
    fields={}
    for lab_el, val_el in zip(labels, values):
        lt = _clean(lab_el.text or "")
        vt = _clean(val_el.text or "")
        if lt and vt and lt not in fields: fields[lt]=vt
    headers = driver.find_elements(By.XPATH, f"//div[@id='{pid}']//div[contains(@class,'border-b') and contains(@class,'border-gray-400')]")
    schedule=[]
    for h in headers:
        fecha = h.find_elements(By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm')][1]")
        mangas= h.find_elements(By.XPATH, "following-sibling::div[contains(@class,'font-bold') and contains(@class,'text-sm')][2]")
        schedule.append({
            "day": _clean(h.text or ""),
            "fecha": _clean(fecha[0].text if fecha else ""),
            "mangas": _clean(mangas[0].text if mangas else "")
        })
    return {"fields": fields, "schedule": schedule}

EVENT_HEADER = [
    "uuid","event_url","title","organizer","location","dates",
    "header_1","header_2","header_3","header_4","header_5","header_6",
    "judges"
]
PART_BASE = [
    "participants_url","BinomID","Dorsal","Guía","Perro","Raza","Edad","Género",
    "Altura (cm)","Nombre de Pedigree","País","Licencia","Club","Federación","Equipo",
    "event_uuid","event_title"
]
PART_SLOTS=[]
for i in range(1,7):
    PART_SLOTS += [f"Día {i}", f"Fecha {i}", f"Mangas {i}"]
PART_HEADER = PART_BASE + PART_SLOTS

def _ensure_csv_header(path, header):
    if not os.path.exists(path):
        with open(path,"w",newline="",encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=header).writeheader()

def _append_csv_row(path, header, row_dict):
    _ensure_csv_header(path, header)
    with open(path,"a",newline="",encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writerow({k: row_dict.get(k, "") for k in header})

def _scrape_event_title_from_header(driver, By):
    try:
        heads = [e.text.strip() for e in driver.find_elements(By.CSS_SELECTOR, "h1, h2, [role='heading']") if e.text.strip()]
        heads = [h for h in heads if h.lower() != "flowagility"]
        if heads:
            heads.sort(key=len, reverse=True)
            return heads[0]
        tmeta = driver.execute_script("return (document.querySelector(\"meta[property='og:title']\")||{}).content || ''") or ""
        if tmeta and tmeta.lower()!="flowagility": return tmeta
        t = (driver.title or "").strip()
        return t if t.lower()!="flowagility" else "N/D"
    except Exception:
        return "N/D"

def run_03_participants(driver, By, WebDriverWait, EC, TimeoutException, base_events):
    csv_event = os.path.join(OUT_DIR, f"03events_{DATE_STR}.csv")
    csv_part  = os.path.join(OUT_DIR, f"03participantes_{DATE_STR}.csv")
    _ensure_csv_header(csv_event, EVENT_HEADER)
    _ensure_csv_header(csv_part,  PART_HEADER)

    for idx_ev, ev in enumerate(base_events, 1):
        plist = (ev.get("enlaces") or {}).get("participants_list","")
        base  = (ev.get("enlaces") or {}).get("base","")
        uuid  = ev.get("uuid","")
        if not plist:
            continue

        start_ts = time.time()

        # Event-level info
        try:
            driver.get(base)
            WebDriverWait(driver, 20).until(lambda d: d.find_element(By.TAG_NAME,"body"))
            _accept_cookies(driver, By)
            slow_pause(0.4,0.8)
            ev_title = _scrape_event_title_from_header(driver, By)
        except Exception:
            ev_title = ev.get("nombre") or "N/D"

        _append_csv_row(csv_event, EVENT_HEADER, {
            "uuid": uuid, "event_url": base, "title": ev_title,
            "organizer": "N/D", "location": "N/D", "dates": "N/D",
            "header_1":"","header_2":"","header_3":"","header_4":"","header_5":"","header_6":"",
            "judges":"N/D"
        })
        slow_pause()

        # participants_list
        try:
            driver.get(plist)
            WebDriverWait(driver, 25).until(lambda d: d.find_element(By.TAG_NAME, "body"))
            _accept_cookies(driver, By)
            slow_pause(0.8,1.6)
        except Exception:
            continue

        booking_ids = _collect_booking_ids(driver)
        total = len(booking_ids)
        if total == 0:
            continue

        if LIMIT_PARTICIPANTS > 0:
            booking_ids = booking_ids[:LIMIT_PARTICIPANTS]
            total = len(booking_ids)

        for i,pid in enumerate(booking_ids, start=1):
            if i%25==0 or i==total: log(f"[03] {ev_title}  {i}/{total}")
            block_el = _click_toggle_by_pid(driver, pid, By, WebDriverWait, EC, TimeoutException)
            if not block_el:
                slow_pause(0.2,0.5)
                continue
            # pintar
            painted=False; end=time.time()+PER_PART_TIMEOUT_S
            while time.time()<end:
                try:
                    strongs = block_el.find_elements(By.XPATH, ".//div[contains(@class,'font-bold') and contains(@class,'text-sm')]")
                    if strongs:
                        painted=True; break
                except Exception:
                    pass
                time.sleep(0.25)
            if not painted:
                slow_pause(0.2,0.5); continue

            # payload
            try:
                payload = driver.execute_script(JS_MAP_PARTICIPANT_RICH, pid)
            except Exception:
                payload = None
            if not payload or not isinstance(payload, dict):
                payload = _fallback_map_participant(driver, pid, By)

            fields = (payload.get("fields") or {})
            schedule = (payload.get("schedule") or [])

            def pick(keys, default="No disponible"):
                for k in keys:
                    v = fields.get(k)
                    if v: return _clean(v)
                return default

            row = {
                "participants_url": plist,
                "BinomID": pid,
                "Dorsal": pick(["Dorsal"]),
                "Guía": pick(["Guía","Guia"]),
                "Perro": pick(["Perro"]),
                "Raza": pick(["Raza"]),
                "Edad": pick(["Edad"], default=""),
                "Género": pick(["Género","Genero"]),
                "Altura (cm)": pick(["Altura (cm)","Altura"]),
                "Nombre de Pedigree": pick(["Nombre de Pedigree","Nombre de Pedrigree"]),
                "País": pick(["País","Pais"]),
                "Licencia": pick(["Licencia"]),
                "Club": pick(["Club"]),
                "Federación": pick(["Federación","Federacion"]),
                "Equipo": pick(["Equipo"]),
                "event_uuid": uuid,
                "event_title": ev_title,
            }
            for j in range(1,7):
                day = schedule[j-1]["day"] if j-1 < len(schedule) else ""
                fec = schedule[j-1]["fecha"] if j-1 < len(schedule) else ""
                man = schedule[j-1]["mangas"] if j-1 < len(schedule) else ""
                row[f"Día {j}"]    = _clean(day)
                row[f"Fecha {j}"]  = _clean(fec)
                row[f"Mangas {j}"] = _clean(man)

            _append_csv_row(csv_part, PART_HEADER, row)
            slow_pause()

        if time.time() - start_ts > MAX_EVENT_SECONDS:
            log(f"[03] evento lento; sigo con el siguiente…")
            continue

    # Procesado (como 03)
    import pandas as pd, numpy as np
    from dateutil import parser as dtparser

    def to_spanish_dd_mm_yyyy(val):
        if not isinstance(val, str) or not val.strip(): return val
        try:
            dt = dtparser.parse(val, dayfirst=True, fuzzy=True)
            return dt.strftime("%d-%m-%Y")
        except Exception:
            return val

    def strip_accents(s):
        if not isinstance(s, str): return s
        return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch)!="Mn")

    VALID_GRADO={"G3","G2","G1","PRE","PROM","COMP","ROOKIES","TRIATHLON"}
    VALID_CAT={"I","L","M","S","XS","20","30","40","50","60"}
    VALID_EXTRA={"J12","J15","J19","SEN","PA","MST","ESP"}

    GRADO_SYNS = {r"\bG\s*3\b":"G3", r"\bG\s*2\b":"G2", r"\bG\s*1\b":"G1",
                  r"\bGRADO\s*3\b":"G3", r"\bGRADO\s*2\b":"G2", r"\bGRADO\s*1\b":"G1",
                  r"\bPRE\b":"PRE", r"\bPRE\s*AGILITY\b":"PRE", r"\bPREAGILITY\b":"PRE",
                  r"\bPROM\b":"PROM", r"\bPROMO(?!c)":"PROM", r"\bPROMOCION\b":"PROM",
                  r"\bCOMP\b":"COMP", r"\bCOMPET(ICI[OÓ]N|ITION)?\b":"COMP",
                  r"\bROOK(IE|IES)?\b":"ROOKIES",
                  r"\bTRIAT(H?L)ON\b":"TRIATHLON", r"\bTRIATLON\b":"TRIATHLON"}
    CAT_SYNS = {r"\bXS(MALL)?\b":"XS", r"\bX[-\s]?SMALL\b":"XS", r"\bTOY\b":"XS", r"\bEXTRA\s*SMALL\b":"XS",
                r"\bS(MALL)?\b":"S", r"\bM(EDIUM)?\b":"M", r"\bL(ARGE)?\b":"L",
                r"\bI(NTER(MEDIATE)?)?\b":"I", r"\bINTERMED(IO|IA|IATE)\b":"I"}
    EXTRA_SYNS={r"\bJ\s*1\s*2\b":"J12", r"\bJUNIOR\s*12\b":"J12", r"\bJ12\b":"J12",
                r"\bJ\s*1\s*5\b":"J15", r"\bJUNIOR\s*15\b":"J15", r"\bJ15\b":"J15",
                r"\bJ\s*1\s*9\b":"J19", r"\bJUNIOR\s*19\b":"J19", r"\bJ19\b":"J19",
                r"\bSEN(IOR)?\b":"SEN", r"\bPA(RA(GILITY)?)?\b":"PA",
                r"\bM(Á|A)STER\b":"MST", r"\bMST\b":"MST", r"\bESP(ECIAL)?\b":"ESP"}

    def robust_parse_mangas(manga_val, federacion_val):
        grado=None; cat=None; extra=None
        raw = manga_val if isinstance(manga_val,str) else ""
        txt = strip_accents(raw).upper()
        txt = re.sub(r"[|,;]+"," ", txt)
        paren = re.findall(r"\(([^)]+)\)", txt)
        heights = re.findall(r"\b(20|30|40|50|60)\b", txt)
        if heights and heights[0] in VALID_CAT: cat=heights[0]
        if cat is None:
            for pat,canon in CAT_SYNS.items():
                if re.search(pat, txt): cat=canon; break
        for source in [txt]+paren:
            if source is None: continue
            src=str(source)
            for pat,canon in EXTRA_SYNS.items():
                if re.search(pat, src): extra=canon; break
            if extra: break
        for pat,canon in GRADO_SYNS.items():
            if re.search(pat, txt): grado=canon; break
        if grado not in VALID_GRADO: grado=""
        if cat not in VALID_CAT: cat=""
        if extra not in VALID_EXTRA: extra=""
        return grado,cat,extra

    events = pd.read_csv(csv_event, dtype=str).replace({"": pd.NA})
    parts  = pd.read_csv(csv_part,  dtype=str).replace({"": pd.NA})

    pt_cols = ["event_uuid","event_title","BinomID","Dorsal","Guía","Perro","Raza","Edad","Género",
               "Altura (cm)","Licencia","Club","Federación"]
    # normaliza fechas y mangas
    fecha_cols = [c for c in parts.columns if c.startswith("Fecha ")]
    mangas_cols= [c for c in parts.columns if c.startswith("Mangas")]

    # Edad -> años
    import numpy as np
    def edad_to_years_numeric(s):
        if pd.isna(s): return np.nan
        if isinstance(s,(int,float)): return float(s)
        text=str(s).lower().strip().replace(",",".")
        years=0.0; months=0.0
        my = re.search(r"(\d+(?:\.\d+)?)\s*a(?:ño|nios|ños)?", text)
        if my: years=float(my.group(1))
        mm = re.search(r"(\d+(?:\.\d+)?)\s*m(?:es|eses)?", text)
        if mm: months=float(mm.group(1))
        if my or mm: return years + months/12.0
        try: return float(text)
        except Exception: return np.nan

    sel = parts[pt_cols].copy()
    sel["Edad"] = sel["Edad"].apply(edad_to_years_numeric)

    for c in fecha_cols:
        sel[c] = parts[c].apply(to_spanish_dd_mm_yyyy)

    if mangas_cols:
        first_manga = parts[mangas_cols].bfill(axis=1).iloc[:,0]
    else:
        first_manga = pd.Series([pd.NA]*len(parts), index=parts.index)
    fed_series = parts["Federación"] if "Federación" in parts.columns else pd.Series([pd.NA]*len(parts), index=parts.index)

    parsed = [robust_parse_mangas(mv, fv) for mv, fv in zip(first_manga, fed_series)]
    grado, cat, catextra = zip(*parsed) if parsed else ([],[],[])
    sel["Grado"] = list(grado)
    sel["Cat"]   = [str(c) if c is not None else "" for c in cat]
    sel["CatExtra"] = list(catextra)

    ecols = ["uuid","event_url","title","organizer","location","dates"]
    for c in ecols:
        if c not in events.columns: events[c]=pd.NA
    ev_sel = events[ecols].copy()
    ev_sel["dates"] = ev_sel["dates"].apply(to_spanish_dd_mm_yyyy)

    merged = sel.merge(ev_sel, left_on="event_uuid", right_on="uuid", how="left")
    mask_missing = merged["title"].isna() | (merged["title"].astype(str).str.strip().eq("N/D"))
    merged.loc[mask_missing, "title"] = merged.loc[mask_missing, "event_title"].fillna("N/D")

    src_cols = ["event_url","title","organizer","location","dates",
                "BinomID","Dorsal","Guía","Perro","Raza","Edad","Género","Altura (cm)",
                "Licencia","Club","Federación","Grado","Cat","CatExtra"] + fecha_cols
    final = merged.reindex(columns=src_cols, fill_value="").copy()
    RENAME_MAP = {"title":"PruebaNom","organizer":"Organiza","location":"Lugar","dates":"Fechas","Guía":"Guia","Género":"SexoPerro","Altura (cm)":"AlturaPerro","Federación":"Federacion"}
    final.rename(columns=RENAME_MAP, inplace=True)
    target_cols = ["event_url","PruebaNom","Organiza","Lugar","Fechas","BinomID","Dorsal","Guia","Perro","Raza","Edad","SexoPerro","AlturaPerro","Licencia","Club","Federacion","Grado","Cat","CatExtra"] + fecha_cols
    final = final.reindex(columns=target_cols, fill_value="")

    csv_out = os.path.join(OUT_DIR, f"participantes_procesado_{DATE_STR}.csv")
    final.to_csv(csv_out, index=False, encoding="utf-8-sig")

    json_dated  = os.path.join(OUT_DIR, f"participantes_{DATE_STR}.json")
    json_latest = os.path.join(OUT_DIR, "participantes.json")
    final.to_json(json_dated, orient="records", force_ascii=False, indent=2)
    final.to_json(json_latest, orient="records", force_ascii=False, indent=2)

    log(f"03 -> {csv_out}")
    log(f"03 -> {json_latest}  (y fechado)")

# ======================= 04: Unión final =======================
def newest(paths):
    if not paths: return None
    def date_key(p):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(p))
        return m.group(1) if m else "0000-00-00"
    return sorted(paths, key=lambda p: (date_key(p), os.path.getmtime(p)))[-1]

def consolidate_final():
    # Prioridad: participantes.json; si no, participantes_procesado_*.csv
    p_json = sorted(Path(OUT_DIR).glob("participantes.json"))
    rows=[]
    if p_json:
        with open(p_json[-1],"r",encoding="utf-8") as f:
            rows = json.load(f)
    else:
        cand = newest([str(p) for p in Path(OUT_DIR).glob("participantes_procesado_*.csv")])
        if cand:
            with open(cand, newline="", encoding="utf-8-sig") as f:
                r=csv.DictReader(f)
                for row in r:
                    rows.append({k:(v.strip() if isinstance(v,str) else v) for k,v in row.items()})

    out_path = os.path.join(OUT_DIR, "participants_completos_final.json")
    with open(out_path,"w",encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    log(f"04 -> {out_path}")
    return out_path

# ======================= MAIN =======================
def main():
    ap = argparse.ArgumentParser(description="Unificado 01+02+03+04")
    ap.add_argument("--skip-02", action="store_true", help="Saltar enriquecido INFO")
    ap.add_argument("--skip-03", action="store_true", help="Saltar participantes")
    ap.add_argument("--skip-04", action="store_true", help="Saltar unión final")
    args = ap.parse_args()

    _print_effective_config()

    (webdriver, By, Options, WebDriverWait, EC,
     TimeoutException, StaleElementReferenceException, NoSuchElementException,
     ElementClickInterceptedException, JavascriptException, WebDriverException, Service) = _import_selenium()

    driver = _get_driver()
    try:
        _login(driver, By, WebDriverWait, EC)

        # 01
        base_events = collect_events_base(driver, By, WebDriverWait, EC)
        write_01events(base_events)

        # 02
        if not args.skip_02:
            run_02_enrich(driver, By, WebDriverWait, EC, base_events)

        # 03
        if not args.skip_03:
            run_03_participants(driver, By, WebDriverWait, EC, TimeoutException, base_events)

    finally:
        try: driver.quit()
        except Exception: pass
        log("Navegador cerrado")

    # 04
    if not args.skip_04:
        consolidate_final()

if __name__ == "__main__":
    main()
