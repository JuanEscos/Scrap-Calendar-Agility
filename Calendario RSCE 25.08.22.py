import os, csv, time, re, datetime
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

# ====== Fechas en español ======
SPANISH_MONTHS = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,
    "noviembre":11,"diciembre":12
}
DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-záéíóúñ]+),?\s+(\d{4})")

def parse_spanish_date(txt:str)->Optional[datetime.date]:
    if not txt: return None
    t = txt.strip().lower().replace(" de ", " ")
    m = DATE_RE.search(t)
    if not m: return None
    try:
        d, mm, y = int(m.group(1)), m.group(2), int(m.group(3))
        mnum = SPANISH_MONTHS.get(mm)
        return datetime.date(y, mnum, d) if mnum else None
    except: return None

def parse_date_range(inicio:str, fin:str)->Tuple[Optional[datetime.date], Optional[datetime.date]]:
    return parse_spanish_date(inicio), parse_spanish_date(fin)

# ====== Scraper reducido (CSV + Lat/Lon) ======
class RSCEAgilityCSV:
    def __init__(self):
        self.URL_BASE = os.getenv("URL_BASE",
            "https://www.rsce.es/eventos-rsce/jsf/jet-engine:eventocuadro/tax/tipos-de-disciplinas:38/meta/fecha-evento!date:2025.1.1-/"
        )
        self.OUTDIR   = os.getenv("CARPETA_DESTINO","./resultados_agility")
        self.OUTCSV   = os.path.join(self.OUTDIR, os.getenv("NOMBRE_CSV","eventos_agility_2025.csv"))
        self.SOLO_PRIMERA = os.getenv("SOLO_PRIMERA_PAGINA","false").lower() in ("1","true","si","sí","y")
        self.MAX_PAGINAS  = int(os.getenv("MAX_PAGINAS", "50"))
        self.APLICAR_FILTRO_UI = os.getenv("APLICAR_FILTRO_UI","true").lower() in ("1","true","si","sí","y")
        self.FILTRAR_DESDE_HOY = os.getenv("FILTRAR_DESDE_HOY","true").lower() in ("1","true","si","sí","y")

        os.makedirs(self.OUTDIR, exist_ok=True)

    # ---------- Selenium ----------
    def _init_driver(self):
        opts=Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox")
        opts.add_argument("--window-size=1600,1000")
        return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

    def _esperar_listado(self,d):
        WebDriverWait(d, 20).until(EC.presence_of_element_located((By.CLASS_NAME,"jet-listing-grid__item")))
        time.sleep(0.7)

    def _scroll_hasta_el_final(self,d, rounds=3, pause=1.0):
        for _ in range(rounds):
            d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause)

    def _aplicar_filtro_desde_hoy_ui(self, d):
        """Rellena inputs 'Desde' = hoy, 'Hasta' = vacío y pulsa Ordenar (si existe)."""
        try:
            hoy = datetime.date.today().strftime("%d/%m/%Y")
            desde = d.find_element(By.CSS_SELECTOR, "input.jet-date-range__from")
            hasta = d.find_element(By.CSS_SELECTOR, "input.jet-date-range__to")
            d.execute_script("""
                arguments[0].value = arguments[2];
                arguments[0].dispatchEvent(new Event('input')); arguments[0].dispatchEvent(new Event('change'));
                arguments[1].value = '';
                arguments[1].dispatchEvent(new Event('input')); arguments[1].dispatchEvent(new Event('change'));
            """, desde, hasta, hoy)
            # botón Ordenar si lo hay
            try:
                ordenar = d.find_element(By.XPATH, "//button[normalize-space(.)='Ordenar'] | //input[@value='Ordenar']")
                d.execute_script("arguments[0].click();", ordenar)
            except Exception:
                pass
            self._esperar_listado(d)
            print("[DEBUG] Filtro UI 'Desde=hoy' aplicado")
        except Exception as e:
            print(f"[WARN] No se pudo aplicar filtro UI: {e}")

    def _detectar_total_paginas(self,d)->int:
        try:
            elems = d.find_elements(By.CSS_SELECTOR, ".jet-filters-pagination__link")
            nums=[]
            for el in elems:
                txt=(el.text or "").strip()
                if txt.isdigit(): nums.append(int(txt))
            total = max(nums) if nums else 1
            print(f"[DEBUG] total_pages detectadas: {total}")
            return max(1, min(total, self.MAX_PAGINAS))
        except Exception:
            return 1

    def _ir_a_pagina(self,d, page_num:int)->bool:
        try:
            if page_num==1: return True
            btn = WebDriverWait(d, 5).until(
                EC.element_to_be_clickable((By.XPATH, f"//div[contains(@class,'jet-filters-pagination__link') and normalize-space(text())='{page_num}']"))
            )
            d.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            d.execute_script("arguments[0].click();", btn)
            self._esperar_listado(d)
            return True
        except Exception:
            try:
                nxt = d.find_element(By.CSS_SELECTOR, ".jet-filters-pagination__link.next")
                d.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
                d.execute_script("arguments[0].click();", nxt)
                self._esperar_listado(d)
                return True
            except Exception:
                print("    ⚠️ Paginación no disponible o fin de páginas.")
                return False

    # ---------- Parse ----------
    def _extraer_eventos(self, html)->List[Tuple[str,str,str,str,str]]:
        soup=BeautifulSoup(html,"html.parser")
        bloques = soup.select("div.jet-listing-grid__item")
        eventos=[]
        for b in bloques:
            h2=b.find("h2")
            if not h2: continue
            a=h2.find("a")
            nombre=h2.get_text(strip=True) if h2 else ""
            url=a.get("href","").strip() if a else ""
            fechas=b.select(".jet-listing-dynamic-field__content")
            inicio=fechas[0].get_text(strip=True) if len(fechas)>0 else ""
            fin   =fechas[1].get_text(strip=True) if len(fechas)>1 else ""
            lugar =b.select_one(".elementor-icon-box-title span")
            ciudad=lugar.get_text(strip=True) if lugar else ""
            if nombre or url:
                eventos.append((nombre, inicio, fin, url, ciudad))
        # Debug: muestra los tres primeros títulos
        print("      Ejemplos:", [e[0][:50] for e in eventos[:3]])
        return eventos

    # ---------- Filtro por fecha (segunda capa de seguridad) ----------
    def _filtrar_desde_hoy_post(self, eventos):
        if not self.FILTRAR_DESDE_HOY:
            return eventos
        hoy = datetime.date.today()
        out=[]
        for (n,i,f,u,c) in eventos:
            di, df = parse_date_range(i,f)
            keep = (df and df>=hoy) or (df is None and di and di>=hoy) or (di is None and df is None)  # si no parsea, mantener
            if keep: out.append((n,i,f,u,c))
        return out

    # ---------- Geocoding ----------
    def _geocode_ciudades(self, eventos):
        geolocator=Nominatim(user_agent="agility-mapper-rsce/1.0", timeout=10)
        geocode=RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2, error_wait_seconds=2, swallow_exceptions=True)
        cache={}
        for *_, ciudad in eventos:
            if ciudad and ciudad not in cache:
                q = f"{ciudad}, España"
                loc = geocode(q)
                cache[ciudad] = (loc.latitude, loc.longitude) if loc else (None, None)
        return cache

    # ---------- CSV ----------
    def _guardar_csv(self, eventos):
        cache = self._geocode_ciudades(eventos)
        with open(self.OUTCSV,"w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["Nombre","Fecha inicio","Fecha fin","URL","Ciudad","Latitud","Longitud"])
            for n,i,f,u,c in eventos:
                lat,lon = cache.get(c,(None,None)) if c else (None,None)
                w.writerow([n,i,f,u,c,lat,lon])
        print(f"📁 CSV guardado en {self.OUTCSV} con {len(eventos)} eventos")

    # ---------- Run ----------
    def run(self):
        print(f"[DEBUG] URL_BASE: {self.URL_BASE}")
        print(f"[DEBUG] SOLO_PRIMERA_PAGINA={self.SOLO_PRIMERA} | APLICAR_FILTRO_UI={self.APLICAR_FILTRO_UI} | FILTRAR_DESDE_HOY={self.FILTRAR_DESDE_HOY}")

        d=self._init_driver()
        d.get(self.URL_BASE)
        self._esperar_listado(d)

        if self.APLICAR_FILTRO_UI:
            self._aplicar_filtro_desde_hoy_ui(d)

        total_pages = self._detectar_total_paginas(d)
        pages = [1] if self.SOLO_PRIMERA else list(range(1, total_pages+1))

        eventos_totales=[]; seen=set()
        for p in pages:
            ok = self._ir_a_pagina(d, p)
            if not ok: break
            self._scroll_hasta_el_final(d, rounds=3)
            html=d.page_source
            eventos=self._extraer_eventos(html)

            nuevos=0
            for ev in eventos:
                if ev[3] and ev[3] not in seen:
                    eventos_totales.append(ev); seen.add(ev[3]); nuevos+=1
            print(f"    ➕ {nuevos} nuevos en página {p}")

        d.quit()
        print(f"🔍 Total brutos: {len(eventos_totales)}")

        eventos_filtrados = self._filtrar_desde_hoy_post(eventos_totales)
        print(f"🔍 Tras filtro fecha: {len(eventos_filtrados)}")

        self._guardar_csv(eventos_filtrados)

if __name__=="__main__":
    RSCEAgilityCSV().run()

