# -*- coding: utf-8 -*-
"""
Scraper RSCE Agility -> CSV + GeoJSON (Lat/Lon + Estado)
- Excluye eventos "Anulado"
- Filtro "desde hoy" (UI y/o postproceso)
- Paginación robusta 1..N (o solo 1)
- Geocodifica ciudades únicas (Nominatim) y añade Lat/Lon
- Versión robusta: espera flexible + debug HTML/screenshot si falla
Requisitos: selenium, webdriver-manager, beautifulsoup4, geopy, python-dotenv (opcional)
"""

import os
import csv
import time
import re
import datetime
import json
import pathlib
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# Geocoding
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# .env (opcional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# Fechas (ES)
# =========================
SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-záéíóúñ]+),?\s+(\d{4})")


def parse_spanish_date(txt: str) -> Optional[datetime.date]:
    if not txt:
        return None

    t = txt.strip().lower().replace(" de ", " ")
    m = DATE_RE.search(t)

    if not m:
        return None

    try:
        d = int(m.group(1))
        mm = m.group(2)
        y = int(m.group(3))
        mnum = SPANISH_MONTHS.get(mm)

        return datetime.date(y, mnum, d) if mnum else None

    except Exception:
        return None


def parse_date_range(inicio: str, fin: str) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
    return parse_spanish_date(inicio), parse_spanish_date(fin)


# =========================
# Scraper
# =========================
class RSCEAgilityExporter:
    def __init__(self):
        self.URL_BASE = os.getenv(
            "URL_BASE",
            "https://www.rsce.es/eventos-rsce/jsf/jet-engine:eventocuadro/tax/tipos-de-disciplinas:38/meta/fecha-evento!date:2026.1.1-/",
        )

        self.OUTDIR = os.getenv("CARPETA_DESTINO", "./resultados_agility")
        os.makedirs(self.OUTDIR, exist_ok=True)

        self.OUTCSV = os.path.join(
            self.OUTDIR,
            os.getenv("NOMBRE_CSV", "eventos_agility_2026.csv"),
        )

        self.OUTGEO = os.path.join(
            self.OUTDIR,
            os.getenv("NOMBRE_GEOJSON", "eventos_agility_2026.geojson"),
        )

        self.SOLO_PRIMERA = self._to_bool(os.getenv("SOLO_PRIMERA_PAGINA"), False)
        self.MAX_PAGINAS = int(os.getenv("MAX_PAGINAS", "50"))

        # Importante:
        # Si la web RSCE cambia y el filtro UI falla, no rompemos el script.
        # Luego se aplica el filtrado en postproceso.
        self.APLICAR_FILTRO_UI = self._to_bool(os.getenv("APLICAR_FILTRO_UI"), True)
        self.FILTRAR_DESDE_HOY = self._to_bool(os.getenv("FILTRAR_DESDE_HOY"), True)

        self.GEOCODIFICAR = self._to_bool(os.getenv("GEOCODIFICAR"), True)

        self.DEBUG_DIR = pathlib.Path("debug_rsce")
        self.DEBUG_DIR.mkdir(exist_ok=True)

    @staticmethod
    def _to_bool(v, default=False):
        if v is None:
            return default
        return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "si", "sí")

    # ---------- Selenium ----------
    def _init_driver(self):
        opts = Options()

        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1600,1200")
        opts.add_argument("--lang=es-ES,es")

        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        d = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=opts,
        )

        d.set_page_load_timeout(90)
        return d

    def _guardar_debug(self, d, nombre_base: str):
        """
        Guarda HTML y screenshot para poder ver qué ha visto Selenium en GitHub Actions.
        """
        self.DEBUG_DIR.mkdir(exist_ok=True)

        try:
            html_path = self.DEBUG_DIR / f"{nombre_base}.html"
            html_path.write_text(d.page_source, encoding="utf-8")
            print(f"📄 Debug HTML guardado en: {html_path}")
        except Exception as e:
            print(f"⚠️ No se pudo guardar HTML debug: {e}")

        try:
            png_path = self.DEBUG_DIR / f"{nombre_base}.png"
            d.save_screenshot(str(png_path))
            print(f"📸 Debug screenshot guardado en: {png_path}")
        except Exception as e:
            print(f"⚠️ No se pudo guardar screenshot debug: {e}")

    def _aceptar_cookies_si_aparece(self, d):
        """
        Intenta cerrar/aceptar cookies si aparece algún banner.
        No rompe si no existe.
        """
        posibles_xpath = [
            "//button[contains(translate(., 'ACEPTAR', 'aceptar'), 'aceptar')]",
            "//a[contains(translate(., 'ACEPTAR', 'aceptar'), 'aceptar')]",
            "//button[contains(translate(., 'ALLOW', 'allow'), 'allow')]",
            "//button[contains(translate(., 'OK', 'ok'), 'ok')]",
        ]

        for xp in posibles_xpath:
            try:
                btn = WebDriverWait(d, 3).until(
                    EC.element_to_be_clickable((By.XPATH, xp))
                )
                d.execute_script("arguments[0].click();", btn)
                print("[DEBUG] Banner/cookies cerrado")
                time.sleep(1)
                return
            except Exception:
                pass

    def _esperar_listado(self, d):
        """
        Antes el script esperaba obligatoriamente:
            class='jet-listing-grid__item'

        Eso es frágil. Ahora espera señales alternativas:
        - bloques JetEngine
        - artículos
        - enlaces a eventos
        - textos reales como Agility / Prueba de Agility / Leer más
        """
        try:
            WebDriverWait(d, 90).until(
                lambda driver: (
                    len(driver.find_elements(By.CSS_SELECTOR, "div.jet-listing-grid__item")) > 0
                    or len(driver.find_elements(By.CSS_SELECTOR, "article")) > 0
                    or len(driver.find_elements(By.CSS_SELECTOR, "h2 a")) > 0
                    or len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/eventos-rsce/']")) > 0
                    or "Prueba de Agility" in driver.page_source
                    or "Agility" in driver.page_source
                    or "Leer más" in driver.page_source
                )
            )

            time.sleep(2)
            print("✅ Listado de eventos detectado")
            return True

        except TimeoutException:
            print("❌ Timeout esperando listado de eventos RSCE")
            self._guardar_debug(d, "rsce_timeout_listado")
            raise

    def _scroll_hasta_el_final(self, d, rounds=4, pause=1.2):
        for _ in range(rounds):
            d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause)

    def _aplicar_filtro_desde_hoy_ui(self, d):
        """
        Rellena 'Desde' con hoy y 'Hasta' vacío. Pulsa 'Ordenar' si existe.
        Si falla, no rompe: el filtrado desde hoy se hace también en postproceso.
        """
        try:
            hoy = datetime.date.today().strftime("%d/%m/%Y")

            desde = d.find_element(By.CSS_SELECTOR, "input.jet-date-range__from")
            hasta = d.find_element(By.CSS_SELECTOR, "input.jet-date-range__to")

            d.execute_script(
                """
                arguments[0].value = arguments[2];
                arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
                arguments[0].dispatchEvent(new Event('change', {bubbles:true}));

                arguments[1].value = '';
                arguments[1].dispatchEvent(new Event('input', {bubbles:true}));
                arguments[1].dispatchEvent(new Event('change', {bubbles:true}));
                """,
                desde,
                hasta,
                hoy,
            )

            try:
                ordenar = d.find_element(
                    By.XPATH,
                    "//button[normalize-space(.)='Ordenar'] | //input[@value='Ordenar']",
                )
                d.execute_script("arguments[0].click();", ordenar)
                time.sleep(3)
            except Exception:
                pass

            self._esperar_listado(d)
            print("[DEBUG] Filtro UI 'Desde=hoy' aplicado")

        except Exception as e:
            print(f"[WARN] No se pudo aplicar filtro UI. Continúo con filtrado postproceso: {e}")

    def _detectar_total_paginas(self, d) -> int:
        try:
            elems = d.find_elements(By.CSS_SELECTOR, ".jet-filters-pagination__link")
            nums = []

            for el in elems:
                txt = (el.text or "").strip()
                if txt.isdigit():
                    nums.append(int(txt))

            total = max(nums) if nums else 1
            total = max(1, min(total, self.MAX_PAGINAS))

            print(f"[DEBUG] total_pages detectadas: {total}")
            return total

        except Exception:
            return 1

    def _ir_a_pagina(self, d, page_num: int) -> bool:
        try:
            if page_num == 1:
                return True

            btn = WebDriverWait(d, 8).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        f"//div[contains(@class,'jet-filters-pagination__link') and normalize-space(text())='{page_num}']",
                    )
                )
            )

            d.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.5)
            d.execute_script("arguments[0].click();", btn)

            self._esperar_listado(d)
            return True

        except Exception:
            try:
                nxt = d.find_element(By.CSS_SELECTOR, ".jet-filters-pagination__link.next")
                d.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
                time.sleep(0.5)
                d.execute_script("arguments[0].click();", nxt)

                self._esperar_listado(d)
                return True

            except Exception:
                print("    ⚠️ Paginación no disponible o fin de páginas.")
                return False

    # ---------- Extracción ----------
    def _texto_limpio(self, value: str) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()

    def _extraer_ciudad_desde_texto(self, txt: str) -> str:
        """
        Fallback muy conservador. Intenta detectar ciudad cuando no aparece
        el selector original de Elementor.
        """
        if not txt:
            return ""

        txt = self._texto_limpio(txt)

        patrones = [
            r"Lugar\s*:?\s*([^|]+)",
            r"Localidad\s*:?\s*([^|]+)",
            r"Ciudad\s*:?\s*([^|]+)",
        ]

        for p in patrones:
            m = re.search(p, txt, flags=re.I)
            if m:
                return self._texto_limpio(m.group(1))[:120]

        return ""

    def _extraer_fechas_desde_texto(self, txt: str) -> Tuple[str, str]:
        """
        Fallback por si cambian las clases de JetEngine.
        Busca fechas tipo '15 enero 2026'.
        """
        if not txt:
            return "", ""

        fechas = DATE_RE.findall(txt.lower().replace(" de ", " "))

        fechas_txt = []
        for d, mes, y in fechas:
            fechas_txt.append(f"{int(d)} {mes}, {y}")

        if len(fechas_txt) >= 2:
            return fechas_txt[0], fechas_txt[1]

        if len(fechas_txt) == 1:
            return fechas_txt[0], fechas_txt[0]

        return "", ""

    def _extraer_eventos(self, html: str) -> List[Tuple[str, str, str, str, str, str]]:
        """
        Devuelve lista:
        (nombre, inicio, fin, url, ciudad, estado) con estado='Anulado' o 'Activo'

        Mantiene el extractor original y añade fallback si RSCE cambia estructura.
        """
        soup = BeautifulSoup(html, "html.parser")
        eventos = []

        # Extractor principal: estructura original JetEngine
        bloques = soup.select("div.jet-listing-grid__item")

        # Fallback: si no hay bloques JetEngine, probamos otros contenedores comunes.
        if not bloques:
            bloques = soup.select("article, .elementor-widget-container, .jet-listing-grid, .jet-listing")

        print(f"[DEBUG] Bloques candidatos encontrados: {len(bloques)}")

        for b in bloques:
            h2 = b.find("h2")
            a = h2.find("a") if h2 else None

            # Fallback si el título no está en h2
            if not a:
                a = b.select_one("a[href*='/eventos-rsce/']") or b.select_one("a[href*='rsce.es']")

            if h2:
                nombre = self._texto_limpio(h2.get_text(" ", strip=True))
            elif a:
                nombre = self._texto_limpio(a.get_text(" ", strip=True))
            else:
                nombre = ""

            url = a.get("href", "").strip() if a else ""

            texto_bloque = self._texto_limpio(b.get_text(" ", strip=True))

            # Evita bloques genéricos enormes que no sean un evento.
            if not nombre and "Agility" not in texto_bloque:
                continue

            # Si el nombre está vacío pero el bloque menciona agility, intentamos construir algo mínimo.
            if not nombre and "Agility" in texto_bloque:
                nombre = texto_bloque[:120]

            # Fechas: selector original
            fechas = b.select(".jet-listing-dynamic-field__content")
            inicio = fechas[0].get_text(strip=True) if len(fechas) > 0 else ""
            fin = fechas[1].get_text(strip=True) if len(fechas) > 1 else ""

            # Fechas: fallback por texto
            if not inicio and not fin:
                inicio, fin = self._extraer_fechas_desde_texto(texto_bloque)

            # Ciudad: selector original
            lugar = b.select_one(".elementor-icon-box-title span")
            ciudad = lugar.get_text(strip=True) if lugar else ""

            # Ciudad: fallback
            if not ciudad:
                ciudad = self._extraer_ciudad_desde_texto(texto_bloque)

            # Estado
            badge = b.select_one("span.jet-listing-dynamic-terms__link")
            estado_txt = badge.get_text(" ", strip=True).lower() if badge else texto_bloque.lower()
            estado = "Anulado" if "anulado" in estado_txt else "Activo"

            # Filtro mínimo para no meter enlaces vacíos o navegación
            nombre_lower = nombre.lower()
            if (
                nombre
                and (
                    "agility" in nombre_lower
                    or "agility" in texto_bloque.lower()
                    or "prueba" in nombre_lower
                )
            ):
                eventos.append((nombre, inicio, fin, url, ciudad, estado))

        # Deduplicado suave por URL/nombre/inicio
        dedup = []
        seen = set()

        for ev in eventos:
            nombre, inicio, fin, url, ciudad, estado = ev
            key = url or f"{nombre}|{inicio}|{ciudad}"
            if key in seen:
                continue
            seen.add(key)
            dedup.append(ev)

        print("      Ejemplos:", [f"{e[5]} · {e[0][:48]}" for e in dedup[:3]])
        return dedup

    # ---------- Filtros ----------
    def _filtrar_eventos(self, eventos):
        """
        1) Excluye ANULADOS.
        2) Fecha desde hoy: fin >= hoy o, si no hay fin, inicio >= hoy.
           Si no se parsea la fecha, conserva.
        """
        hoy = datetime.date.today()
        out = []

        for (n, i, f, u, c, estado) in eventos:
            if estado.lower() == "anulado":
                continue

            if not self.FILTRAR_DESDE_HOY:
                out.append((n, i, f, u, c, estado))
                continue

            di, df = parse_date_range(i, f)

            keep = (
                (df and df >= hoy)
                or (df is None and di and di >= hoy)
                or (di is None and df is None)
            )

            if keep:
                out.append((n, i, f, u, c, estado))

        return out

    # ---------- Geocoding ----------
    def _geocode_ciudades(self, eventos):
        """
        eventos: (n,i,f,u,c,estado) activos.
        Devuelve dict ciudad -> (lat, lon). Si GEOCODIFICAR=False, devuelve {}.
        """
        cache = {}

        if not self.GEOCODIFICAR:
            return cache

        geolocator = Nominatim(user_agent="agility-mapper-rsce/1.0", timeout=10)

        geocode = RateLimiter(
            geolocator.geocode,
            min_delay_seconds=1,
            max_retries=2,
            error_wait_seconds=2,
            swallow_exceptions=True,
        )

        for *_, c, _estado in eventos:
            if c and c not in cache:
                q = f"{c}, España"
                loc = geocode(q)
                cache[c] = (loc.latitude, loc.longitude) if loc else (None, None)

        return cache

    # ---------- CSV ----------
    def _guardar_csv(self, eventos):
        """
        eventos: (n,i,f,u,c,estado) activos.
        """
        cache = self._geocode_ciudades(eventos)

        with open(self.OUTCSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)

            w.writerow(
                [
                    "Nombre",
                    "Fecha inicio",
                    "Fecha fin",
                    "URL",
                    "Ciudad",
                    "Estado",
                    "Latitud",
                    "Longitud",
                ]
            )

            for n, i, f_fin, u, c, estado in eventos:
                lat, lon = cache.get(c, (None, None)) if c else (None, None)
                w.writerow([n, i, f_fin, u, c, estado, lat, lon])

        print(f"📁 CSV guardado en: {self.OUTCSV} con {len(eventos)} eventos")

    # ---------- GeoJSON ----------
    def _guardar_geojson(self, eventos):
        """
        Genera GeoJSON con puntos (lon,lat) solo para filas con coordenadas válidas.
        """
        cache = self._geocode_ciudades(eventos)

        feats = []

        for n, i, f_fin, u, c, estado in eventos:
            lat, lon = cache.get(c, (None, None)) if c else (None, None)

            try:
                if lat is not None and lon is not None:
                    feats.append(
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [float(lon), float(lat)],
                            },
                            "properties": {
                                "nombre": n,
                                "inicio": i,
                                "fin": f_fin,
                                "ciudad": c,
                                "estado": estado,
                                "url": u,
                            },
                        }
                    )
            except Exception:
                pass

        fc = {"type": "FeatureCollection", "features": feats}

        with open(self.OUTGEO, "w", encoding="utf-8") as f:
            json.dump(fc, f, ensure_ascii=False, indent=2)

        print(f"🧭 GeoJSON guardado en: {self.OUTGEO} ({len(feats)} features)")

    def _descargar_html_directo(self) -> str:
        """
        Descarga directa del HTML sin Selenium.
        La página de RSCE está devolviendo los eventos en el HTML,
        así que esto evita los timeouts del navegador headless.
        """
        import requests

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        }

        print("[DEBUG] Descargando HTML directo con requests...")
        r = requests.get(self.URL_BASE, headers=headers, timeout=60)
        r.raise_for_status()

        html = r.text

        self.DEBUG_DIR.mkdir(exist_ok=True)
        (self.DEBUG_DIR / "rsce_requests.html").write_text(html, encoding="utf-8")

        print(f"[DEBUG] HTML directo descargado: {len(html)} caracteres")
        return html

    def _extraer_eventos_html_directo(self, html: str):
        """
        Extractor específico para el HTML actual de RSCE.
        Busca bloques por títulos h2 que enlazan a eventos y extrae:
        nombre, inicio, fin, url, ciudad, estado.
        """
        soup = BeautifulSoup(html, "html.parser")
        eventos = []

        enlaces = soup.select("h2 a")

        print(f"[DEBUG] h2 a encontrados: {len(enlaces)}")

        for a in enlaces:
            nombre = self._texto_limpio(a.get_text(" ", strip=True))
            url = a.get("href", "").strip()

            if not nombre:
                continue

            txt_nombre = nombre.lower()

            if "agility" not in txt_nombre:
                continue

            # Buscamos el bloque padre más cercano con texto suficiente.
            bloque = a
            for _ in range(8):
                if bloque.parent:
                    bloque = bloque.parent
                    texto = self._texto_limpio(bloque.get_text(" ", strip=True))
                    if "leer más" in texto.lower() and len(texto) > len(nombre) + 20:
                        break

            texto = self._texto_limpio(bloque.get_text(" ", strip=True))

            estado = "Anulado" if "anulado" in texto.lower() else "Activo"

            fechas = DATE_RE.findall(texto.lower().replace(" de ", " "))
            fechas_txt = []

            for d, mes, y in fechas:
                fechas_txt.append(f"{int(d)} {mes}, {y}")

            inicio = fechas_txt[0] if len(fechas_txt) >= 1 else ""
            fin = fechas_txt[1] if len(fechas_txt) >= 2 else inicio

            ciudad = ""

            # En la web suele aparecer como un h3 después de las fechas.
            h3 = bloque.find("h3")
            if h3:
                ciudad = self._texto_limpio(h3.get_text(" ", strip=True))

            # Fallback: intenta sacar ciudad después de las fechas.
            if not ciudad and fechas_txt:
                partes = texto.split(fechas_txt[-1])
                if len(partes) > 1:
                    resto = partes[1]
                    resto = resto.replace("Leer más", "")
                    resto = self._texto_limpio(resto)
                    ciudad = resto[:120]

            eventos.append((nombre, inicio, fin, url, ciudad, estado))

        # Deduplicado
        dedup = []
        seen = set()

        for ev in eventos:
            nombre, inicio, fin, url, ciudad, estado = ev
            key = url or f"{nombre}|{inicio}|{ciudad}"

            if key in seen:
                continue

            seen.add(key)
            dedup.append(ev)

        print("      Ejemplos directo:", [f"{e[5]} · {e[0][:48]}" for e in dedup[:3]])
        return dedup

    
    # ---------- Run ----------
    def run(self):
        print(f"[DEBUG] URL_BASE: {self.URL_BASE}")
        print(
            f"[DEBUG] SOLO_PRIMERA={self.SOLO_PRIMERA} | "
            f"APLICAR_FILTRO_UI={self.APLICAR_FILTRO_UI} | "
            f"FILTRAR_DESDE_HOY={self.FILTRAR_DESDE_HOY} | "
            f"GEOCODIFICAR={self.GEOCODIFICAR}"
        )

        eventos_totales = []

        # =====================================================
        # 1) Intento principal: descarga directa sin Selenium
        # =====================================================
        try:
            html = self._descargar_html_directo()
            eventos_totales = self._extraer_eventos_html_directo(html)
            print(f"🔍 Total brutos por HTML directo: {len(eventos_totales)}")

        except Exception as e:
            print(f"⚠️ Falló extracción directa con requests: {e}")
            eventos_totales = []

        # =====================================================
        # 2) Fallback: Selenium, solo si la extracción directa falla
        # =====================================================
        if len(eventos_totales) == 0:
            print("⚠️ Sin eventos por HTML directo. Probando Selenium...")

            d = None

            try:
                d = self._init_driver()
                d.get(self.URL_BASE)

                time.sleep(3)
                self._aceptar_cookies_si_aparece(d)

                self._esperar_listado(d)

                if self.APLICAR_FILTRO_UI:
                    self._aplicar_filtro_desde_hoy_ui(d)

                total_pages = self._detectar_total_paginas(d)
                pages = [1] if self.SOLO_PRIMERA else list(range(1, total_pages + 1))

                seen_urls = set()

                for p in pages:
                    ok = self._ir_a_pagina(d, p)

                    if not ok:
                        break

                    self._scroll_hasta_el_final(d)

                    eventos = self._extraer_eventos(d.page_source)

                    nuevos = 0

                    for ev in eventos:
                        url = ev[3]
                        key = url or f"{ev[0]}|{ev[1]}|{ev[4]}"

                        if key and key not in seen_urls:
                            eventos_totales.append(ev)
                            seen_urls.add(key)
                            nuevos += 1

                    print(f"    ➕ {nuevos} nuevos en página {p}")

            finally:
                if d is not None:
                    try:
                        d.quit()
                    except Exception:
                        pass

        print(f"🔍 Total brutos final: {len(eventos_totales)}")

        if len(eventos_totales) == 0:
            raise RuntimeError("No se ha extraído ningún evento de RSCE")

        eventos_final = self._filtrar_eventos(eventos_totales)
        print(f"🔍 Tras filtros estado/fecha: {len(eventos_final)}")

        self._guardar_csv(eventos_final)
        self._guardar_geojson(eventos_final)


if __name__ == "__main__":
    RSCEAgilityExporter().run()
