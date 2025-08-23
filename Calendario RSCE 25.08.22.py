# -*- coding: utf-8 -*-
"""
Created on Mon Jun 16 12:35:21 2025

@author: Juan
C:/xampp/htdocs/Lista2/Proyectos Archivo en FlowRes/23.12.07 RSCE pdf
"""
# In[ ]:
##################################  Console Reset  ##################################################
# Código más seguro para limpiar solo la pantalla de la consola
try:
    from IPython import get_ipython
    shell = get_ipython()
    if shell is not None:
        shell.magic('clear')  # Limpia solo el texto de la consola
except Exception:
    pass # No hagas nada si falla```

# Este fragmento de código solo limpiará la pantalla sin tocar las variables ni el estado interno de la consola, por lo que es completamente seguro.

# En conclusión, te recomiendo **evitar el uso de `shell.magic('reset -f')`** dentro de un script y optar por la configuración nativa de Spyder para limpiar variables, ya que es la solución más robusta y segura.
##################################  Fin Borrado  ##################################################


import os


print("📂 Carpeta actual:", os.getcwd())
# In[]: Carpetas
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  Gestion carpetas ****************************

parent_dir ="C:/JEscos 25.07.07/Agility/Calendario"
os.chdir (parent_dir) # Provide the new path here. Share.



# Creamos la carpeta de Results si no existe
try: 
    os.mkdir("./resultados_agility")
    print('Carpeta Results creada')
except:
    print('Carpeta Results existente')    

# Indica el directorio de trabajo
os.getcwd()  
os.listdir()

contenido = os.listdir(parent_dir)
pathResults = './resultados_agility/'


# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& FIN Gestion Carpetas
print("📂 Carpeta actual:", os.getcwd())
# %%
"""
Script de Scraping para el Calendario de Eventos de Agility de la RSCE

Propósito:
Este script automatiza la extracción de datos de competiciones de Agility del sitio web
de la Real Sociedad Canina de España (RSCE). Su objetivo principal es mantener
un calendario de eventos actualizado, lo que lo hace ideal para ser ejecutado
diariamente a través de un programador de tareas como `crontab` o GitHub Actions.

Funcionalidad:
1. Navegación y Extracción: Utiliza Selenium para cargar dinámicamente el listado
   de eventos, navegando por las páginas y extrayendo la información clave (nombre,
   fechas, URL y ciudad) de cada competición.

2. Geocodificación: Emplea la librería Geopy para convertir los nombres de las
   ciudades en coordenadas geográficas (latitud y longitud). Esto permite la
   posterior visualización en un mapa.

3. Generación de Archivos:
   - **CSV**: Crea un archivo CSV (`eventos_agility_completo.csv`) con todos los
     datos extraídos y sus coordenadas, ideal para su uso en hojas de cálculo
     o bases de datos.
   - **Mapa Interactivo**: Genera un mapa HTML interactivo (`mapa_agility.html`)
     utilizando Folium, mostrando la ubicación de cada evento con marcadores
     que incluyen información detallada.
   - **Índice HTML**: Produce un archivo HTML sencillo (`indice_eventos.html`) que
     actúa como un contenedor para el mapa, facilitando su visualización en un
     navegador o la integración en un sitio web.

Configuración:
Todas las variables de configuración, como URLs, rutas de archivos y límites de
páginas, se gestionan a través del archivo `.env`, lo que permite una
configuración sencilla sin necesidad de modificar el código.

Autor: [Tu Nombre]
Fecha de Creación: [Fecha de creación]
Última Actualización: [Fecha de última modificación]
"""
# -*- coding: utf-8 -*-
"""
Scraper RSCE Agility -> CSV, Mapa Folium (capas por mes/ciudad) y modo Debug.
- Lee configuración desde .env
- Geocodifica con Nominatim + RateLimiter
- Limpia nombres de ciudad para mejorar geocodificación
- Duplica marcadores por capa (sin MarkerCluster)
- Exportación opcional a GeoJSON

Requisitos:
pip install selenium webdriver-manager beautifulsoup4 geopy folium python-dotenv
"""

import os
import csv
import time
import re
import datetime
from dataclasses import dataclass
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# Utilidades de fechas
# =========================
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

DATE_RE = re.compile(
    r"(?P<d>\d{1,2})\s+(?P<m>[A-Za-záéíóúÁÉÍÓÚñÑ]+),?\s+(?P<y>\d{4})"
)

def parse_spanish_date(txt: str) -> Optional[datetime.date]:
    """
    Convierte '22 agosto, 2025' -> date(2025, 8, 22).
    Retorna None si no puede parsear.
    """
    if not txt:
        return None
    m = DATE_RE.search(txt.strip())
    if not m:
        return None
    try:
        d = int(m.group("d"))
        mm = SPANISH_MONTHS.get(m.group("m").lower())
        y = int(m.group("y"))
        if not mm:
            return None
        return datetime.date(y, mm, d)
    except Exception:
        return None


def parse_date_range(inicio_txt: str, fin_txt: str) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
    """
    Devuelve (fecha_inicio, fecha_fin) como date o None.
    """
    di = parse_spanish_date(inicio_txt)
    df = parse_spanish_date(fin_txt)
    return di, df


# =========================
# Scraper
# =========================
@dataclass
class Config:
    URL_BASE: str
    CARPETA_DESTINO: str
    NOMBRE_CSV: str
    SOLO_PRIMERA_PAGINA: bool
    FILTRAR_DESDE_HOY: bool
    ESPERA_CORTA: float = 0.7
    ESPERA_MEDIA: float = 1.2

    @property
    def ruta_csv(self) -> str:
        return os.path.join(self.CARPETA_DESTINO, self.NOMBRE_CSV)


class RSCEAgilityCSV:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        os.makedirs(self.cfg.CARPETA_DESTINO, exist_ok=True)

    # ---------- Selenium ----------
    def _init_driver(self):
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--window-size=1920,1080")
        return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

    def _esperar_listado(self, driver):
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "jet-listing-grid__item"))
        )
        time.sleep(self.cfg.ESPERA_CORTA)

    def _scroll_hasta_el_final(self, driver, pause=1.0, max_reps=2):
        last_h = driver.execute_script("return document.body.scrollHeight")
        reps = 0
        while reps < max_reps:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                reps += 1
            else:
                last_h = new_h
                reps = 0

    def _detectar_total_paginas(self, driver) -> int:
        """
        Busca la paginación y devuelve el mayor número encontrado (>=1).
        Si no hay paginación, devuelve 1.
        """
        try:
            # En la RSCE suelen ser DIVs con clase 'jet-filters-pagination__link'
            elems = driver.find_elements(By.CSS_SELECTOR, ".jet-filters-pagination__link")
            nums = []
            for el in elems:
                txt = (el.text or "").strip()
                if txt.isdigit():
                    nums.append(int(txt))
            total = max(nums) if nums else 1
            print(f"[DEBUG] total_pages detectadas: {total}")
            return max(total, 1)
        except Exception:
            return 1

    def _ir_a_pagina(self, driver, page_num: int) -> bool:
        """
        Navega a una página concreta de la paginación. Devuelve True si cree haberlo conseguido.
        """
        try:
            # La primera página ya está cargada; si piden 1, no tocar nada
            if page_num == 1:
                return True

            # Botón por número exacto
            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    f"//div[contains(@class,'jet-filters-pagination__link') and normalize-space(text())='{page_num}']"
                ))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            self._esperar_listado(driver)
            return True
        except Exception:
            # Reintentar con botón 'Siguiente' las veces necesarias
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, ".jet-filters-pagination__link.next")
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
                driver.execute_script("arguments[0].click();", next_btn)
                self._esperar_listado(driver)
                return True
            except Exception:
                print("    ⚠️ Paginación no disponible o fin de páginas.")
                return False

    # ---------- Parse ----------
    def _extraer_eventos_de_html(self, html: str) -> List[Tuple[str, str, str, str, str]]:
        """
        Devuelve lista de tuplas (nombre, inicio, fin, url, ciudad) de la página actual.
        """
        soup = BeautifulSoup(html, "html.parser")
        bloques = soup.select("div.jet-listing-grid__item")
        eventos = []
        for b in bloques:
            try:
                h2 = b.find("h2")
                if not h2:
                    continue
                a = h2.find("a")
                nombre = h2.get_text(strip=True)
                url = a.get("href", "").strip() if a else ""
                fechas = b.select(".jet-listing-dynamic-field__content")
                inicio = fechas[0].get_text(strip=True) if len(fechas) > 0 else ""
                fin = fechas[1].get_text(strip=True) if len(fechas) > 1 else ""
                lugar = b.select_one(".elementor-icon-box-title span")
                ciudad = lugar.get_text(strip=True) if lugar else ""
                eventos.append((nombre, inicio, fin, url, ciudad))
            except Exception:
                # Silencioso; continuamos con el resto
                pass
        return eventos

    # ---------- Filtro ----------
    def _filtrar_desde_hoy(self, eventos: List[Tuple[str, str, str, str, str]]) -> List[Tuple[str, str, str, str, str]]:
        """
        Mantiene eventos cuyo fin >= hoy, o si fin no existe, cuyo inicio >= hoy.
        """
        if not self.cfg.FILTRAR_DESDE_HOY:
            return eventos
        hoy = datetime.date.today()
        filtrados = []
        for (nombre, inicio, fin, url, ciudad) in eventos:
            di, df = parse_date_range(inicio, fin)
            keep = False
            if df:
                keep = (df >= hoy)
            elif di:
                keep = (di >= hoy)
            else:
                # Si no hay fechas parseables, por defecto los dejamos (o cámbialo a False si prefieres)
                keep = True
            if keep:
                filtrados.append((nombre, inicio, fin, url, ciudad))
        return filtrados

    # ---------- CSV ----------
    def _guardar_csv(self, eventos: List[Tuple[str, str, str, str, str]]) -> None:
        with open(self.cfg.ruta_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Nombre", "Fecha inicio", "Fecha fin", "URL", "Ciudad"])
            w.writerows(eventos)
        print(f"📁 CSV guardado en: {self.cfg.ruta_csv}")

    # ---------- Run ----------
    def run(self):
        print(f"[DEBUG] URL_BASE: {self.cfg.URL_BASE}")
        print(f"[DEBUG] SOLO_PRIMERA_PAGINA = {self.cfg.SOLO_PRIMERA_PAGINA}")
        driver = self._init_driver()
        try:
            driver.get(self.cfg.URL_BASE)
            self._esperar_listado(driver)
            total_pages = self._detectar_total_paginas(driver)

            if self.cfg.SOLO_PRIMERA_PAGINA:
                pages = [1]
            else:
                # Recorremos 1..total_pages
                pages = list(range(1, total_pages + 1))

            eventos_totales: List[Tuple[str, str, str, str, str]] = []
            urls_vistas = set()

            for p in pages:
                ok = self._ir_a_pagina(driver, p)
                if not ok:
                    break
                # Asegurar carga de todo el contenido lazy
                self._scroll_hasta_el_final(driver)
                html = driver.page_source
                eventos = self._extraer_eventos_de_html(html)
                # Evitar duplicados por URL
                nuevos = 0
                for ev in eventos:
                    url = ev[3]
                    if url and url not in urls_vistas:
                        eventos_totales.append(ev)
                        urls_vistas.add(url)
                        nuevos += 1
                print(f"    ➕ {nuevos} eventos en página {p}")

            # Filtrar por fecha (desde hoy)
            eventos_filtrados = self._filtrar_desde_hoy(eventos_totales)

            print(f"🔍 Total eventos recogidos: {len(eventos_totales)} | Tras filtro: {len(eventos_filtrados)}")
            self._guardar_csv(eventos_filtrados)

        finally:
            driver.quit()


# =========================
# Entrypoint
# =========================
def _to_bool(v: Optional[str], default: bool) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "si", "sí")

if __name__ == "__main__":
    # Valores por defecto sensatos
    URL_BASE = os.getenv(
        "URL_BASE",
        # Agility RSCE 2025 a partir del 1 de enero:
        "https://www.rsce.es/eventos-rsce/jsf/jet-engine:eventocuadro/tax/tipos-de-disciplinas:38/meta/fecha-evento!date:2025.1.1-/"
    )
    CARPETA_DESTINO = os.getenv("CARPETA_DESTINO", "./resultados_agility")
    NOMBRE_CSV = os.getenv("NOMBRE_CSV", "eventos_agility_2025.csv")
    SOLO_PRIMERA_PAGINA = _to_bool(os.getenv("SOLO_PRIMERA_PAGINA"), False)
    FILTRAR_DESDE_HOY = _to_bool(os.getenv("FILTRAR_DESDE_HOY"), True)

    os.makedirs(CARPETA_DESTINO, exist_ok=True)

    cfg = Config(
        URL_BASE=URL_BASE,
        CARPETA_DESTINO=CARPETA_DESTINO,
        NOMBRE_CSV=NOMBRE_CSV,
        SOLO_PRIMERA_PAGINA=SOLO_PRIMERA_PAGINA,
        FILTRAR_DESDE_HOY=FILTRAR_DESDE_HOY,
    )
    RSCEAgilityCSV(cfg).run()
