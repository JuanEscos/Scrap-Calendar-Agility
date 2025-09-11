#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FlowAgility Scraper - Sistema completo de extracción y procesamiento de datos
"""

import os
import sys
import json
import csv
import re
import time
import argparse
import traceback
import unicodedata
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin
from pathlib import Path
from glob import glob

# Third-party imports
import pandas as pd
import numpy as np
from dateutil import parser
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Selenium imports (conditional)
try:
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
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# ============================== CONFIGURACIÓN GLOBAL ==============================

# Configuración base
BASE = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"
SCRIPT_DIR = Path(__file__).resolve().parent

# Cargar variables de entorno
load_dotenv(SCRIPT_DIR / ".env")

# Credenciales (OBLIGATORIAS)
FLOW_EMAIL = os.getenv("FLOW_EMAIL", "pilar1959suarez@gmail.com")
FLOW_PASS = os.getenv("FLOW_PASS", "Seattle1")

# Flags/tunables con valores por defecto seguros
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "10"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "2.0"))
CLICK_RETRIES = int(os.getenv("CLICK_RETRIES", "3"))
PER_PART_TIMEOUT_S = float(os.getenv("PER_PART_TIMEOUT_S", "10"))
RENDER_POLL_S = float(os.getenv("RENDER_POLL_S", "0.25"))
MAX_EVENT_SECONDS = int(os.getenv("MAX_EVENT_SECONDS", "1200"))
RESUME = os.getenv("RESUME", "true").lower() == "true"
SLOW_MIN_S = float(os.getenv("SLOW_MIN_S", "1.0"))
SLOW_MAX_S = float(os.getenv("SLOW_MAX_S", "3.0"))
OUT_DIR = os.getenv("OUT_DIR", "./output")

# Expresiones regulares
UUID_RE = re.compile(r"/zone/events/([0-9a-fA-F-]{36})(?:/.*)?$")
EMOJI_RE = re.compile(
    "[\U0001F1E6-\U0001F1FF\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+"
)

# ============================== UTILIDADES GENERALES ==============================

def log(message):
    """Función de logging"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=None, max_s=None):
    """Pausa aleatoria para no saturar; usa .env si no se indica."""
    a = SLOW_MIN_S if min_s is None else float(min_s)
    b = SLOW_MAX_S if max_s is None else float(max_s)
    if b < a: 
        a, b = b, a
    time.sleep(random.uniform(a, b))

def next_free_path(path: str) -> str:
    """Si path existe, devuelve path con sufijo _v2, _v3, … libre."""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 2
    while True:
        cand = f"{base}_v{i}{ext}"
        if not os.path.exists(cand):
            return cand
        i += 1

def _clean(s: str) -> str:
    """Limpia y normaliza texto"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = EMOJI_RE.sub("", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:;")

def _ensure_csv_header(path, header):
    """Asegura que el CSV tenga cabecera"""
    exist = os.path.exists(path)
    if not exist:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()

def _append_csv_row(path, header, row_dict):
    """Añade una fila a CSV asegurando cabecera"""
    _ensure_csv_header(path, header)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writerow({k: row_dict.get(k, "") for k in header})

# ============================== FUNCIONES DE NAVEGACIÓN MEJORADAS ==============================

def _get_driver(headless=True):
    """Crea y configura el driver de Selenium"""
    if not HAS_SELENIUM:
        raise ImportError("Selenium no está instalado")
    
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    if INCOGNITO:
        opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--remote-debugging-port=0")  # Puerto aleatorio
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # Configuración adicional para mejorar estabilidad
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-default-apps")
    
    try:
        if HAS_WEBDRIVER_MANAGER:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
        else:
            log("webdriver_manager no instalado, usando ChromeDriver del sistema")
            driver = webdriver.Chrome(options=opts)
        
        # Configurar timeouts
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        return driver
        
    except Exception as e:
        log(f"Error creando driver: {e}")
        # Fallback sin service
        return webdriver.Chrome(options=opts)

def _save_screenshot(driver, name):
    """Guarda screenshot del navegador"""
    try:
        path = os.path.join(OUT_DIR, name)
        driver.save_screenshot(path)
        log(f"Screenshot -> {path}")
    except Exception:
        pass

def _accept_cookies(driver):
    """Acepta cookies si es necesario"""
    try:
        for sel in (
            '[data-testid="uc-accept-all-button"]',
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            'button[mode="primary"]',
            'button:contains("Aceptar")',
            'button:contains("Accept")',
        ):
            try:
                btns = driver.find_elements(By.CSS_SELECTOR, sel)
                if btns:
                    btns[0].click()
                    slow_pause(1, 2)
                    return True
            except:
                continue
        
        # Fallback con JavaScript
        driver.execute_script("""
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                const text = btn.textContent.toLowerCase();
                if (text.includes('aceptar') || text.includes('accept') || text.includes('consent')) {
                    btn.click();
                    break;
                }
            }
        """)
        slow_pause(0.5, 1)
        return True
        
    except Exception:
        return False

def _is_login_page(driver):
    """Verifica si está en página de login"""
    current_url = driver.current_url or ""
    return "/user/login" in current_url or "login" in current_url.lower()

def _login(driver):
    """Inicia sesión en FlowAgility - VERSIÓN MEJORADA"""
    log("Iniciando login...")
    
    try:
        driver.get(f"{BASE}/user/login")
        
        # Esperar a que cargue la página
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        slow_pause(2, 3)
        
        # Aceptar cookies primero
        _accept_cookies(driver)
        
        # Buscar campos de login con múltiples selectores
        email_selectors = [
            (By.NAME, "user[email]"),
            (By.ID, "user_email"),
            (By.CSS_SELECTOR, "input[type='email']"),
            (By.CSS_SELECTOR, "input[name*='email']"),
        ]
        
        password_selectors = [
            (By.NAME, "user[password]"),
            (By.ID, "user_password"),
            (By.CSS_SELECTOR, "input[type='password']"),
            (By.CSS_SELECTOR, "input[name*='password']"),
        ]
        
        submit_selectors = [
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.CSS_SELECTOR, "button:contains('Sign in')"),
            (By.CSS_SELECTOR, "button:contains('Iniciar')"),
        ]
        
        # Encontrar y llenar email
        email_field = None
        for by, selector in email_selectors:
            try:
                email_field = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((by, selector))
                )
                break
            except:
                continue
        
        if not email_field:
            raise Exception("No se pudo encontrar el campo de email")
        
        email_field.clear()
        email_field.send_keys(FLOW_EMAIL)
        slow_pause(0.5, 1)
        
        # Encontrar y llenar password
        password_field = None
        for by, selector in password_selectors:
            try:
                password_field = driver.find_element(by, selector)
                break
            except:
                continue
        
        if not password_field:
            raise Exception("No se pudo encontrar el campo de password")
        
        password_field.clear()
        password_field.send_keys(FLOW_PASS)
        slow_pause(0.5, 1)
        
        # Encontrar y hacer clic en submit
        submit_button = None
        for by, selector in submit_selectors:
            try:
                submit_button = driver.find_element(by, selector)
                break
            except:
                continue
        
        if not submit_button:
            # Fallback: buscar cualquier botón que pueda ser de submit
            buttons = driver.find_elements(By.TAG_NAME, "button")
            for button in buttons:
                if button.is_displayed() and button.is_enabled():
                    submit_button = button
                    break
        
        if submit_button:
            submit_button.click()
        else:
            # Último fallback: enviar Enter en el campo de password
            password_field.send_keys(Keys.RETURN)
        
        # Esperar a que se complete el login - con verificación más robusta
        log("Esperando completar login...")
        
        def login_success(driver):
            current_url = driver.current_url or ""
            return (
                "/user/login" not in current_url and 
                "login" not in current_url.lower() and
                not current_url.endswith("/user/login")
            )
        
        WebDriverWait(driver, 30).until(login_success)
        slow_pause(3, 5)
        
        # Verificar que realmente estamos logueados
        if _is_login_page(driver):
            raise Exception("El login no se completó correctamente")
        
        log("Login exitoso")
        return True
        
    except Exception as e:
        log(f"Error en login: {e}")
        _save_screenshot(driver, "login_error.png")
        raise

def _ensure_logged_in(driver, max_tries=2):
    """Asegura que está logueado, reintenta si es necesario"""
    for attempt in range(max_tries):
        if not _is_login_page(driver):
            return True
        log(f"Reintentando login... (intento {attempt + 1}/{max_tries})")
        try:
            _login(driver)
            slow_pause(2, 3)
            if not _is_login_page(driver):
                return True
        except Exception as e:
            log(f"Error en reintento de login: {e}")
            if attempt == max_tries - 1:
                return False
    return False

def _full_scroll(driver):
    """Hace scroll completo de la página"""
    last_h = 0
    no_change_count = 0
    for i in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        h = driver.execute_script("return document.body.scrollHeight;")
        
        if h == last_h:
            no_change_count += 1
            if no_change_count >= 3:  # Si no cambia 3 veces seguidas, salir
                break
        else:
            no_change_count = 0
            
        last_h = h
        
        # Mostrar progreso cada 5 scrolls
        if i % 5 == 0:
            log(f"Scroll {i+1}/{MAX_SCROLLS} - Altura: {h}px")
    
    log("Scroll completado")

# ============================== MÓDULO 1: EXTRACCIÓN DE EVENTOS ==============================

def extract_event_details(container_html):
    """Extrae detalles específicos de un evento del HTML"""
    soup = BeautifulSoup(container_html, 'html.parser')
    
    event_data = {}
    
    # ID del evento
    event_container = soup.find('div', class_='group mb-6')
    if event_container:
        event_data['id'] = event_container.get('id', '')
    
    # Información básica
    info_div = soup.find('div', class_='relative flex flex-col w-full pt-1 pb-6 mb-4 border-b border-gray-300')
    if info_div:
        # Fechas
        date_elems = info_div.find_all('div', class_='text-xs')
        if date_elems:
            event_data['fechas'] = date_elems[0].get_text(strip=True)
        
        # Organización
        if len(date_elems) > 1:
            event_data['organizacion'] = date_elems[1].get_text(strip=True)
        
        # Nombre del evento
        name_elem = info_div.find('div', class_='font-caption text-lg text-black truncate -mt-1')
        if name_elem:
            event_data['nombre'] = name_elem.get_text(strip=True)
        
        # Club organizador
        club_elem = info_div.find('div', class_='text-xs mb-0.5 mt-0.5')
        if club_elem:
            event_data['club'] = club_elem.get_text(strip=True)
        
        # Lugar - buscar en todos los divs con text-xs
        location_divs = info_div.find_all('div', class_='text-xs')
        for div in location_divs:
            text = div.get_text(strip=True)
            if '/' in text and ('Spain' in text or 'España' in text):
                event_data['lugar'] = text
                break
    
    # Estado del evento
    status_button = soup.find('div', class_='py-1 px-4 border text-white font-bold rounded text-sm')
    if status_button:
        event_data['estado'] = status_button.get_text(strip=True)
        # Determinar tipo de estado
        if 'Inscribirse' in event_data['estado']:
            event_data['estado_tipo'] = 'inscripcion_abierta'
        elif 'En curso' in event_data['estado']:
            event_data['estado_tipo'] = 'en_curso'
        else:
            event_data['estado_tipo'] = 'desconocido'
    
    # Enlaces
    event_data['enlaces'] = {}
    info_link = soup.find('a', href=lambda x: x and '/info/' in x)
    if info_link:
        event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
    
    participants_link = soup.find('a', href=lambda x: x and '/participants_list' in x)
    if participants_link:
        event_data['enlaces']['participantes'] = urljoin(BASE, participants_link['href'])
    
    runs_link = soup.find('a', href=lambda x: x and '/runs' in x)
    if runs_link:
        event_data['enlaces']['runs'] = urljoin(BASE, runs_link['href'])
    
    # Bandera del país
    flag_div = soup.find('div', class_='text-md')
    if flag_div:
        event_data['pais_bandera'] = flag_div.get_text(strip=True)
    
    return event_data

def extract_events():
    """Función principal para extraer eventos básicos"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado. Ejecuta: pip install selenium")
        return
    
    log("=== Scraping FlowAgility - Competiciones de Agility ===")
    
    driver = _get_driver(headless=HEADLESS)
    
    try:
        # Login
        _login(driver)
        
        # Navegar a eventos
        log("Navegando a la página de eventos...")
        driver.get(EVENTS_URL)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Aceptar cookies
        _accept_cookies(driver)
        
        # Scroll completo para cargar todos los eventos
        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(2, 3)
        
        # Obtener HTML de la página
        page_html = driver.page_source
        
        # Extraer eventos
        log("Extrayendo información de eventos...")
        events = []
        
        # Buscar todos los contenedores de eventos
        soup = BeautifulSoup(page_html, 'html.parser')
        event_containers = soup.find_all('div', class_='group mb-6')
        
        log(f"Encontrados {len(event_containers)} eventos")
        
        for i, container in enumerate(event_containers, 1):
            try:
                event_data = extract_event_details(str(container))
                events.append(event_data)
                log(f"Procesado evento {i}/{len(event_containers)}: {event_data.get('nombre', 'Sin nombre')}")
            except Exception as e:
                log(f"Error procesando evento {i}: {str(e)}")
                continue
        
        # Guardar resultados con timestamp para GitHub Actions
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'01events_{today_str}.json')
        os.makedirs(OUT_DIR, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Extracción completada. {len(events)} eventos guardados en {output_file}")
        
        # Mostrar resumen
        print(f"\n{'='*80}")
        print("RESUMEN DE COMPETICIONES ENCONTRADAS:")
        print(f"{'='*80}")
        
        for i, event in enumerate(events, 1):
            print(f"\n{i}. {event.get('nombre', 'Sin nombre')}")
            print(f"   📅 {event.get('fechas', 'Fecha no especificada')}")
            print(f"   🏢 {event.get('organizacion', 'Organización no especificada')}")
            print(f"   🏆 {event.get('club', 'Club no especificado')}")
            print(f"   📍 {event.get('lugar', 'Lugar no especificado')}")
            print(f"   🚦 {event.get('estado', 'Estado no especificado')}")
        
        print(f"\n{'='*80}")
        print(f"Total: {len(events)} competiciones de agility")
        
        return events
        
    except Exception as e:
        log(f"Error durante el scraping: {str(e)}")
        _save_screenshot(driver, "error_screenshot.png")
        raise
        
    finally:
        try:
            driver.quit()
            log("Navegador cerrado")
        except:
            pass

# ============================== MÓDULO 2: INFORMACIÓN DETALLADA ==============================

def extract_detailed_info(driver, info_url, event_base_info):
    """Extraer información detallada de la página de info de un evento"""
    try:
        log(f"Accediendo a información detallada: {info_url}")
        driver.get(info_url)
        
        # Esperar a que cargue la página
        slow_pause(2, 3)
        
        # Obtener el HTML de la página
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        detailed_info = event_base_info.copy()
        
        # ===== INFORMACIÓN GENERAL =====
        general_info = {}
        
        # Título principal
        title_elem = soup.find('h1', class_=lambda x: x and 'text' in x.lower())
        if title_elem:
            general_info['titulo'] = title_elem.get_text(strip=True)
        
        # Fechas y ubicación
        date_location_elems = soup.find_all('div', class_=lambda x: x and 'text' in x.lower())
        for elem in date_location_elems:
            text = elem.get_text(strip=True)
            if ' - ' in text and len(text) < 50:  # Probablemente fechas
                general_info['fechas_completas'] = text
            elif any(word in text.lower() for word in ['spain', 'españa', 'madrid', 'barcelona']):
                general_info['ubicacion_completa'] = text
        
        # ===== INFORMACIÓN DE INSCRIPCIÓN =====
        registration_info = {}
        
        # Fechas de inscripción
        registration_dates = soup.find_all('div', class_=lambda x: x and any(word in str(x).lower() for word in ['date', 'fecha', 'inscrip']))
        for elem in registration_dates:
            text = elem.get_text(strip=True)
            if 'inscrip' in text.lower() or 'registration' in text.lower():
                registration_info['periodo_inscripcion'] = text
        
        # Precios
        price_elems = soup.find_all(lambda tag: tag.name in ['div', 'span'] and any(word in tag.get_text().lower() for word in ['€', 'euro', 'precio', 'price', 'coste']))
        for elem in price_elems:
            text = elem.get_text(strip=True)
            if '€' in text:
                registration_info['precios'] = text
        
        # ===== INFORMACIÓN DE PRUEBAS =====
        pruebas_info = []
        
        # Buscar secciones de pruebas
        prueba_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(word in str(x).lower() for word in ['prueba', 'competition', 'event', 'round']))
        
        for section in prueba_sections:
            prueba = {}
            
            # Nombre de la prueba
            name_elem = section.find(['h2', 'h3', 'h4', 'strong'])
            if name_elem:
                prueba['nombre'] = name_elem.get_text(strip=True)
            
            # Horarios
            time_elems = section.find_all(lambda tag: any(word in tag.get_text().lower() for word in ['hora', 'time', 'horario', 'schedule']))
            for elem in time_elems:
                prueba['horarios'] = elem.get_text(strip=True)
            
            # Categorías
            category_elems = section.find_all(lambda tag: any(word in tag.get_text().lower() for word in ['categor', 'level', 'nivel', 'class']))
            for elem in category_elems:
                prueba['categorias'] = elem.get_text(strip=True)
            
            if prueba:
                pruebas_info.append(prueba)
        
        # ===== INFORMACIÓN DE CONTACTO =====
        contact_info = {}
        
        # Email de contacto
        email_elems = soup.find_all(lambda tag: '@' in tag.get_text() and '.' in tag.get_text())
        for elem in email_elems:
            contact_info['email'] = elem.get_text(strip=True)
        
        # Teléfono
        phone_elems = soup.find_all(lambda tag: any(word in tag.get_text() for word in ['+34', 'tel:', 'phone', 'tlf']))
        for elem in phone_elems:
            contact_info['telefono'] = elem.get_text(strip=True)
        
        # ===== ENLACES ADICIONALES =====
        additional_links = {}
        
        # Enlaces a reglamentos
        reglamento_links = soup.find_all('a', href=lambda x: x and any(word in x.lower() for word in ['reglamento', 'regulation', 'normas', 'rules']))
        for link in reglamento_links:
            additional_links['reglamento'] = urljoin(BASE, link['href'])
        
        # Enlaces a mapas
        mapa_links = soup.find_all('a', href=lambda x: x and any(word in x.lower() for word in ['map', 'ubicacion', 'location', 'google']))
        for link in mapa_links:
            additional_links['mapa'] = urljoin(BASE, link['href'])
        
        # ===== COMPILAR TODA LA INFORMACIÓN =====
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
        return event_base_info  # Devolver al menos la info básica

def extract_detailed_events():
    """Función principal para extraer información detallada"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado. Ejecuta: pip install selenium")
        return
    
    log("=== EXTRACCIÓN DE INFORMACIÓN DETALLADA DE COMPETICIONES ===")
    
    # Cargar competiciones desde el archivo JSON
    COMPETITIONS_FILE = os.path.join(OUT_DIR, '01events_*.json')
    comp_files = glob(COMPETITIONS_FILE)
    
    if not comp_files:
        log(f"Error: No se encuentra el archivo {COMPETITIONS_FILE}")
        return
    
    # Tomar el archivo más reciente
    latest_comp_file = max(comp_files, key=os.path.getctime)
    
    with open(latest_comp_file, 'r', encoding='utf-8') as f:
        competiciones = json.load(f)
    
    log(f"Cargadas {len(competiciones)} competiciones desde {latest_comp_file}")
    
    driver = _get_driver(headless=HEADLESS)
    
    try:
        # Iniciar sesión
        _login(driver)
        
        competiciones_detalladas = []
        
        for i, competicion in enumerate(competiciones, 1):
            try:
                # Verificar si tiene enlace de información
                if 'enlaces' in competicion and 'info' in competicion['enlaces']:
                    info_url = competicion['enlaces']['info']
                    
                    log(f"Procesando competición {i}/{len(competiciones)}: {competicion.get('nombre', 'Sin nombre')}")
                    
                    # Extraer información detallada
                    competicion_detallada = extract_detailed_info(driver, info_url, competicion)
                    competiciones_detalladas.append(competicion_detallada)
                    
                    # Pausa entre solicitudes
                    slow_pause(2, 4)
                    
                else:
                    log(f"Competición {i} no tiene enlace de información, saltando...")
                    competiciones_detalladas.append(competicion)  # Mantener la info básica
                    
            except Exception as e:
                log(f"Error procesando competición {i}: {str(e)}")
                competiciones_detalladas.append(competicion)  # Mantener la info básica
                continue
        
        # Guardar información detallada con timestamp para GitHub Actions
        today_str = datetime.now().strftime("%Y-%m-%d")
        DETAILED_FILE = os.path.join(OUT_DIR, f'02competiciones_detalladas_{today_str}.json')
        
        with open(DETAILED_FILE, 'w', encoding='utf-8') as f:
            json.dump(competiciones_detalladas, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Información detallada guardada en {DETAILED_FILE}")
        
        # Mostrar resumen
        print(f"\n{'='*80}")
        print("RESUMEN DE INFORMACIÓN EXTRAÍDA:")
        print(f"{'='*80}")
        
        comp_con_info = sum(1 for c in competiciones_detalladas if 'informacion_general' in c)
        comp_con_precios = sum(1 for c in competiciones_detalladas if 'inscripcion' in c and 'precios' in c['inscripcion'])
        comp_con_pruebas = sum(1 for c in competiciones_detalladas if 'pruebas' in c and c['pruebas'])
        
        print(f"Competiciones procesadas: {len(competiciones_detalladas)}")
        print(f"Con información general: {comp_con_info}")
        print(f"Con precios: {comp_con_precios}")
        print(f"Con pruebas detalladas: {comp_con_pruebas}")
        
        # Mostrar ejemplo de una competición con información detallada
        for comp in competiciones_detalladas:
            if 'informacion_general' in comp:
                print(f"\nEjemplo de información detallada:")
                print(f"Nombre: {comp.get('nombre')}")
                if 'informacion_general' in comp:
                    print(f"Fechas: {comp['informacion_general'].get('fechas_completas', 'N/A')}")
                if 'inscripcion' in comp and 'precios' in comp['inscripcion']:
                    print(f"Precios: {comp['inscripcion']['precios']}")
                break
        
        return competiciones_detalladas
        
    except Exception as e:
        log(f"Error durante la extracción detallada: {str(e)}")
        raise
        
    finally:
        try:
            driver.quit()
            log("Navegador cerrado")
        except:
            pass

# ============================== FUNCIÓN PARA GITHUB ACTIONS ==============================

def generate_final_unification():
    """Genera el archivo participants_completos_final.json que espera el workflow"""
    try:
        # Buscar el archivo más reciente de participantes procesados
        participantes_files = glob(os.path.join(OUT_DIR, "participantes_procesado_*.csv"))
        if not participantes_files:
            log("No se encontraron archivos de participantes procesados")
            return False
            
        # Tomar el más reciente
        latest_file = max(participantes_files, key=os.path.getctime)
        
        # Leer y convertir a JSON
        df = pd.read_csv(latest_file)
        final_json_path = os.path.join(OUT_DIR, "participants_completos_final.json")
        
        # Convertir a formato JSON adecuado
        records = df.to_dict('records')
        with open(final_json_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        
        log(f"Archivo final de unificación generado: {final_json_path}")
        return True
        
    except Exception as e:
        log(f"Error generando archivo final: {e}")
        return False

# ============================== INTERFAZ PRINCIPAL ==============================

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description="FlowAgility Scraper - Sistema completo")
    parser.add_argument("module", choices=["events", "info", "all"], 
                       nargs="?", default="all", help="Módulo a ejecutar")
    args = parser.parse_args()
    
    # Crear directorio de salida
    os.makedirs(OUT_DIR, exist_ok=True)
    
    try:
        if args.module == "events" or args.module == "all":
            extract_events()
        
        if args.module == "info" or args.module == "all":
            extract_detailed_events()
        
        # Para GitHub Actions, siempre generar el archivo final de unificación
        if args.module == "all":
            success = generate_final_unification()
            if not success:
                log("Advertencia: No se pudo generar el archivo final de unificación")
            
        log("Proceso completado exitosamente")
        
    except Exception as e:
        log(f"Error durante la ejecución: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
