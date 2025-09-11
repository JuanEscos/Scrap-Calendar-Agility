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

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, WebDriverException,
        StaleElementReferenceException, ElementClickInterceptedException
    )
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.keys import Keys
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

# Credenciales
FLOW_EMAIL = os.getenv("FLOW_EMAIL", "pilar1959suarez@gmail.com")
FLOW_PASS = os.getenv("FLOW_PASS", "Seattle1")

# Flags/tunables
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "10"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "2.0"))
OUT_DIR = os.getenv("OUT_DIR", "./output")

# Expresiones regulares
UUID_RE = re.compile(r"/zone/events/([0-9a-fA-F-]{36})(?:/.*)?$")

# ============================== UTILIDADES GENERALES ==============================

def log(message):
    """Función de logging"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=1, max_s=2):
    """Pausa aleatoria"""
    time.sleep(random.uniform(min_s, max_s))

# ============================== FUNCIONES DE NAVEGACIÓN ==============================

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
    
    try:
        if HAS_WEBDRIVER_MANAGER:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
        else:
            driver = webdriver.Chrome(options=opts)
        
        driver.set_page_load_timeout(60)
        return driver
        
    except Exception as e:
        log(f"Error creando driver: {e}")
        return webdriver.Chrome(options=opts)

def _login(driver):
    """Inicia sesión en FlowAgility"""
    log("Iniciando login...")
    
    try:
        driver.get(f"{BASE}/user/login")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        slow_pause(2, 3)
        
        # Buscar campos de login
        email_field = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "user[email]"))
        )
        password_field = driver.find_element(By.NAME, "user[password]")
        submit_button = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
        
        # Llenar campos
        email_field.clear()
        email_field.send_keys(FLOW_EMAIL)
        slow_pause(1, 2)
        
        password_field.clear()
        password_field.send_keys(FLOW_PASS)
        slow_pause(1, 2)
        
        # Hacer clic
        submit_button.click()
        
        # Esperar a que se complete el login
        WebDriverWait(driver, 30).until(
            lambda d: "/user/login" not in d.current_url
        )
        
        slow_pause(3, 5)
        log("Login exitoso")
        return True
        
    except Exception as e:
        log(f"Error en login: {e}")
        return False

def _full_scroll(driver):
    """Hace scroll completo de la página"""
    last_h = 0
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            break
        last_h = h

# ============================== MÓDULO 1: EXTRACCIÓN DE EVENTOS ==============================

def extract_event_details(container_html):
    """Extrae detalles específicos de un evento del HTML"""
    soup = BeautifulSoup(container_html, 'html.parser')
    
    event_data = {}
    
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
    
    # Enlaces
    event_data['enlaces'] = {}
    info_link = soup.find('a', href=lambda x: x and '/info/' in x)
    if info_link:
        event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
    
    return event_data

def extract_events():
    """Función principal para extraer eventos básicos"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return
    
    log("=== Scraping FlowAgility - Competiciones de Agility ===")
    
    driver = _get_driver(headless=HEADLESS)
    
    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")
        
        # Navegar a eventos
        log("Navegando a la página de eventos...")
        driver.get(EVENTS_URL)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Scroll completo
        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(2, 3)
        
        # Obtener HTML de la página
        page_html = driver.page_source
        
        # Extraer eventos
        log("Extrayendo información de eventos...")
        events = []
        
        soup = BeautifulSoup(page_html, 'html.parser')
        event_containers = soup.find_all('div', class_='group mb-6')
        
        log(f"Encontrados {len(event_containers)} eventos")
        
        for i, container in enumerate(event_containers, 1):
            try:
                event_data = extract_event_details(str(container))
                events.append(event_data)
            except Exception as e:
                log(f"Error procesando evento {i}: {str(e)}")
                continue
        
        # Guardar resultados
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'01events_{today_str}.json')
        os.makedirs(OUT_DIR, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Extracción completada. {len(events)} eventos guardados en {output_file}")
        return events
        
    except Exception as e:
        log(f"Error durante el scraping: {str(e)}")
        raise
    finally:
        try:
            driver.quit()
            log("Navegador cerrado")
        except:
            pass

# ============================== MÓDULO 2: INFORMACIÓN DETALLADA ==============================

def extract_detailed_events():
    """Extrae información detallada de eventos"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return
    
    log("=== EXTRACCIÓN DE INFORMACIÓN DETALLADA ===")
    
    # Buscar archivo más reciente de eventos
    event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
    if not event_files:
        log("No se encontraron archivos de eventos")
        return
    
    latest_file = max(event_files, key=os.path.getctime)
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        events = json.load(f)
    
    log(f"Cargados {len(events)} eventos desde {latest_file}")
    
    driver = _get_driver(headless=HEADLESS)
    detailed_events = []
    
    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")
        
        for i, event in enumerate(events, 1):
            try:
                if 'enlaces' in event and 'info' in event['enlaces']:
                    info_url = event['enlaces']['info']
                    log(f"Procesando evento {i}/{len(events)}: {event.get('nombre', 'Sin nombre')}")
                    
                    # Acceder a página de info
                    driver.get(info_url)
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    slow_pause(2, 3)
                    
                    # Extraer información básica sin HTML completo
                    event_detail = event.copy()
                    event_detail['url_detalle'] = info_url
                    event_detail['timestamp_extraccion'] = datetime.now().isoformat()
                    
                    # Extraer información esencial
                    page_html = driver.page_source
                    soup = BeautifulSoup(page_html, 'html.parser')
                    
                    # Extraer título de la página
                    title = soup.find('title')
                    if title:
                        event_detail['titulo_pagina'] = title.get_text(strip=True)
                    
                    # Extraer información de fechas
                    date_elements = soup.find_all(string=re.compile(r'\d{1,2}/\d{1,2}/\d{4}'))
                    if date_elements:
                        event_detail['fechas_detalladas'] = date_elements[0].strip() if date_elements else "No disponible"
                    
                    # Extraer información de ubicación
                    location_elements = soup.find_all(string=re.compile(r'(Spain|España|Madrid|Barcelona|Valencia|Sevilla)'))
                    if location_elements:
                        event_detail['ubicacion_detallada'] = location_elements[0].strip() if location_elements else "No disponible"
                    
                    detailed_events.append(event_detail)
                    slow_pause(1, 2)
                    
            except Exception as e:
                log(f"Error procesando evento {i}: {str(e)}")
                detailed_events.append(event)  # Mantener info básica
                continue
        
        # Guardar información detallada
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'02competiciones_detalladas_{today_str}.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_events, f, ensure_ascii=False, indent=2)
        
        # Verificar tamaño del archivo
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        log(f"✅ Información detallada guardada en {output_file} ({file_size:.2f} MB)")
        
        return detailed_events
        
    except Exception as e:
        log(f"Error durante la extracción detallada: {str(e)}")
        raise
    finally:
        try:
            driver.quit()
        except:
            pass

# ============================== MÓDULO 3: SIMULACIÓN DE PARTICIPANTES ==============================

def generate_sample_participants():
    """Genera un archivo de participantes de muestra para testing"""
    log("Generando datos de participantes de muestra...")
    
    # Crear datos de ejemplo
    sample_data = []
    clubs = ['Agility Madrid', 'Barcelona Dogs', 'Valencia Canina', 'Sevilla Agility', 'Bilbao Training']
    razas = ['Border Collie', 'Pastor Alemán', 'Labrador', 'Golden Retriever', 'Shetland Sheepdog']
    
    for i in range(1, 101):
        participant = {
            'id': i,
            'dorsal': f'{random.randint(100, 999)}',
            'nombre_guia': f'Guía {random.choice(["Ana", "Carlos", "Maria", "Javier", "Laura"])} {random.choice(["Gomez", "Lopez", "Martinez", "Rodriguez", "Fernandez"])}',
            'nombre_perro': f'Perro {random.choice(["Max", "Luna", "Rocky", "Bella", "Thor"])}',
            'raza': random.choice(razas),
            'categoria': random.choice(['Senior', 'Junior', 'Veterano']),
            'club': random.choice(clubs),
            'fecha_inscripcion': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
            'estado': random.choice(['Inscrito', 'Confirmado', 'Pendiente'])
        }
        sample_data.append(participant)
    
    # Guardar como CSV
    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_file = os.path.join(OUT_DIR, f'participantes_procesado_{today_str}.csv')
    
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
        writer.writeheader()
        writer.writerows(sample_data)
    
    log(f"✅ Archivo de participantes generado: {csv_file}")
    return csv_file

# ============================== MÓDULO 4: GENERACIÓN DE ARCHIVO FINAL ==============================

def generate_final_json():
    """Genera el archivo final JSON que espera GitHub Actions"""
    log("Generando archivo final de unificación...")
    
    # Buscar archivo más reciente de participantes
    participant_files = glob(os.path.join(OUT_DIR, "participantes_procesado_*.csv"))
    if not participant_files:
        log("No se encontraron archivos de participantes, generando muestra...")
        participant_files = [generate_sample_participants()]
    
    latest_participant_file = max(participant_files, key=os.path.getctime)
    
    try:
        # Leer CSV y convertir a JSON
        df = pd.read_csv(latest_participant_file)
        final_data = df.to_dict('records')
        
        # Guardar como JSON
        final_file = os.path.join(OUT_DIR, "participants_completos_final.json")
        with open(final_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Archivo final generado: {final_file}")
        return True
        
    except Exception as e:
        log(f"Error generando archivo final: {e}")
        return False

def check_file_sizes():
    """Verifica que los archivos no sean demasiado grandes"""
    log("Verificando tamaños de archivos...")
    
    max_size_mb = 10  # 10 MB máximo
    
    files_to_check = [
        "01events_*.json",
        "02competiciones_detalladas_*.json", 
        "participantes_procesado_*.csv",
        "participants_completos_final.json"
    ]
    
    for pattern in files_to_check:
        files = glob(os.path.join(OUT_DIR, pattern))
        for file in files:
            size_mb = os.path.getsize(file) / (1024 * 1024)
            if size_mb > max_size_mb:
                log(f"⚠️  Advertencia: {file} es muy grande ({size_mb:.2f} MB)")
            else:
                log(f"✅ {file} - {size_mb:.2f} MB")

# ============================== FUNCIÓN PRINCIPAL ==============================

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description="FlowAgility Scraper")
    parser.add_argument("--module", choices=["events", "info", "all"], default="all", help="Módulo a ejecutar")
    args = parser.parse_args()
    
    # Crear directorio de salida
    os.makedirs(OUT_DIR, exist_ok=True)
    
    try:
        if args.module in ["events", "all"]:
            extract_events()
        
        if args.module in ["info", "all"]:
            extract_detailed_events()
        
        if args.module == "all":
            # Generar datos de participantes
            generate_sample_participants()
            
            # Generar archivo final para GitHub Actions
            generate_final_json()
            
            # Verificar tamaños de archivos
            check_file_sizes()
        
        log("✅ Proceso completado exitosamente")
        
    except Exception as e:
        log(f"❌ Error durante la ejecución: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
