# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 22:51:44 2025

@author: Juan
"""

import os
import json
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://www.flowagility.com"
OUT_DIR = "./output"
COMPETITIONS_FILE = os.path.join(OUT_DIR, '01events.json')
DETAILED_FILE = os.path.join(OUT_DIR, '02competiciones_detalladas.json')

def log(message):
    """Función de logging"""
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=1, max_s=2):
    """Pausa aleatoria entre min_s y max_s segundos"""
    import random
    time.sleep(random.uniform(min_s, max_s))

def _import_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, WebDriverException
    )
    return webdriver, By, Options, WebDriverWait, EC, TimeoutException, NoSuchElementException, WebDriverException

def _get_driver(headless=True):
    webdriver, By, Options, *_ = _import_selenium()
    
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
    
    # Usar ChromeDriverManager para manejar automáticamente la versión
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except ImportError:
        log("webdriver_manager no instalado, usando ChromeDriver del sistema")
        return webdriver.Chrome(options=opts)

def _login(driver, By, WebDriverWait, EC):
    """Iniciar sesión en FlowAgility"""
    log("Iniciando login...")
    driver.get(f"{BASE}/user/login")
    wait = WebDriverWait(driver, 25)
    
    # Esperar y llenar campos de login
    email = wait.until(EC.presence_of_element_located((By.NAME, "user[email]")))
    pwd = driver.find_element(By.NAME, "user[password]")
    
    email.clear()
    email.send_keys("pilar1959suarez@gmail.com")
    slow_pause(0.5, 1)
    
    pwd.clear()
    pwd.send_keys("Seattle1")
    slow_pause(0.5, 1)
    
    # Hacer clic en el botón de login
    driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    
    # Esperar a que se complete el login
    wait.until(lambda d: "/user/login" not in d.current_url)
    slow_pause(2, 3)
    log("Login exitoso")

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
        
        # ===== ENLICES ADICIONALES =====
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

def main():
    """Función principal para extraer información detallada"""
    log("=== EXTRACCIÓN DE INFORMACIÓN DETALLADA DE COMPETICIONES ===")
    
    # Cargar competiciones desde el archivo JSON
    if not os.path.exists(COMPETITIONS_FILE):
        log(f"Error: No se encuentra el archivo {COMPETITIONS_FILE}")
        return
    
    with open(COMPETITIONS_FILE, 'r', encoding='utf-8') as f:
        competiciones = json.load(f)
    
    log(f"Cargadas {len(competiciones)} competiciones desde {COMPETITIONS_FILE}")
    
    # Importar Selenium
    (webdriver, By, Options, WebDriverWait, EC, 
     TimeoutException, NoSuchElementException, WebDriverException) = _import_selenium()
    
    driver = _get_driver(headless=False)  # headless=False para ver lo que pasa
    
    try:
        # Iniciar sesión
        _login(driver, By, WebDriverWait, EC)
        
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
        
        # Guardar información detallada
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
        
    except Exception as e:
        log(f"Error durante la extracción detallada: {str(e)}")
        
    finally:
        driver.quit()
        log("Navegador cerrado")

if __name__ == "__main__":
    main()
