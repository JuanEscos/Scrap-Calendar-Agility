# Scrap-Calendar-Agility
Hace un scraping de las competiciones de Agility de la Web de RSCE y lo convierte en un csv, un GeoJSON y genera automáticamente un **mapa interactivo HTML**.

Scraper RSCE Agility → CSV + GeoJSON + Mapa HTML (Folium)

Este proyecto permite **extraer automáticamente** desde la web de la RSCE todas las pruebas de Agility y guardar:

- **CSV** con datos de las pruebas (Nombre, Fechas, URL, Ciudad, Estado, Latitud, Longitud).
- **GeoJSON** con los puntos geográficos de cada prueba (para usar en QGIS, Leaflet, etc.).
- **Mapa HTML Interactivo** generado con Folium, con colores por ciudad y leyenda interactiva.

---

## 🚀 Requisitos

- Python 3.9 o superior
- Google Chrome instalado
- [ChromeDriver](https://chromedriver.chromium.org/) (se gestiona automáticamente con `webdriver-manager`)

Librerías Python necesarias:

```bash
pip install selenium webdriver-manager beautifulsoup4 geopy python-dotenv folium pandas
```

⚙️ Configuración
Puedes configurar las variables en un archivo `.env` en la carpeta del proyecto.

Ejemplo de `.env`:

```ini
URL_BASE=https://www.rsce.es/eventos-rsce/jsf/jet-engine:eventocuadro/tax/tipos-de-disciplinas:38/meta/fecha-evento!date:2026.1.1-/
CARPETA_DESTINO=./resultados_agility
NOMBRE_CSV=eventos_agility_2026.csv
NOMBRE_GEOJSON=eventos_agility_2026.geojson

SOLO_PRIMERA_PAGINA=false
MAX_PAGINAS=50

# Aplica filtro en la propia web "Desde=hoy"
APLICAR_FILTRO_UI=true
# Filtrar por fecha en el post-procesado (si no parsea la fecha, conserva)
FILTRAR_DESDE_HOY=true

# Hacer geocodificación de ciudades (Nominatim). 
# Para pruebas rápidas puedes poner false.
GEOCODIFICAR=true
```

## ▶️ Ejecución
Ejecuta el script principal para obtener los datos y luego genera el mapa:

```bash
# 1. Extraer los datos (genera CSV y GeoJSON)
python scrape_rsce_csv_geo.py

# 2. Generar el mapa HTML
python generate_map.py
```

El programa abrirá Chrome en modo headless, recorrerá las páginas y generará:

- Un **CSV** con las pruebas.
- Un **GeoJSON** con las coordenadas.
- Un archivo **mapa_agility_2026.html** interactivo con las chinchetas agrupadas por color según la ciudad.

## 📂 Formatos de salida

### CSV
El CSV tiene las columnas:
```mathematica
Nombre, Fecha inicio, Fecha fin, URL, Ciudad, Estado, Latitud, Longitud
```
Ejemplo:
```csv
"C.A. Divertidog – Prueba de Agility","13 septiembre, 2026","14 septiembre, 2026","https://www.rsce.es/...","Zaragoza","Activo",41.6488,-0.8891
```

### GeoJSON
El GeoJSON tiene este esquema:
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [-0.8891, 41.6488] },
      "properties": {
        "nombre": "C.A. Divertidog – Prueba de Agility",
        "inicio": "13 septiembre, 2026",
        "fin": "14 septiembre, 2026",
        "ciudad": "Zaragoza",
        "estado": "Activo",
        "url": "https://www.rsce.es/..."
      }
    }
  ]
}
```

### Mapa HTML
Un archivo `.html` totalmente funcional que utiliza la librería de mapas web Leaflet. Muestra una leyenda flotante y permite hacer clic sobre cada evento para ver la información ampliada y el enlace.

## 📝 Notas
- Los eventos **Anulados** se excluyen del CSV y GeoJSON finales.
- Si `GEOCODIFICAR=true`, se usan las coordenadas de Nominatim (una petición por ciudad).
- Si hay muchas pruebas, puede tardar (límite: 1 req/s por Nominatim).
- Puedes poner `false` para omitir coordenadas.
- `SOLO_PRIMERA_PAGINA=true` sirve para depurar más rápido.
- Se recomienda ejecutar en red estable (la RSCE usa scroll dinámico + paginación).

## 🛠️ Futuras mejoras
- Guardar un segundo CSV/GeoJSON con las pruebas anuladas (para auditoría).
