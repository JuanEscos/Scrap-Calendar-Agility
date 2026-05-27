import csv, folium, os
from branca.element import Template, MacroElement

csv_path = 'eventos_agility_2026.csv'
out_path = 'mapa_agility_2026.html'

m = folium.Map(location=[40.4168, -3.7038], zoom_start=6)

colors = [
    'cadetblue', 'purple', 'green', 'darkblue', 'orange',
    'lightgreen', 'black', 'red', 'darkred', 'lightred',
    'beige', 'darkgreen', 'darkpurple', 'pink', 'lightblue',
    'gray', 'lightgray', 'blue'
]

data_rows = []
unique_cities = set()
if os.path.exists(csv_path):
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data_rows.append(row)
            ciudad = row.get('Ciudad', '').strip()
            if ciudad:
                unique_cities.add(ciudad)

cities_sorted = sorted(list(unique_cities))
city_color_map = {}
for i, city in enumerate(cities_sorted):
    city_color_map[city] = colors[i % len(colors)]

for row in data_rows:
    lat, lon = row.get('Latitud'), row.get('Longitud')
    ciudad = row.get('Ciudad', '').strip()
    if lat and lon and lat.strip() and lon.strip():
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            popup_html = f"""
            <b>{row.get('Nombre')}</b><br>
            Ciudad: {ciudad}<br>
            Inicio: {row.get('Fecha inicio')}<br>
            <a href='{row.get('URL')}' target='_blank'>Más Info</a>
            """
            
            marker_color = city_color_map.get(ciudad, 'blue')
            
            folium.Marker(
                [lat_f, lon_f],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=ciudad,
                icon=folium.Icon(color=marker_color, icon='info-sign')
            ).add_to(m)
        except ValueError:
            pass

legend_html = '''
{% macro html(this, kwargs) %}
<!doctype html>
<html lang="en">
<head>
  <style>
    .maplegend {
      position: absolute;
      z-index:9999;
      border:2px solid grey;
      background-color:rgba(255, 255, 255, 0.9);
      border-radius:6px;
      padding: 10px;
      font-size:14px;
      right: 20px;
      top: 20px;
      max-height: 80%;
      overflow-y: auto;
    }
    .maplegend .legend-title {
      text-align: left;
      margin-bottom: 5px;
      font-weight: bold;
      font-size: 90%;
    }
    .maplegend .legend-scale ul {
      margin: 0;
      margin-bottom: 5px;
      padding: 0;
      float: left;
      list-style: none;
    }
    .maplegend .legend-scale ul li {
      font-size: 80%;
      list-style: none;
      margin-left: 0;
      line-height: 18px;
      margin-bottom: 4px;
      display: flex;
      align-items: center;
    }
    .maplegend ul.legend-labels li span {
      display: inline-block;
      height: 14px;
      width: 14px;
      margin-right: 8px;
      border: 1px solid #555;
      border-radius: 50%;
    }
  </style>
</head>
<body>
<div id='maplegend' class='maplegend'>
  <div class='legend-title'>Leyenda (Ciudad &harr; Color)</div>
  <div class='legend-scale'>
    <ul class='legend-labels'>
'''

for city in cities_sorted:
    color = city_color_map[city]
    color_hex_map = {
        'red': '#d33d2a', 'blue': '#38aadd', 'green': '#72b026', 'purple': '#d252b9', 
        'orange': '#f69730', 'darkred': '#a23336', 'lightred': '#ff8e7f', 'beige': '#ffcb92', 
        'darkblue': '#0067a3', 'darkgreen': '#728224', 'cadetblue': '#436978', 
        'darkpurple': '#5b396b', 'white': '#ffffff', 'pink': '#ff91ea', 'lightblue': '#8adaff', 
        'lightgreen': '#bbf970', 'gray': '#575757', 'black': '#303030', 'lightgray': '#a3a3a3'
    }
    hex_col = color_hex_map.get(color, '#38aadd')
    legend_html += f"      <li><span style='background:{hex_col};'></span>{city}</li>\n"

legend_html += '''
    </ul>
  </div>
</div>
</body>
</html>
{% endmacro %}
'''

macro = MacroElement()
macro._template = Template(legend_html)
m.get_root().add_child(macro)

m.save(out_path)
print('Map saved to', out_path)
