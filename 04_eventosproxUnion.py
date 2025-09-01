import os
import json
import re
from datetime import datetime

# Configuración
COMPETICIONES_FILE = "./output/competiciones_detalladas.json"
PARTICIPANTS_DIR = "./output/participants"
OUTPUT_DIR = "./output"
FINAL_OUTPUT_FILE = "./output/participantes_completos_final.json"

def clean_filename(name):
    """Limpia el nombre para usar en archivos"""
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def parse_date(date_str):
    """Intenta parsear la fecha a formato español"""
    if not date_str:
        return ""
    
    try:
        # Mapeo de meses en inglés a español
        month_map = {
            'Jan': 'Ene', 'Feb': 'Feb', 'Mar': 'Mar', 'Apr': 'Abr',
            'May': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Aug': 'Ago',
            'Sep': 'Sep', 'Oct': 'Oct', 'Nov': 'Nov', 'Dec': 'Dic'
        }
        
        # Reemplazar meses en inglés
        for eng, esp in month_map.items():
            date_str = date_str.replace(eng, esp)
        
        return date_str
    except:
        return date_str

def main():
    """Procesa y combina los datos de competiciones y participantes"""
    print("=== COMBINANDO DATOS DE COMPETICIONES Y PARTICIPANTES ===")
    
    # Cargar datos de competiciones
    if not os.path.exists(COMPETICIONES_FILE):
        print(f"❌ Archivo no encontrado: {COMPETICIONES_FILE}")
        return
    
    with open(COMPETICIONES_FILE, 'r', encoding='utf-8') as f:
        competiciones = json.load(f)
    
    print(f"Cargadas {len(competiciones)} competiciones")
    
    participantes_completos = []
    
    # Procesar cada competición
    for competicion in competiciones:
        comp_id = competicion.get('id', '')
        comp_nombre = competicion.get('nombre', 'Sin nombre')
        
        if not comp_id:
            continue
        
        # Construir nombre del archivo de participantes
        safe_nombre = clean_filename(comp_nombre)
        participantes_file = os.path.join(PARTICIPANTS_DIR, f"participantes_{safe_nombre}_{comp_id}.json")
        
        if not os.path.exists(participantes_file):
            print(f"⚠️ Archivo de participantes no encontrado: {participantes_file}")
            continue
        
        # Cargar participantes de esta competición
        with open(participantes_file, 'r', encoding='utf-8') as f:
            participantes = json.load(f)
        
        print(f"📊 Procesando {comp_nombre}: {len(participantes)} participantes")
        
        # Combinar datos para cada participante
        for participante in participantes:
            # Datos básicos del participante
            participante_completo = {
                # Datos de la competición
                "Competicion_ID": comp_id,
                "Competicion_Nombre": comp_nombre,
                "Competicion_Fechas": competicion.get('fechas', ''),
                "Competicion_Organizacion": competicion.get('organizacion', ''),
                "Competicion_Club": competicion.get('club', ''),
                "Competicion_Lugar": competicion.get('lugar', ''),
                
                # Datos del participante
                "Participante_ID": participante.get('ID', ''),
                "Dorsal": participante.get('Dorsal', ''),
                "Guia": participante.get('Guia', ''),
                "Perro": participante.get('Perro', ''),
                "Raza": participante.get('Raza', ''),
                "Edad": participante.get('Edad', ''),
                "Genero": participante.get('Genero', ''),
                "Altura_cm": participante.get('Altura_cm', ''),
                "Pedigree": participante.get('Pedigree', ''),
                "Licencia": participante.get('Licencia', ''),
                "Federacion": participante.get('Federacion', ''),
                "Club_Participante": participante.get('Club', ''),
                
                # Campos para competiciones (se llenarán después)
                "Grado": "",
                "Categoria": "",
                "Fecha_1": "", "Fecha_2": "", "Fecha_3": "", "Fecha_4": "", "Fecha_5": "",
                "Fecha_6": "", "Fecha_7": "", "Fecha_8": "", "Fecha_9": "", "Fecha_10": ""
            }
            
            # Procesar competiciones del participante
            competiciones_participante = participante.get('Competiciones', {})
            
            # Extraer grado y categoría (tomamos los de la primera competición)
            if competiciones_participante:
                primera_comp = next(iter(competiciones_participante.values()), {})
                participante_completo["Grado"] = primera_comp.get('Grado', '')
                participante_completo["Categoria"] = primera_comp.get('Categoria', '')
                
                # Extraer fechas de todas las competiciones
                for i, (comp_key, comp_data) in enumerate(competiciones_participante.items(), 1):
                    if i > 10:  # Máximo 10 fechas
                        break
                    
                    fecha_field = f"Fecha_{i}"
                    fecha_original = comp_data.get('Fecha', '')
                    participante_completo[fecha_field] = parse_date(fecha_original)
            
            participantes_completos.append(participante_completo)
    
    # Guardar el resultado final
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with open(FINAL_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(participantes_completos, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Guardados {len(participantes_completos)} participantes completos en {FINAL_OUTPUT_FILE}")
    
    # Mostrar estadísticas
    if participantes_completos:
        sample = participantes_completos[0]
        print(f"\n📋 EJEMPLO DE DATCOMBINADOS:")
        print(f"  Competición: {sample['Competicion_Nombre']}")
        print(f"  Participante: {sample['Guia']} con {sample['Perro']}")
        print(f"  Dorsal: {sample['Dorsal']}, Grado: {sample['Grado']}, Categoría: {sample['Categoria']}")
        print(f"  Fechas: {sample['Fecha_1']}, {sample['Fecha_2']}")

if __name__ == "__main__":
    main()
