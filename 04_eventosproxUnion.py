import os, json, re, time
from datetime import datetime

COMPETICIONES_FILE = "./output/competiciones_detalladas.json"
PARTICIPANTS_DIR   = "./output/participants"
OUTPUT_DIR         = "./output"
FINAL_OUTPUT_FILE  = "./output/participantes_completos_final.json"

def clean_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def parse_date(date_str):
    if not date_str:
        return ""
    month_map = {'Jan':'Ene','Feb':'Feb','Mar':'Mar','Apr':'Abr','May':'May','Jun':'Jun',
                 'Jul':'Jul','Aug':'Ago','Sep':'Sep','Oct':'Oct','Nov':'Nov','Dec':'Dic'}
    for eng, esp in month_map.items():
        date_str = date_str.replace(eng, esp)
    return date_str

def pick(d, *names):
    """Devuelve la primera key presente/no vacía."""
    if not isinstance(d, dict):
        return ""
    for n in names:
        if n in d and d[n] not in (None, "", []):
            v = d[n]
            return v.strip() if isinstance(v, str) else v
    return ""

def load_participants_file(basename_candidates):
    """Intenta abrir el primer fichero existente y devuelve una LISTA de participantes."""
    for base in basename_candidates:
        path = os.path.join(PARTICIPANTS_DIR, base)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Acepta lista directa o wrapper con "participants"
            if isinstance(data, dict) and "participants" in data:
                participants = data["participants"]
            else:
                participants = data
            # Debe ser lista
            return participants if isinstance(participants, list) else []
    return []

def main():
    print("=== UNIÓN: competiciones + participantes ===")

    if not os.path.exists(COMPETICIONES_FILE):
        print(f"❌ No existe {COMPETICIONES_FILE}")
        return

    with open(COMPETICIONES_FILE, "r", encoding="utf-8") as f:
        competiciones = json.load(f)

    print(f"Competiciones cargadas: {len(competiciones)}")

    participantes_completos = []

    for comp in competiciones:
        comp_id   = comp.get("id", "")
        comp_nom  = comp.get("nombre", "Sin nombre")
        comp_fech = comp.get("fechas", "")
        comp_org  = comp.get("organizacion", "")
        comp_club = comp.get("club", "")
        comp_lug  = comp.get("lugar", "")

        if not comp_id:
            continue

        safe_nom  = clean_filename(comp_nom)

        # Archivos posibles (nuevo vs antiguo)
        candidates = [
            f"participants_{safe_nom}_{comp_id}.json",  # lo que espera 04
            f"participants_{comp_id}.json"              # lo que guarda 03 actual
        ]
        participants = load_participants_file(candidates)

        if not participants:
            print(f"⚠️ Sin participantes para: {comp_nom}")
            continue

        print(f"📊 {comp_nom}: {len(participants)} participantes")

        for p in participants:
            # Normaliza "Competiciones"/"competitions" (dict o lista)
            comps = p.get("Competiciones") or p.get("competitions") or {}
            first_comp = {}
            if isinstance(comps, dict) and comps:
                first_comp = next((v for v in comps.values() if isinstance(v, dict)), {})
            elif isinstance(comps, list) and comps and isinstance(comps[0], dict):
                first_comp = comps[0]

            item = {
                # Competición
                "Competicion_ID":        comp_id,
                "Competicion_Nombre":    comp_nom,
                "Competicion_Fechas":    comp_fech,
                "Competicion_Organizacion": comp_org,
                "Competicion_Club":      comp_club,
                "Competicion_Lugar":     comp_lug,

                # Participante (acepta ES/EN/minúsculas)
                "Participante_ID":   pick(p, "Participante_ID","ID","id","participant_id"),
                "Dorsal":            pick(p, "Dorsal","dorsal","bib"),
                "Guia":              pick(p, "Guia","Guía","guide","handler"),
                "Perro":             pick(p, "Perro","perro","dog","Nombre_perro"),
                "Raza":              pick(p, "Raza","raza","breed"),
                "Edad":              pick(p, "Edad","edad","age"),
                "Genero":            pick(p, "Genero","género","sexo","gender","sex"),
                "Altura_cm":         pick(p, "Altura_cm","altura_cm","altura","height_cm"),
                "Pedigree":          pick(p, "Pedigree","pedigree"),
                "Licencia":          pick(p, "Licencia","licencia","license"),
                "Federacion":        pick(p, "Federacion","federacion","federation"),
                "Club_Participante": pick(p, "Club","club","Club_Participante"),

                # Del primer bloque de competiciones del participante
                "Grado":             pick(first_comp, "Grado","grado","level"),
                "Categoria":         pick(first_comp, "Categoria","categoria","category"),
                "Fecha_1": "", "Fecha_2": "", "Fecha_3": "", "Fecha_4": "", "Fecha_5": "",
                "Fecha_6": "", "Fecha_7": "", "Fecha_8": "", "Fecha_9": "", "Fecha_10": ""
            }

            # Rellenar hasta 10 fechas desde todas las competiciones del participante
            fechas = []
            if isinstance(comps, dict):
                for d in comps.values():
                    if isinstance(d, dict):
                        fechas.append(parse_date(pick(d, "Fecha","fecha","date")))
            elif isinstance(comps, list):
                for d in comps:
                    if isinstance(d, dict):
                        fechas.append(parse_date(pick(d, "Fecha","fecha","date")))
            for i, fch in enumerate(fechas[:10], start=1):
                item[f"Fecha_{i}"] = fch

            participantes_completos.append(item)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(participantes_completos, f, ensure_ascii=False, indent=2)

    print(f"✅ Guardados {len(participantes_completos)} en {FINAL_OUTPUT_FILE}")

if __name__ == "__main__":
    main()
