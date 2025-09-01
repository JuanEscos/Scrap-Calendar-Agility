#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import re
import sys
from glob import glob
from datetime import datetime

# Defaults (se pueden sobreescribir por argv)
COMPETICIONES_FILE = "./output/competiciones_detalladas.json"
PARTICIPANTS_DIR   = "./output/participants"
OUTPUT_DIR         = "./output"
FINAL_OUTPUT_FILE  = "./output/participantes_completos_final.json"

def arg_or_default(i, default):
    return sys.argv[i] if len(sys.argv) > i and sys.argv[i].strip() else default

# Permitir argumentos opcionales: [competiciones_json] [participants_dir] [final_json]
COMPETICIONES_FILE = arg_or_default(1, COMPETICIONES_FILE)
PARTICIPANTS_DIR   = arg_or_default(2, PARTICIPANTS_DIR)
FINAL_OUTPUT_FILE  = arg_or_default(3, FINAL_OUTPUT_FILE)
OUTPUT_DIR         = os.path.dirname(FINAL_OUTPUT_FILE) or OUTPUT_DIR

def clean_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name or "")

def parse_date(date_str):
    if not date_str:
        return ""
    month_map = {
        'Jan': 'Ene','Feb': 'Feb','Mar': 'Mar','Apr': 'Abr','May': 'May','Jun': 'Jun',
        'Jul': 'Jul','Aug': 'Ago','Sep': 'Sep','Oct': 'Oct','Nov': 'Nov','Dec': 'Dic'
    }
    for eng, esp in month_map.items():
        date_str = date_str.replace(eng, esp)
    return date_str

def safe_get(dct, *keys, default=""):
    """Devuelve la primera clave existente (admite niveles con puntos)."""
    for k in keys:
        cur = dct
        ok = True
        for part in k.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return default

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def index_participant_files(participants_dir):
    """
    Devuelve dict {comp_id: {"file": path, "event_name": str, "participants": list}}
    Admitimos:
      - fichero es una LISTA de participantes (legacy)
      - fichero es un OBJETO con {"event_id", "event_name", "participants": [...]}
    Detectamos comp_id por:
      1) Clave 'event_id' si existe
      2) Regex UUID en el nombre del archivo
      3) Último grupo tras 'participants_' o 'participantes_' si parece ID
    """
    index = {}
    for path in glob(os.path.join(participants_dir, "*.json")):
        comp_id = None
        event_name = None
        participants = None

        try:
            data = load_json(path)
        except Exception:
            continue

        # Caso OBJETO moderno
        if isinstance(data, dict):
            event_name = data.get("event_name") or data.get("event") or data.get("Competicion_Nombre")
            participants = data.get("participants") or data.get("Participantes") or []
            comp_id = data.get("event_id") or data.get("eventId") or data.get("Competicion_ID")

        # Caso LISTA legacy
        if comp_id is None and isinstance(data, list):
            participants = data
            # no sabemos comp_id; intentamos deducirlo del nombre
            m = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', os.path.basename(path), re.I)
            if m:
                comp_id = m.group(1).lower()
            else:
                # como fallback, usa base sin prefijo
                base = os.path.splitext(os.path.basename(path))[0]
                # patterns: participantes_{nombre}_{id} | participants_{id}
                for pat in (r'participantes_.*_([0-9a-f-]{36})', r'participants_([0-9a-f-]{36})'):
                    m2 = re.search(pat, base, re.I)
                    if m2:
                        comp_id = m2.group(1).lower()
                        break

        if comp_id:
            index[comp_id.lower()] = {
                "file": path,
                "event_name": event_name,
                "participants": participants if isinstance(participants, list) else []
            }
    return index

def normalize_participant(p, i18n=True):
    """
    Normaliza claves del participante a la salida esperada del CSV final.
    Acepta claves may/min y sinónimos en español/inglés.
    """
    def pick(d, *opts):
        for k in opts:
            if k in d and d[k] not in ("", None):
                return d[k]
        # busca case-insensitive
        lower = {str(k).lower(): v for k, v in d.items()}
        for k in opts:
            v = lower.get(k.lower())
            if v not in ("", None):
                return v
        return ""

    return {
        "Participante_ID": pick(p, "ID", "Id", "id", "participant_id"),
        "Dorsal":          pick(p, "Dorsal", "dorsal", "bib", "start", "bib_number"),
        "Guia":            pick(p, "Guia", "guia", "Guide", "guide", "handler", "handler_name"),
        "Perro":           pick(p, "Perro", "perro", "Dog", "dog", "dog_name"),
        "Raza":            pick(p, "Raza", "raza", "Breed", "breed"),
        "Edad":            pick(p, "Edad", "edad", "Age", "age"),
        "Genero":          pick(p, "Genero", "genero", "Sexo", "sexo", "Gender", "gender"),
        "Altura_cm":       pick(p, "Altura_cm", "altura_cm", "Height", "height_cm", "height"),
        "Pedigree":        pick(p, "Pedigree", "pedigree"),
        "Licencia":        pick(p, "Licencia", "licencia", "License", "license"),
        "Federacion":      pick(p, "Federacion", "federacion", "Federation", "federation"),
        "Club_Participante": pick(p, "Club", "club", "Club_Participante", "participant_club"),
        # se rellenan más abajo
        "Grado":           pick(p, "Grado", "grado", "grade", "level"),
        "Categoria":       pick(p, "Categoria", "categoria", "category", "size_class"),
        # Fechas (hasta 10)
        "Fecha_1": "", "Fecha_2": "", "Fecha_3": "", "Fecha_4": "", "Fecha_5": "",
        "Fecha_6": "", "Fecha_7": "", "Fecha_8": "", "Fecha_9": "", "Fecha_10": "",
    }

def main():
    print("=== UNIÓN DE PARTICIPANTES Y COMPETICIONES ===")
    if not os.path.exists(COMPETICIONES_FILE):
        print(f"❌ Archivo no encontrado: {COMPETICIONES_FILE}")
        sys.exit(1)

    try:
        competiciones = load_json(COMPETICIONES_FILE)
    except Exception as e:
        print(f"❌ Error leyendo {COMPETICIONES_FILE}: {e}")
        sys.exit(1)

    print(f"Competiciones cargadas: {len(competiciones)}")
    idx = index_participant_files(PARTICIPANTS_DIR)
    print(f"Ficheros de participantes indexados: {len(idx)}  (dir={PARTICIPANTS_DIR})")

    participantes_completos = []

    for comp in competiciones:
        # ID y nombre de la competición, aceptando variantes
        comp_id = safe_get(comp, "id", "uuid", default="")
        if not comp_id:
            # intentar sacar del enlace
            url_info = safe_get(comp, "enlaces.info", "urls.info", default="")
            m = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', url_info, re.I)
            if m: comp_id = m.group(1).lower()
        comp_id = (comp_id or "").lower()
        comp_nombre = safe_get(comp, "nombre", "title", default="Sin nombre")

        # Metadatos localizados en varias partes
        comp_fechas = safe_get(comp, "fechas", "dates", "informacion_general.fechas_completas", default="")
        comp_org    = safe_get(comp, "organizacion", "organizer", default="")
        comp_club   = safe_get(comp, "club", default="")
        comp_lugar  = safe_get(comp, "lugar", "place", "informacion_general.ubicacion_completa", default="")

        if not comp_id:
            continue

        # localiza fichero de participantes para este comp_id
        entry = idx.get(comp_id)
        if not entry:
            # como fallback, intenta encontrar por nombre
            for k, v in idx.items():
                if v.get("event_name") and comp_nombre and clean_filename(comp_nombre) in clean_filename(v["event_name"]):
                    entry = v; break
            if not entry:
                print(f"⚠️ Participantes no encontrados para ID={comp_id} ({comp_nombre})")
                continue

        participants = entry.get("participants") or []
        if not isinstance(participants, list):
            print(f"⚠️ Estructura inesperada en {entry['file']}")
            continue

        # Unir
        for p in participants:
            row = normalize_participant(p)
            # Fechas por competiciones (si vienen en estructura anidada)
            comp_map = p.get("Competiciones") or p.get("competitions") or {}
            if isinstance(comp_map, dict):
                # grado/categoria: usa la primera si faltan
                if not row["Grado"] or not row["Categoria"]:
                    first = next(iter(comp_map.values()), {})
                    row["Grado"] = row["Grado"] or safe_get(first, "Grado", "grado", "grade", default="")
                    row["Categoria"] = row["Categoria"] or safe_get(first, "Categoria", "categoria", "category", default="")
                i = 1
                for _, cd in comp_map.items():
                    if i > 10: break
                    fecha = safe_get(cd, "Fecha", "fecha", "date", default="")
                    row[f"Fecha_{i}"] = parse_date(fecha)
                    i += 1

            # Añadir metadatos de la competición
            row.update({
                "Competicion_ID": comp_id,
                "Competicion_Nombre": comp_nombre,
                "Competicion_Fechas": comp_fechas,
                "Competicion_Organizacion": comp_org,
                "Competicion_Club": comp_club,
                "Competicion_Lugar": comp_lugar,
            })
            participantes_completos.append(row)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(FINAL_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(participantes_completos, f, ensure_ascii=False, indent=2)

    print(f"✅ Guardados {len(participantes_completos)} registros en {FINAL_OUTPUT_FILE}")

    if participantes_completos:
        s = participantes_completos[0]
        print("\n📋 EJEMPLO:")
        print(f"  Competición: {s['Competicion_Nombre']}")
        print(f"  Participante: {s.get('Guia','')} con {s.get('Perro','')}")
        print(f"  Dorsal: {s.get('Dorsal','')}, Grado: {s.get('Grado','')}, Categoría: {s.get('Categoria','')}")
        print(f"  Fechas: {s.get('Fecha_1','')}, {s.get('Fecha_2','')}")

if __name__ == "__main__":
    main()
