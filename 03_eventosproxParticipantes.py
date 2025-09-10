def _load_01events_df():
    """
    Carga 01events_last.json (o 01events.json) y devuelve un DataFrame con
    columnas: event_url, nombre, club, organizacion, lugar, fechas.
    Busca en ./output y ./artifacts por si el 04 lo ejecuta desde artifacts.
    """
    import pandas as pd
    from pathlib import Path

    candidates = [
        Path(OUT_DIR) / "01events_last.json",
        Path(OUT_DIR) / "01events.json",
        Path("./artifacts") / "01events_last.json",
        Path("./artifacts") / "01events.json",
        Path("01events_last.json"),
        Path("01events.json"),
    ]
    path = next((str(p) for p in candidates if p.exists()), None)
    if not path:
        print("(Aviso) No se encontró 01events_last.json / 01events.json; sigo sin enriquecer desde 01.")
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
        if not isinstance(j, list):
            print(f"(Aviso) 01events en formato inesperado ({path}); sigo sin enriquecer desde 01.")
            return None
        df = pd.DataFrame(j)
    except Exception as e:
        print(f"(Aviso) No se pudo leer {path}: {e}")
        return None

    # Asegura columnas esperadas
    for c in ["event_url","nombre","club","organizacion","lugar","fechas"]:
        if c not in df.columns:
            df[c] = ""
    # Quita duplicados por URL
    df = df.drop_duplicates(subset=["event_url"])
    return df[["event_url","nombre","club","organizacion","lugar","fechas"]].copy()
