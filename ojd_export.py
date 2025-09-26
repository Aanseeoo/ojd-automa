import os, re, pathlib
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unidecode import unidecode
from playwright.sync_api import sync_playwright

# ================== CONFIG ==================
SHEET_ID = "1ra1VSpOZ6JuMp-S_MsqNbHEGr2n0VA702lbFsVBD-Os"
SHEET_TAB = os.getenv("SHEET_TAB", "OJD")

LOGIN_URL = "https://www.ojdinteractiva.es/traffic-monitoring/login"
TM_URL    = "https://www.ojdinteractiva.es/traffic-monitoring/traffic-monitoring/0/"

OJD_USER = os.environ["OJD_USER"]   # Secrets en GitHub
OJD_PASS = os.environ["OJD_PASS"]

# ================== GOOGLE SHEETS ==================
import gspread
from google.oauth2.service_account import Credentials
from json import loads as json_loads

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds_json = json_loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(SHEET_TAB)
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=SHEET_TAB, rows=2000, cols=10)

# --- PING opcional: escribe un sello de tiempo en A1 para verificar que SÍ podemos escribir ---
try:
    ws.update('A1', [[f'PING {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}']])
    print("[PING] Escribí en A1 correctamente (permiso de edición OK).")
except Exception as e:
    print(f"[PING][ERROR] No pude escribir en la hoja: {e}")

# ================== UTILIDADES ==================
def tz_now(): return datetime.now(ZoneInfo("Europe/Madrid"))
def day_minus(n): return (tz_now() - timedelta(days=n))
def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(str(s).lower()))

# Dominios exactos a conservar (con alias para nombres comerciales)
ALIASES = {
    "ultimahora.es": {"ultimahora.es","ultimahora","ultima hora","última hora"},
    "diariodemallorca.es": {"diariodemallorca.es","diariodemallorca","diario de mallorca"},
    "diariodeibiza.es": {"diariodeibiza.es","diariodeibiza","diario de ibiza"},
    "mallorcamagazin.es": {"mallorcamagazin.es","mallorca magazin"},
    "mallorcazeitung.es": {"mallorcazeitung.es","mallorca zeitung"},
    "majorcadailybulletin.es": {"majorcadailybulletin.es","majorcadaily","majorca daily bulletin","majorca daily"},
}
ORDER = list(ALIASES.keys())

DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
def dbg(page, step):
    png = DEBUG_DIR / f"{step}.png"
    html = DEBUG_DIR / f"{step}.html"
    try: page.screenshot(path=str(png), full_page=True)
    except: pass
    try: html.write_text(page.content(), encoding="utf-8")
    except: pass

def pick_table(tables):
    # Heurística: tabla con métricas típicas
    signals = [
        {"navegadoresunicos","usuariosunicos","usuarios","users","navegadores"},
        {"visitas","sesiones","sessions","visits"},
        {"paginasvistas","pageviews","paginas","pv"},
        {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"},
    ]
    best, score_best = None, -1
    for t in tables:
        cols = {norm(c) for c in t.columns}
        score = sum(any(any(s in col for col in cols) for s in group) for group in signals)
        if score > score_best:
            best, score_best = t, score
    return best

def filter_keep(df: pd.DataFrame, media_col: str) -> pd.DataFrame:
    def matches(val: str) -> bool:
        n = norm(val)
        for al in ALIASES.values():
            if any(norm(a) in n for a in al): return True
        return False
    keep = df[df[media_col].astype(str).apply(matches)].copy()

    def ord_key(v):
        n = norm(str(v))
        for i, dom in enumerate(ORDER):
            if any(norm(a) in n for a in ALIASES[dom]): return i
        return 999
    keep["__ord"] = keep[media_col].apply(ord_key)
    keep = keep.sort_values("__ord").drop(columns="__ord")
    return keep

def shape_output(df: pd.DataFrame, media_col: str, forced_date: datetime) -> pd.DataFrame:
    def find_col(cands):
        for c in df.columns:
            nc = norm(c)
            if any(nc == x or x in nc for x in cands):
                return c
        return None
    navu_col   = find_col({"navegadoresunicos","usuariosunicos","usuarios","users"})
    visitas_col= find_col({"visitas","sesiones","sessions","visits"})
    pv_col     = find_col({"paginasvistas","pageviews","paginas","pv"})

    out = pd.DataFrame()
    out["Fecha"] = forced_date.strftime("%Y-%m-%d")
    out["Nombre"] = df[media_col].astype(str)
    out["Navegadores Únicos"] = df[navu_col] if navu_col in df else ""
    out["Visitas"] = df[visitas_col] if visitas_col in df else ""
    out["Páginas Vistas"] = df[pv_col] if pv_col in df else ""
    return out

def write_overwrite(df: pd.DataFrame):
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

# =============
