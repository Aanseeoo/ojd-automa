import os, re, pathlib
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unidecode import unidecode
from playwright.sync_api import sync_playwright

# ---------- Config ----------
SHEET_ID = "1ra1VSpOZ6JuMp-S_MsqNbHEGr2n0VA702lbFsVBD-Os"
SHEET_TAB = os.getenv("SHEET_TAB", "OJD")
BASE_URL = "https://www.ojdinteractiva.es/"
TM_URL   = "https://www.ojdinteractiva.es/traffic-monitoring/traffic-monitoring/0/"

OJD_USER = os.environ["OJD_USER"]          # Secrets en GitHub
OJD_PASS = os.environ["OJD_PASS"]
# ---------- Google Sheets (Service Account vía Secret JSON) ----------
import gspread
from google.oauth2.service_account import Credentials
from json import loads as json_loads
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
creds_json = json_loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(SHEET_TAB)
except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=SHEET_TAB, rows=2000, cols=10)

# ---------- Utilidades ----------
def tz_now(): return datetime.now(ZoneInfo("Europe/Madrid"))
def today_minus(n): return (tz_now() - timedelta(days=n))

DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
def dbg(page, step):
    # Guarda captura y HTML del paso
    png = DEBUG_DIR / f"{step}.png"
    html = DEBUG_DIR / f"{step}.html"
    try:
        page.screenshot(path=str(png), full_page=True)
    except Exception as e:
        print(f"[DEBUG] screenshot error {step}: {e}")
    try:
        html.write_text(page.content(), encoding="utf-8")
    except Exception as e:
        print(f"[DEBUG] html save error {step}: {e}")

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(str(s).lower()))

# Dominios exactos que mencionaste (con algunos alias por si el panel usa nombre comercial)
ALIASES = {
    "ultimahora.es": {"ultimahora.es","ultimahora","últimahora","ultima hora"},
    "diariodemallorca.es": {"diariodemallorca.es","diariodemallorca","diario de mallorca"},
    "diariodeibiza.es": {"diariodeibiza.es","diariodeibiza","diario de ibiza"},
    "mallorcamagazin.es": {"mallorcamagazin.es","mallorca magazin"},
    "mallorcazeitung.es": {"mallorcazeitung.es","mallorca zeitung"},
    "majorcadailybulletin.es": {"majorcadailybulletin.es","majorcadaily","majorca daily bulletin","majorca daily"},
}
ORDER = list(ALIASES.keys())

def row_matches(val: str) -> bool:
    n = norm(val)
    for _, al in ALIASES.items():
        for a in al:
            if norm(a) in n:
                return True
    return False

def filter_keep(df: pd.DataFrame, media_col: str) -> pd.DataFrame:
    keep = df[df[media_col].astype(str).apply(row_matches)].copy()
    # Ordenar según ORDER
    def key(v):
        n = norm(str(v))
        for i, dom in enumerate(ORDER):
            for a in ALIASES[dom]:
                if norm(a) in n:
                    return i
        return 999
    keep["__ord"] = keep[media_col].apply(key)
    keep = keep.sort_values("__ord").drop(columns="__ord")
    return keep

def pick_table(tables):
    # Heurística: tabla con columnas de métricas
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

def format_output(df: pd.DataFrame, media_col: str, forced_date: datetime) -> pd.DataFrame:
    # intenta detectar cols métricas
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
    out["Fecha"] = forced_date.strftime("%Y-%m-%d")  # usamos explícitamente hoy-2
    out["Nombre"] = df[media_col].astype(str)
    out["Navegadores Únicos"] = df[navu_col] if navu_col in df else ""
    out["Visitas"] = df[visitas_col] if visitas_col in df else ""
    out["Páginas Vistas"] = df[pv_col] if pv_col in df else ""
    return out

def write_sheet_overwrite(df: pd.DataFrame):
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

# ---------- Flujo Playwright ----------
def do_login_and_go_to_tm(page):
    page.goto(BASE_URL, wait_until="domcontentloaded")
    dbg(page, "01_loaded")
    # Login genérico (probamos varios selectores comunes)
    try:
        page.wait_for_selector('input[name="username"], input[type="email"], input#username, input[name="user"]', timeout=8000)
        for sel_u in ['input[name="username"]','input[type="email"]','input#username','input[name="user"]']:
            if page.locator(sel_u).count():
                page.fill(sel_u, OJD_USER); break
        for sel_p in ['input[name="password"]','input[type="password"]','input#password','input[name="pass"]']:
            if page.locator(sel_p).count():
                page.fill(sel_p, OJD_PASS); break
        for sel_b in ['button[type="submit"]','input[type="submit"]','button:has-text("Acceder")','button:has-text("Entrar")','text=Login','text=Iniciar sesión']:
            if page.locator(sel_b).count():
                page.click(sel_b); break
    except:
        pass
    page.wait_for_load_state("networkidle")
    dbg(page, "02_after_login")

    # Intento directo a Traffic Monitoring
    page.goto(TM_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    # Por si requiere click explícito en menú
    for candidate in [
        'role=link[name="Traffic Monitoring"]',
        'text=Traffic Monitoring',
        'a:has-text("Traffic Monitoring")',
        'button:has-text("Traffic Monitoring")'
    ]:
        try:
            if page.locator(candidate).first.count():
                page.locator(candidate).first.click(timeout=1500)
                page.wait_for_load_state("networkidle")
                break
        except:
            pass
    dbg(page, "03_tm")

def set_day_to_today_minus_2(page):
    """Abre el selector 'Día' y fija la fecha a hoy-2 (Madrid). Varias estrategias."""
    target = today_minus(2)
    dmy = target.strftime("%d/%m/%Y")      # formato común en ES
    ymd = target.strftime("%Y-%m-%d")      # para inputs type=date
    # 1) Click en control que contenga texto 'Día' o 'Dia'
    opened = False
    for sel in ['text=Día', 'text=Dia', 'label:has-text("Día")', 'label:has-text("Dia")']:
        try:
            if page.locator(sel).first.count():
                page.locator(sel).first.click(timeout=1500)
                opened = True
                break
        except: pass
    # 2) Rellenar input de fecha (type=date u otros)
    for inp in ['input[type="date"]','input[placeholder*="dd"]','input[name*="date"]','input[name*="dia"]','input[id*="date"]']:
        try:
            if page.locator(inp).first.count():
                # prueba con yyyy-mm-dd y con dd/mm/yyyy
                page.fill(inp, ymd)
                page.keyboard.press("Enter")
                opened = True
                break
        except: pass
    if not opened:
        # último recurso: intenta asignar por JS a cualquier input visible con longitud razonable
        try:
            page.evaluate("""(dmy, ymd) => {
                const cand = Array.from(document.querySelectorAll('input')).find(i => i.offsetParent && i.type!=='hidden');
                if (cand) { cand.value = dmy; cand.dispatchEvent(new Event('input',{bubbles:true})); cand.dispatchEvent(new Event('change',{bubbles:true})); }
            }""", dmy, ymd)
        except: pass
    page.wait_for_load_state("networkidle")
    dbg(page, "04_after_date")

def extract_table_df(page):
    page.wait_for_selector("table", timeout=30000)
    html = page.content()
    tables = pd.read_html(html)
    if not tables:
        raise RuntimeError("No se encontraron tablas en la página.")
    df = pick_table(tables)
    if df is None or df.empty:
        raise RuntimeError("No se pudo identificar la tabla principal.")
    return df

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        do_login_and_go_to_tm(page)
        set_day_to_today_minus_2(page)

        # Extraer tabla
        df = extract_table_df(page)
        dbg(page, "05_table")

        # Detecta columna del medio
        candidates = [c for c in df.columns if norm(c) in {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"}]
        media_col = candidates[0] if candidates else df.columns[0]

        # Filtra solo tus dominios
        df = filter_keep(df, media_col)

        # Formatea a las 5 columnas y fuerza la fecha = hoy-2
        out = format_output(df, media_col, forced_date=today_minus(2))

        # Si quedó vacío, deja cabecera para que lo veamos claro
        if out.empty:
            out = pd.DataFrame(columns=["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"])

        write_sheet_overwrite(out)
        dbg(page, "06_done")
        browser.close()

if __name__ == "__main__":
    run()
