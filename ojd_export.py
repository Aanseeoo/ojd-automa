import os, re, pathlib, sys
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unidecode import unidecode
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ================== CONFIG ==================
SHEET_ID = "1ra1VSpOZ6JuMp-S_MsqNbHEGr2n0VA702lbFsVBD-Os"
SHEET_TAB = os.getenv("SHEET_TAB", "OJD")

LOGIN_URL = "https://www.ojdinteractiva.es/traffic-monitoring/login"
TM_URL    = "https://www.ojdinteractiva.es/traffic-monitoring/traffic-monitoring/0/"

OJD_USER = os.environ["OJD_USER"]   # Repository secrets
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

# --- PING: escribe un sello en A1 para verificar permisos de edición ---
try:
    ws.update('A1', [[f'PING {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}']])
    print("[PING] Escribí en A1 correctamente (permiso de edición OK).")
except Exception as e:
    print(f"[PING][ERROR] No pude escribir en la hoja: {e}")

# ================== UTILIDADES ==================
def tz_now(): return datetime.now(ZoneInfo("Europe/Madrid"))

def day_target():
    """
    - Antes de 12:05 Madrid: usa hoy-3 (porque hoy-2 suele no estar aún).
    - A partir de 12:05 Madrid: usa hoy-2.
    """
    now = tz_now()
    if now.hour < 12 or (now.hour == 12 and now.minute < 5):
        return now - timedelta(days=3)
    return now - timedelta(days=2)

def guard_1215():
    """Solo ejecutar si ya son >= 12:15 Europe/Madrid (por si el cron dispara dos veces)."""
    nm = tz_now()
    if nm.hour < 12 or (nm.hour == 12 and nm.minute < 15):
        print(f"[SKIP] Son {nm.strftime('%H:%M')} Europe/Madrid (< 12:15). No ejecuto.")
        return False
    return True

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(str(s).lower()))

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
    except Exception as e: print(f"[DEBUG] screenshot {step} err: {e}")
    try: html.write_text(page.content(), encoding="utf-8")
    except Exception as e: print(f"[DEBUG] html {step} err: {e}")

def pick_table(tables):
    signals = [
        {"navegadoresunicos","usuariosunicos","usuarios","users","navegadores"},
        {"visitas","sesiones","sessions","visits"},
        {"paginasvistas","pageviews","paginas","pv"},
        {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"},
    ]
    best, best_score = None, -1
    for t in tables:
        cols = {norm(c) for c in t.columns}
        score = sum(any(any(s in col for col in cols) for s in group) for group in signals)
        if score > best_score:
            best, best_score = t, score
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
    print(f"[WRITE] Filas a escribir (sin cabecera): {len(df)}")
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
    print("[WRITE] OK")

# ================== PLAYWRIGHT FLOW ==================
def login_tm(page):
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    dbg(page, "01_login_page")

    # Cerrar banner de cookies si aparece
    for txt in ["ACEPTAR TODO","Aceptar todo","Aceptar cookies","RECHAZAR","Rechazar todo"]:
        try:
            btn = page.get_by_role("button", name=re.compile(txt, re.I))
            if btn.count():
                btn.first.click()
                break
        except: pass

    # Login por placeholder (según tu captura: "Usuario" y "Contraseña")
    page.get_by_placeholder("Usuario").fill(OJD_USER)
    page.get_by_placeholder("Contraseña").fill(OJD_PASS)
    page.get_by_role("button", name=re.compile(r"Acceder", re.I)).click()
    page.wait_for_load_state("networkidle")
    dbg(page, "02_after_login")

    # Comprobar que no seguimos en login
    if page.locator("h1:has-text('Inicio de sesión')").count():
        raise RuntimeError("No se pudo iniciar sesión: pantalla de login sigue visible.")

def open_tm_and_set_day(page, target_date: datetime):
    page.goto(TM_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    dbg(page, "03_tm_loaded")

    # Fijar día (hoy-2 o hoy-3 según la hora)
    ymd = target_date.strftime("%Y-%m-%d")
    ddmmyyyy = target_date.strftime("%d/%m/%Y")

    filled = False
    for sel in [
        'input[type="date"]',
        'input[name*="dia"]','input[id*="dia"]',
        'input[name*="date"]','input[id*="date"]',
        'input[placeholder*="dd"]','input[placeholder*="día"]','input[placeholder*="Dia"]'
    ]:
        try:
            if page.locator(sel).first.count():
                page.locator(sel).first.fill(ymd)
                page.keyboard.press("Enter")
                filled = True
                break
        except: pass

    if not filled:
        try:
            page.evaluate("""(dmy) => {
                const inputs = Array.from(document.querySelectorAll('input'))
                    .filter(i => i.offsetParent && i.type !== 'hidden');
                if (inputs[0]) {
                    inputs[0].value = dmy;
                    inputs[0].dispatchEvent(new Event('input', {bubbles:true}));
                    inputs[0].dispatchEvent(new Event('change', {bubbles:true}));
                }
            }""", ddmmyyyy)
        except: pass

    page.wait_for_load_state("networkidle")
    dbg(page, "04_after_set_day")

    # Click en "Buscar"
    clicked = False
    for sel in [
        "button:has-text('Buscar')",
        "input[type='submit'][value*='Buscar']",
        "button[title*='Buscar']",
        'text=/^\\s*Buscar\\s*$/i'
    ]:
        if page.locator(sel).first.count():
            page.locator(sel).first.click()
            clicked = True
            break
    if not clicked:
        print("[WARN] No encontré botón 'Buscar'. Envío Enter.")
        page.keyboard.press("Enter")

    page.wait_for_load_state("networkidle")
    dbg(page, "05_after_search")

def extract_table(page) -> pd.DataFrame:
    try:
        page.wait_for_selector("table", timeout=30000)
    except PWTimeout:
        dbg(page, "05b_no_table")
        raise RuntimeError("No apareció ninguna tabla tras 'Buscar'.")
    html = page.content()
    tables = pd.read_html(html)
    if not tables:
        raise RuntimeError("No se encontraron tablas en el HTML.")
    df = pick_table(tables)
    if df is None or df.empty:
        raise RuntimeError("Tabla principal vacía o no identificable.")
    return df

# ================== MAIN ==================
def run():
    print("[START] ojd_export.py arrancó")

    if not guard_1215():
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # 1) Login
        login_tm(page)

        # 2) Día objetivo y Buscar
        target = day_target()
        print(f"[INFO] Fecha objetivo: {target.strftime('%Y-%m-%d')}")
        open_tm_and_set_day(page, target)

        # 3) Extraer tabla
        df = extract_table(page)
        dbg(page, "06_table_captured")
        print(f"[INFO] Filas totales detectadas en tabla: {len(df)}")

        # 4) Detectar columna del medio
        candidates = [c for c in df.columns if norm(c) in {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"}]
        media_col = candidates[0] if candidates else df.columns[0]
        print(f"[INFO] Columna de medio detectada: {media_col}")

        # 5) Filtrar dominios
        df = filter_keep(df, media_col)
        print(f"[INFO] Filas tras filtrar dominios: {len(df)}")

        # 6) Formato final
        out = shape_output(df, media_col, forced_date=target)
        if out.empty:
            print("[WARN] No hubo coincidencias; escribiré solo cabeceras.")
            out = pd.DataFrame(columns=["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"])

        # 7) Escribir a Sheets
        print(f"[WRITE] Voy a escribir {len(out)} filas (sin cabecera) en '{SHEET_TAB}' del Sheet {SHEET_ID}")
        write_overwrite(out)
        dbg(page, "07_done")

        browser.close()

if __name__ == "__main__":
    run()
