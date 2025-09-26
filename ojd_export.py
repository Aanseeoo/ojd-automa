import os, re, pathlib
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

OJD_USER = os.environ["OJD_USER"]
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

# ================== UTILIDADES ==================
def tz_now(): return datetime.now(ZoneInfo("Europe/Madrid"))

def day_target():
    # Antes de 12:05 -> hoy-3; a partir de 12:05 -> hoy-2
    now = tz_now()
    return (now - timedelta(days=3)) if (now.hour < 12 or (now.hour == 12 and now.minute < 5)) else (now - timedelta(days=2))

def guard_1215():
    nm = tz_now()
    if nm.hour < 12 or (nm.hour == 12 and nm.minute < 15):
        print(f"[SKIP] {nm.strftime('%H:%M')} Europe/Madrid (<12:15) -> no ejecuto.")
        return False
    return True

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(str(s).lower()))

# Mapeo final de nombres (como quieres verlos en la hoja)
TARGET_NAMES = {
    "ultimahora": "ULTIMAHORA.ES",
    "diariodemallorca": "DIARIODEMALLORCA.ES",
    "diariodeibiza": "DIARIODEIBIZA.ES",
    "mallorcamagazin": "MALLORCAMAGAZIN.COM",
    "mallorcazeitung": "MALLORCAZEITUNG.ES",
    "majorcadaily": "MAJORCADAILYBULLETIN.COM",
    "majorcadailybulletin": "MAJORCADAILYBULLETIN.COM",
}

ALIASES = {
    "ultimahora": {"ultimahora.es","ultimahora","ultima hora","última hora"},
    "diariodemallorca": {"diariodemallorca.es","diariodemallorca","diario de mallorca"},
    "diariodeibiza": {"diariodeibiza.es","diariodeibiza","diario de ibiza"},
    "mallorcamagazin": {"mallorcamagazin.es","mallorca magazin","mallorcamagazin.com"},
    "mallorcazeitung": {"mallorcazeitung.es","mallorca zeitung"},
    "majorcadaily": {"majorcadailybulletin.es","majorcadaily","majorca daily bulletin","majorca daily","majorcadailybulletin.com"},
}
ORDER = list(TARGET_NAMES.keys())

DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
def dbg(page, step):
    try:
        page.screenshot(path=str(DEBUG_DIR / f"{step}.png"), full_page=True)
        (DEBUG_DIR / f"{step}.html").write_text(page.content(), encoding="utf-8")
    except Exception as e:
        print(f"[DEBUG] {step}: {e}")

def pick_table(tables):
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

def canonical_name(val: str) -> str | None:
    n = norm(val)
    for key, aliases in ALIASES.items():
        if any(norm(a) in n for a in aliases):
            return TARGET_NAMES[key]
    return None

def to_int(x):
    # "232.762" -> 232762 ; "81,379" -> 81379 ; vacíos -> None
    s = str(x).strip()
    if s.lower() in {"", "nan", "none", "null"}:
        return None
    s = s.replace(".", "").replace(",", "")
    return int(s) if s.isdigit() else None

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
    out["Nombre"] = df[media_col].map(canonical_name)
    out["Navegadores Únicos"] = df[navu_col].map(to_int) if navu_col in df else None
    out["Visitas"] = df[visitas_col].map(to_int) if visitas_col in df else None
    out["Páginas Vistas"] = df[pv_col].map(to_int) if pv_col in df else None

    out = out.dropna(subset=["Nombre"]).reset_index(drop=True)
    # Orden fijo
    order_index = {TARGET_NAMES[k]: i for i,k in enumerate(ORDER)}
    out["__ord"] = out["Nombre"].map(lambda x: order_index.get(x, 999))
    out = out.sort_values("__ord").drop(columns="__ord")
    return out

def write_append_and_dedupe(df_new: pd.DataFrame):
    # Lee lo que ya hay (si hay algo)
    existing_vals = ws.get_all_values()
    if not existing_vals:
        ws.update([df_new.columns.tolist()] + df_new.astype(object).where(pd.notna(df_new), "").values.tolist())
        print(f"[WRITE] Nueva hoja con {len(df_new)} filas.")
        return

    header = existing_vals[0]
    data = existing_vals[1:]
    df_old = pd.DataFrame(data, columns=header)
    # Normaliza tipos
    if "Fecha" in df_old.columns:
        # mantén texto tal cual
        pass
    for c in ["Navegadores Únicos","Visitas","Páginas Vistas"]:
        if c in df_old.columns:
            df_old[c] = pd.to_numeric(df_old[c].str.replace(".","",regex=False).str.replace(",","",regex=False), errors="coerce").astype("Int64")

    # Concat + dedupe por (Fecha, Nombre)
    combo = pd.concat([df_old, df_new], ignore_index=True)
    combo = combo.drop_duplicates(subset=["Fecha","Nombre"], keep="last")
    # Ordena por Fecha asc y orden fijo de Nombre
    order_index = {TARGET_NAMES[k]: i for i,k in enumerate(ORDER)}
    combo["__ord"] = combo["Nombre"].map(lambda x: order_index.get(x, 999))
    combo = combo.sort_values(["Fecha","__ord"]).drop(columns="__ord")

    ws.clear()
    ws.update([combo.columns.tolist()] + combo.astype(object).where(pd.notna(combo), "").values.tolist())
    print(f"[WRITE] Guardadas {len(combo)} filas (histórico).")

# ================== PLAYWRIGHT ==================
def login_tm(page):
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    dbg(page, "01_login_page")

    # Cerrar cookies si tapa el botón
    for txt in ["ACEPTAR TODO","Aceptar todo","Aceptar cookies","RECHAZAR","Rechazar todo"]:
        try:
            btn = page.get_by_role("button", name=re.compile(txt, re.I))
            if btn.count():
                btn.first.click(); break
        except: pass

    page.get_by_placeholder("Usuario").fill(OJD_USER)
    page.get_by_placeholder("Contraseña").fill(OJD_PASS)
    page.get_by_role("button", name=re.compile(r"Acceder", re.I)).click()
    page.wait_for_load_state("networkidle")
    dbg(page, "02_after_login")

    if page.locator("h1:has-text('Inicio de sesión')").count():
        raise RuntimeError("No se pudo iniciar sesión: pantalla de login sigue visible.")

def open_tm_and_search(page, target_date: datetime):
    page.goto(TM_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    dbg(page, "03_tm_loaded")

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

    clicked = False
    for sel in [
        "button:has-text('Buscar')",
        "input[type='submit'][value*='Buscar']",
        "button[title*='Buscar']",
        'text=/^\\s*Buscar\\s*$/i'
    ]:
        if page.locator(sel).first.count():
            page.locator(sel).first.click(); clicked = True; break
    if not clicked:
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
    print("[START] ojd_export.py")

    # Guardia 12:15 España
    if not guard_1215():
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # 1) Login
        login_tm(page)

        # 2) Fijar fecha y buscar
        target = day_target()
        print(f"[INFO] Fecha objetivo: {target.strftime('%Y-%m-%d')}")
        open_tm_and_search(page, target)

        # 3) Tabla
        df = extract_table(page)
        dbg(page, "06_table_captured")
        print(f"[INFO] Filas en tabla: {len(df)}")

        # 4) Columna de medio
        candidates = [c for c in df.columns if norm(c) in {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"}]
        media_col = candidates[0] if candidates else df.columns[0]
        print(f"[INFO] Columna medio: {media_col}")

        # 5) Formato final (normaliza nombre y números)
        out = shape_output(df, media_col, forced_date=target)
        print(f"[INFO] Filas tras filtro de dominios: {len(out)}")

        # 6) Escribir: APPEND + DEDUPE
        if out.empty:
            print("[WARN] No hubo coincidencias; no escribo.")
        else:
            write_append_and_dedupe(out)
            dbg(page, "07_done")

        browser.close()

if __name__ == "__main__":
    run()

