import os, re
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from unidecode import unidecode
from playwright.sync_api import sync_playwright

# --------- Config ---------
SHEET_ID = "1ra1VSpOZ6JuMp-S_MsqNbHEGr2n0VA702lbFsVBD-Os"  # tu nuevo Sheet
SHEET_TAB = os.getenv("SHEET_TAB", "OJD")                  # pestaña destino

OJD_USER = os.environ["OJD_USER"]          # Secret en GitHub
OJD_PASS = os.environ["OJD_PASS"]          # Secret en GitHub

BASE_URL = "https://www.ojdinteractiva.es/"
TM_URL   = "https://www.ojdinteractiva.es/traffic-monitoring/traffic-monitoring/0/"

# --------- Google Sheets auth (Service Account vía Secret JSON) ---------
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

# --------- Utilidades ---------
WHITELIST = {
    "ultimahora",
    "diariodemallorca",
    "mallorcamagazin",
    "mallorcazeitung",
    "diariodeibiza",
    "majorcadaily",
}
ORDER = [
    "ultimahora",
    "diariodemallorca",
    "mallorcamagazin",
    "mallorcazeitung",
    "diariodeibiza",
    "majorcadaily",
]

def now_madrid():
    return datetime.now(ZoneInfo("Europe/Madrid"))

def norm(s: str) -> str:
    s = unidecode(str(s).lower())
    return re.sub(r"[^a-z0-9]", "", s)

def pick_table(tables):
    # Heurística para elegir la tabla "principal" de métricas
    must_any = [
        {"navegadoresunicos","usuariosunicos","usuarios","users","navegadores"},
        {"visitas","sesiones","sessions","visits"},
        {"paginasvistas","pageviews","paginas","pv"},
        {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"},
        {"fecha","date","dia","day"},
    ]
    best, best_score = None, -1
    for t in tables:
        cols = {norm(c) for c in t.columns}
        score = sum(any(any(x in col for col in cols) for x in group) for group in must_any)
        if score > best_score:
            best, best_score = t, score
    return best

def filter_media(df: pd.DataFrame, media_col: str) -> pd.DataFrame:
    df["__norm"] = df[media_col].apply(norm)
    mask = df["__norm"].apply(lambda x: any(k in x for k in WHITELIST))
    df = df[mask].copy()
    df["__ord"] = df[media_col].apply(lambda x: next((i for i,k in enumerate(ORDER) if k in norm(x)), 999))
    df = df.sort_values("__ord").drop(columns=["__norm","__ord"])
    return df

def rename_and_shape(df: pd.DataFrame, media_col: str) -> pd.DataFrame:
    # Mapea nombres de columnas al formato: Fecha, Nombre, Navegadores Únicos, Visitas, Páginas Vistas
    def find_col(cands):
        for c in df.columns:
            nc = norm(c)
            if any(nc == x or x in nc for x in cands):
                return c
        return None

    fecha_col  = find_col({"fecha","date","dia","day"})
    nombre_col = media_col
    navu_col   = find_col({"navegadoresunicos","usuariosunicos","usuarios","users","unique"})
    visitas_col= find_col({"visitas","sesiones","sessions","visits"})
    pv_col     = find_col({"paginasvistas","pageviews","paginas","pv"})

    if not fecha_col:
        fecha_col = "Fecha"
        df[fecha_col] = now_madrid().strftime("%Y-%m-%d")

    out = pd.DataFrame()
    out["Fecha"] = pd.to_datetime(df[fecha_col]).dt.strftime("%Y-%m-%d")
    out["Nombre"] = df[nombre_col]
    out["Navegadores Únicos"] = df[navu_col] if navu_col in df else ""
    out["Visitas"] = df[visitas_col] if visitas_col in df else ""
    out["Páginas Vistas"] = df[pv_col] if pv_col in df else ""
    return out

def write_sheet_overwrite(df: pd.DataFrame):
    ws.clear()
    if df.empty:
        ws.update([["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]])
        return
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

def run():
    # Solo ejecuta si son ≥ 12:05 (hora Madrid), por seguridad con UTC/verano-invierno
    nm = now_madrid()
    if nm.hour < 12 or (nm.hour == 12 and nm.minute < 5):
        print("Antes de las 12:05 Europe/Madrid; no hago nada.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # 1) Abre base y login
        page.goto(BASE_URL, wait_until="domcontentloaded")

        # 2) LOGIN (intenta selectores comunes; si tu login usa otros, te los ajusto)
        try:
            page.wait_for_selector('input[name="username"], input[type="email"], input#username', timeout=7000)
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

        # 3) Ir a Traffic Monitoring (directo y/o por menú)
        page.goto(TM_URL, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        for candidate in [
            'role=link[name="Traffic Monitoring"]',
            'text=Traffic Monitoring',
            'a:has-text("Traffic Monitoring")',
            'button:has-text("Traffic Monitoring")'
        ]:
            try:
                page.locator(candidate).first.click(timeout=2000)
                break
            except:
                pass

        # 4) Leer tabla
        page.wait_for_selector("table", timeout=30000)
        html = page.content()
        tables = pd.read_html(html)
        if not tables:
            raise RuntimeError("No se encontraron tablas.")
        df = pick_table(tables)
        if df is None or df.empty:
            raise RuntimeError("No se pudo identificar la tabla principal.")

        # 5) Detectar columna "Nombre" del medio y filtrar 6 medios
        candidates = [c for c in df.columns if norm(c) in {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"}]
        media_col = candidates[0] if candidates else df.columns[0]
        df = filter_media(df, media_col)

        # 6) Formatear columnas finales y escribir en la pestaña OJD
        df = rename_and_shape(df, media_col)
        write_sheet_overwrite(df)

        browser.close()

if __name__ == "__main__":
    run()
