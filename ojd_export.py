import os, re, pathlib
import pandas as pd
from datetime import datetime, date, timedelta
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
from gspread_formatting import CellFormat, numberFormat, format_cell_range

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

# 🔠 Nombres objetivo exactamente como te los piden:
TARGET_NAMES = {
    "ultimahora": "ULTIMAHORA.ES",
    "diariodemallorca": "DIARIODEMALLORCA.ES",
    "diariodeibiza": "DIARIODEIBIZA.ES",
    "mallorcamagazin": "MALLORCAMAGAZIN.COM",
    "mallorcazeitung": "MALLORCAZEITUNG.COM",   # <- .COM según tu especificación final
    "majorcadaily": "MAJORCADAILYBULLETIN.COM",
    "majorcadailybulletin": "MAJORCADAILYBULLETIN.COM",
}

ALIASES = {
    "ultimahora": {"ultimahora.es","ultimahora","ultima hora","última hora"},
    "diariodemallorca": {"diariodemallorca.es","diariodemallorca","diario de mallorca"},
    "diariodeibiza": {"diariodeibiza.es","diariodeibiza","diario de ibiza"},
    "mallorcamagazin": {"mallorcamagazin.es","mallorca magazin","mallorcamagazin.com"},
    "mallorcazeitung": {"mallorcazeitung.es","mallorca zeitung","mallorcazeitung.com"},
    "majorcadaily": {"majorcadailybulletin.es","majorcadaily","majorca daily bulletin","majorca daily","majorcadailybulletin.com"},
}

ORDER = ["ultimahora","diariodemallorca","diariodeibiza","mallorcamagazin","mallorcazeitung","majorcadaily"]

# Semana en español (lunes=0)
WEEKDAYS_ES = ["lunes","martes","miércoles","jueves","viernes","sábado","domingo"]

# Debug
DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
def dbg(page, step):
    try:
        page.screenshot(path=str(DEBUG_DIR / f"{step}.png"), full_page=True)
        (DEBUG_DIR / f"{step}.html").write_text(page.content(), encoding="utf-8")
    except Exception as e:
        print(f"[DEBUG] {step}: {e}")

# Google Sheets usa "serial" (días desde 1899-12-30)
def gs_date_serial(d: date) -> int:
    return (d - date(1899, 12, 30)).days

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

# ================== ESCRITURA: OJD (histórico por medio) ==================
def write_append_and_dedupe_types(df_new: pd.DataFrame):
    """
    Mantiene histórico en 'OJD' con tipos correctos (Fecha=serial, métricas=número)
    """
    # Leer existente
    vals = ws.get_all_values()
    if not vals:
        header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
        data = df_new.astype(object).where(pd.notna(df_new), "").values.tolist()
        _write_with_formats([[*header]] + _to_serials(data))
        return

    header = vals[0]
    data = vals[1:]
    df_old = pd.DataFrame(data, columns=header)

    # Tipos
    if "Fecha" in df_old.columns:
        # viene como texto visible, lo dejamos tal cual y volvemos a escribir serial más abajo
        pass
    for c in ["Navegadores Únicos","Visitas","Páginas Vistas"]:
        if c in df_old.columns:
            df_old[c] = pd.to_numeric(df_old[c].str.replace(".","",regex=False).str.replace(",","",regex=False), errors="coerce").astype("Int64")

    # Concat y dedupe
    combo = pd.concat([df_old, df_new], ignore_index=True)
    combo = combo.drop_duplicates(subset=["Fecha","Nombre"], keep="last")
    # Orden
    order_index = {TARGET_NAMES[k]: i for i,k in enumerate(ORDER)}
    combo["__ord"] = combo["Nombre"].map(lambda x: order_index.get(x, 999))
    combo = combo.sort_values(["Fecha","__ord"]).drop(columns="__ord")

    # Escribir con formatos
    rows = combo.astype(object).where(pd.notna(combo), "").values.tolist()
    _write_with_formats([[*combo.columns.tolist()]] + _to_serials(rows))

def _to_serials(rows):
    """
    Convierte Fecha textual 'YYYY-MM-DD' (col 0) a serial de Google (número).
    """
    out = []
    for r in rows:
        rr = list(r)
        try:
            dt = datetime.strptime(str(rr[0]), "%Y-%m-%d").date()
            rr[0] = gs_date_serial(dt)
        except Exception:
            pass
        # C/D/E a int si procede
        for i in [2,3,4]:
            try:
                rr[i] = int(rr[i]) if rr[i] not in ("", None, "nan") else ""
            except Exception:
                pass
        out.append(rr)
    return out

def _write_with_formats(values):
    ws.clear()
    ws.update(range_name=f"{SHEET_TAB}!A1", values=values)
    fmt_date = CellFormat(numberFormat=numberFormat(type="DATE", pattern="yyyy-mm-dd"))
    fmt_num  = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0"))
    try:
        format_cell_range(ws, "A2:A10000", fmt_date)  # Fecha
        format_cell_range(ws, "C2:E10000", fmt_num)   # Métricas
    except Exception as e:
        print(f"[FORMAT][WARN] {e}")

# ================== HOJAS DE PAREJAS (histórico por día) ==================
PAIR_SHEETS = {
    "UH-DM": ("ULTIMAHORA.ES","DIARIODEMALLORCA.ES"),
    "UH-DI": ("ULTIMAHORA.ES","DIARIODEIBIZA.ES"),
    "MM-MZ": ("MALLORCAMAGAZIN.COM","MALLORCAZEITUNG.COM"),
}
SINGLE_SHEET = {
    "MDB": "MAJORCADAILYBULLETIN.COM"
}

def upsert_pair_sheet(sheet_name: str, mediaA: str, mediaB: str, out_df: pd.DataFrame, target: datetime):
    """
    Upsert en hoja de comparación:
    Día | Nº | Fecha | Usuarios A | Usuarios B | Visitas A | Visitas B | Páginas A | Páginas B | PV/U A | PV/U B
    """
    try:
        ws_pair = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws_pair = sh.add_worksheet(title=sheet_name, rows=2000, cols=20)
        header = ["Día","Nº","Fecha",
                  f"Usuarios {mediaA.split('.')[0].split('-')[0].split()[0].split('/')[0]}",
                  f"Usuarios {mediaB.split('.')[0].split('-')[0].split()[0].split('/')[0]}",
                  f"Visitas {mediaA.split('.')[0]}", f"Visitas {mediaB.split('.')[0]}",
                  f"Páginas {mediaA.split('.')[0]}", f"Páginas {mediaB.split('.')[0]}",
                  "PV/U A","PV/U B"]
        ws_pair.update("A1", [header])

    # Extraer valores del día
    def pick(media):
        row = out_df[out_df["Nombre"]==media]
        if row.empty:
            return None,None,None
        r = row.iloc[0]
        u,v,p = r["Navegadores Únicos"], r["Visitas"], r["Páginas Vistas"]
        return int(u) if pd.notna(u) else None, int(v) if pd.notna(v) else None, int(p) if pd.notna(p) else None

    uA,vA,pA = pick(mediaA)
    uB,vB,pB = pick(mediaB)

    weekday = WEEKDAYS_ES[target.weekday()].capitalize()
    daynum  = target.day
    serial  = gs_date_serial(target.date())

    pvuA = round(pA/uA, 2) if uA and pA else None
    pvuB = round(pB/uB, 2) if uB and pB else None

    newrow = [weekday, daynum, serial, uA, uB, vA, vB, pA, pB, pvuA, pvuB]

    # Leer existente para dedupe por Fecha
    vals = ws_pair.get_all_values()
    header = vals[0] if vals else []
    data = vals[1:] if len(vals) > 1 else []
    df = pd.DataFrame(data, columns=header) if header else pd.DataFrame()

    if not df.empty and "Fecha" in df.columns:
        # Reemplazar si ya existe la fecha
        serial_str = str(serial)
        mask = df["Fecha"] == serial_str
        if mask.any():
            idx = mask.idxmax()
            rng = f"A{int(idx)+2}:K{int(idx)+2}"
            ws_pair.update(rng, [newrow])
        else:
            ws_pair.append_row(newrow, value_input_option="USER_ENTERED")
    else:
        ws_pair.append_row(newrow, value_input_option="USER_ENTERED")

    # Formatos
    fmt_date = CellFormat(numberFormat=numberFormat(type="DATE", pattern="yyyy-mm-dd"))
    fmt_num  = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0"))
    fmt_dec2 = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0.00"))
    try:
        format_cell_range(ws_pair, "C2:C10000", fmt_date)   # Fecha
        format_cell_range(ws_pair, "D2:I10000", fmt_num)    # enteros
        format_cell_range(ws_pair, "J2:K10000", fmt_dec2)   # PV/U
    except Exception as e:
        print(f"[FORMAT][WARN] {sheet_name}: {e}")

def upsert_single_sheet(sheet_name: str, media: str, out_df: pd.DataFrame, target: datetime):
    """
    Hoja de un solo medio (MDB):
    Día | Nº | Fecha | Usuarios | Páginas vistas
    """
    try:
        ws_single = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws_single = sh.add_worksheet(title=sheet_name, rows=2000, cols=10)
        ws_single.update("A1", [["Día","Nº","Fecha","Usuarios","Páginas vistas"]])

    row = out_df[out_df["Nombre"]==media]
    if row.empty:
        u,p = None, None
    else:
        r = row.iloc[0]
        u = int(r["Navegadores Únicos"]) if pd.notna(r["Navegadores Únicos"]) else None
        p = int(r["Páginas Vistas"]) if pd.notna(r["Páginas Vistas"]) else None

    weekday = WEEKDAYS_ES[target.weekday()].capitalize()
    daynum  = target.day
    serial  = gs_date_serial(target.date())

    newrow = [weekday, daynum, serial, u, p]

    vals = ws_single.get_all_values()
    header = vals[0] if vals else []
    data = vals[1:] if len(vals) > 1 else []
    df = pd.DataFrame(data, columns=header) if header else pd.DataFrame()

    if not df.empty and "Fecha" in df.columns:
        serial_str = str(serial)
        mask = df["Fecha"] == serial_str
        if mask.any():
            idx = mask.idxmax()
            ws_single.update(f"A{int(idx)+2}:E{int(idx)+2}", [newrow])
        else:
            ws_single.append_row(newrow, value_input_option="USER_ENTERED")
    else:
        ws_single.append_row(newrow, value_input_option="USER_ENTERED")

    fmt_date = CellFormat(numberFormat=numberFormat(type="DATE", pattern="yyyy-mm-dd"))
    fmt_num  = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0"))
    try:
        format_cell_range(ws_single, "C2:C10000", fmt_date)
        format_cell_range(ws_single, "D2:E10000", fmt_num)
    except Exception as e:
        print(f"[FORMAT][WARN] {sheet_name}: {e}")

# ================== PLAYWRIGHT (login + scraping) ==================
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
        open_tm_and_search(page, target)

        # 3) Tabla
        df = extract_table(page)
        dbg(page, "06_table_captured")
        print(f"[INFO] Filas en tabla: {len(df)}")

        # 4) Columna de medio
        candidates = [c for c in df.columns if norm(c) in {"nombre","medio","site","sitio","dominio","brand","marca","titulo","name"}]
        media_col = candidates[0] if candidates else df.columns[0]
        print(f"[INFO] Columna medio: {media_col}")

        # 5) Formato final por medio (nombres y números normalizados)
        out = shape_output(df, media_col, forced_date=target)
        print(f"[INFO] Filas tras filtro de dominios: {len(out)}")

        # 6) Escribir histórico por medio en 'OJD'
        write_append_and_dedupe_types(out)

        # 7) Escribir/actualizar hojas de parejas (histórico día a día)
        for sheet, (A, B) in PAIR_SHEETS.items():
            upsert_pair_sheet(sheet, A, B, out, target)

        # 8) Hoja de un medio (MDB)
        for sheet, M in SINGLE_SHEET.items():
            upsert_single_sheet(sheet, M, out, target)

        dbg(page, "07_done")
        browser.close()

if __name__ == "__main__":
    run()
