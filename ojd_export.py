import os, re, pathlib
from json import loads as json_loads
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from unidecode import unidecode
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ========================== CONFIGURACIÓN ==========================
SHEET_ID  = "1ra1VSpOZ6JuMp-S_MsqNbHEGr2n0VA702lbFsVBD-Os"     # BASE
SHEET_TAB = os.getenv("SHEET_TAB", "OJD")                      # pestaña base

LOGIN_URL = "https://www.ojdinteractiva.es/traffic-monitoring/login"
TM_URL    = "https://www.ojdinteractiva.es/traffic-monitoring/traffic-monitoring/0/"

OJD_USER = os.environ["OJD_USER"]
OJD_PASS = os.environ["OJD_PASS"]

# ========================== GOOGLE SHEETS ==========================
import gspread
from google.oauth2.service_account import Credentials
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

# ========================== UTILIDADES ==========================
def tz_now() -> datetime:
    return datetime.now(ZoneInfo("Europe/Madrid"))

def day_target() -> datetime:
    # Siempre hoy-2 (como pediste)
    now = tz_now()
    return now - timedelta(days=2)

def guard_1215() -> bool:
    # Sin bloqueo horario
    return True

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(str(s).lower()))

# Nombres oficiales en salida (según tu entregable)
TARGET_NAMES = {
    "ultimahora":       "ULTIMAHORA.ES",
    "diariodemallorca": "DIARIODEMALLORCA.ES",
    "diariodeibiza":    "DIARIODEIBIZA.ES",
    "mallorcamagazin":  "MALLORCAMAGAZIN.COM",
    "mallorcazeitung":  "MALLORCAZEITUNG.COM",   # .COM
    "majorcadaily":     "MAJORCADAILYBULLETIN.COM",
    "majorcadailybulletin": "MAJORCADAILYBULLETIN.COM",
}

ALIASES = {
    "ultimahora": {"ultimahora.es","ultimahora","ultima hora","última hora"},
    "diariodemallorca": {"diariodemallorca.es","diariodemallorca","diario de mallorca"},
    "diariodeibiza": {"diariodeibiza.es","diariodeibiza","diario de ibiza"},
    "mallorcamagazin": {"mallorcamagazin.es","mallorca magazin","mallorcamagazin.com"},
    "mallorcazeitung": {"mallorcazeitung.es","mallorca zeitung","mallorcazeitung.com"},
    "majorcadaily": {"majorcadailybulletin.es","majorcadaily","majorca daily bulletin",
                     "majorca daily","majorcadailybulletin.com"},
}
ORDER_KEYS = ["ultimahora","diariodemallorca","diariodeibiza","mallorcamagazin","mallorcazeitung","majorcadaily"]

WEEKDAYS_ES = ["lunes","martes","miércoles","jueves","viernes","sábado","domingo"]

DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
def dbg(page, step):
    try:
        page.screenshot(path=str(DEBUG_DIR / f"{step}.png"), full_page=True)
        (DEBUG_DIR / f"{step}.html").write_text(page.content(), encoding="utf-8")
    except Exception as e:
        print(f"[DEBUG] {step}: {e}")

# Google Sheets almacena fechas como "serial" (días desde 1899-12-30)
def gs_date_serial(d: date) -> int:
    return (d - date(1899, 12, 30)).days

def pick_table(tables):
    # Elige la tabla que tenga columnas relevantes
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
    navu_col    = find_col({"navegadoresunicos","usuariosunicos","usuarios","users"})
    visitas_col = find_col({"visitas","sesiones","sessions","visits"})
    pv_col      = find_col({"paginasvistas","pageviews","paginas","pv"})

    out = pd.DataFrame()
    out["Fecha"] = forced_date.strftime("%Y-%m-%d")
    out["Nombre"] = df[media_col].map(canonical_name)
    out["Navegadores Únicos"] = df[navu_col].map(to_int)    if navu_col    else None
    out["Visitas"]            = df[visitas_col].map(to_int) if visitas_col else None
    out["Páginas Vistas"]     = df[pv_col].map(to_int)      if pv_col      else None

    out = out.dropna(subset=["Nombre"]).reset_index(drop=True)
    order_index = {TARGET_NAMES[k]: i for i,k in enumerate(ORDER_KEYS)}
    out["__ord"] = out["Nombre"].map(lambda x: order_index.get(x, 999))
    out = out.sort_values("__ord").drop(columns="__ord")
    return out

# ========================== ESCRITURA OJD (histórico) ==========================
def _to_serial_rows(rows):
    out = []
    for r in rows:
        rr = list(r)
        try:
            dt = datetime.strptime(str(rr[0]), "%Y-%m-%d").date()
            rr[0] = gs_date_serial(dt)
        except Exception:
            pass
        for i in (2,3,4):
            try:
                rr[i] = int(rr[i]) if rr[i] not in ("", None, "nan") else ""
            except Exception:
                pass
        out.append(rr)
    return out

def _write_ws_with_formats(target_ws, values):
    target_ws.clear()
    # MUY IMPORTANTE: rango simple "A1" (evita 'OJD!OJD!A1')
    target_ws.update("A1", values)
    fmt_date = CellFormat(numberFormat=numberFormat(type="DATE", pattern="yyyy-mm-dd"))
    fmt_num  = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0"))
    try:
        format_cell_range(target_ws, "A2:A10000", fmt_date)
        format_cell_range(target_ws, "C2:E10000", fmt_num)
    except Exception as e:
        print(f"[FORMAT][WARN] {e}")

def write_append_and_dedupe_types(df_new: pd.DataFrame):
    vals = ws.get_all_values()
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]

    if not vals:
        data = df_new.astype(object).where(pd.notna(df_new), "").values.tolist()
        _write_ws_with_formats(ws, [header] + _to_serial_rows(data))
        return

    df_old = pd.DataFrame(vals[1:], columns=vals[0])
    for c in ("Navegadores Únicos","Visitas","Páginas Vistas"):
        if c in df_old.columns:
            df_old[c] = pd.to_numeric(
                df_old[c].str.replace(".","",regex=False).str.replace(",","",regex=False),
                errors="coerce"
            ).astype("Int64")

    combo = pd.concat([df_old, df_new], ignore_index=True)
    combo = combo.drop_duplicates(subset=["Fecha","Nombre"], keep="last")

    order_index = {TARGET_NAMES[k]: i for i,k in enumerate(ORDER_KEYS)}
    combo["__ord"] = combo["Nombre"].map(lambda x: order_index.get(x, 999))
    combo = combo.sort_values(["Fecha","__ord"]).drop(columns="__ord")

    rows = combo.astype(object).where(pd.notna(combo), "").values.tolist()
    _write_ws_with_formats(ws, [header] + _to_serial_rows(rows))

# ========================== HOJAS DE PAREJAS / SINGLE ==========================
PAIR_SHEETS = {
    "UH-DM": ("ULTIMAHORA.ES","DIARIODEMALLORCA.ES"),
    "UH-DI": ("ULTIMAHORA.ES","DIARIODEIBIZA.ES"),
    "MM-MZ": ("MALLORCAMAGAZIN.COM","MALLORCAZEITUNG.COM"),
}
SINGLE_SHEET = {
    "MDB": "MAJORCADAILYBULLETIN.COM"
}

def upsert_pair_sheet(sheet_name: str, mediaA: str, mediaB: str, out_df: pd.DataFrame, target: datetime):
    try:
        ws_pair = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws_pair = sh.add_worksheet(title=sheet_name, rows=2000, cols=20)
        ws_pair.update("A1", [[
            "Día","Nº","Fecha",
            f"Usuarios {mediaA}", f"Usuarios {mediaB}",
            f"Visitas {mediaA}",  f"Visitas {mediaB}",
            f"Páginas {mediaA}", f"Páginas {mediaB}",
            "PV/U A","PV/U B"
        ]])

    def pick(media):
        row = out_df[out_df["Nombre"]==media]
        if row.empty: return None,None,None
        r = row.iloc[0]
        u = int(r["Navegadores Únicos"]) if pd.notna(r["Navegadores Únicos"]) else None
        v = int(r["Visitas"])            if pd.notna(r["Visitas"])            else None
        p = int(r["Páginas Vistas"])     if pd.notna(r["Páginas Vistas"])     else None
        return u,v,p

    uA,vA,pA = pick(mediaA)
    uB,vB,pB = pick(mediaB)
    weekday  = WEEKDAYS_ES[target.weekday()].capitalize()
    daynum   = target.day
    serial   = gs_date_serial(target.date())
    pvuA = round(pA/uA, 2) if uA and pA else None
    pvuB = round(pB/uB, 2) if uB and pB else None
    newrow = [weekday, daynum, serial, uA, uB, vA, vB, pA, pB, pvuA, pvuB]

    vals = ws_pair.get_all_values()
    if len(vals) > 1:
        header = vals[0]
        df = pd.DataFrame(vals[1:], columns=header)
        if "Fecha" in df.columns and (df["Fecha"] == str(serial)).any():
            idx = (df["Fecha"] == str(serial)).idxmax()
            ws_pair.update(f"A{idx+2}:K{idx+2}", [newrow])
        else:
            ws_pair.append_row(newrow, value_input_option="USER_ENTERED")
    else:
        ws_pair.append_row(newrow, value_input_option="USER_ENTERED")

    fmt_date = CellFormat(numberFormat=numberFormat(type="DATE", pattern="yyyy-mm-dd"))
    fmt_num  = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0"))
    fmt_dec2 = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0.00"))
    try:
        format_cell_range(ws_pair, "C2:C10000", fmt_date)
        format_cell_range(ws_pair, "D2:I10000", fmt_num)
        format_cell_range(ws_pair, "J2:K10000", fmt_dec2)
    except Exception as e:
        print(f"[FORMAT][WARN] {sheet_name}: {e}")

def upsert_single_sheet(sheet_name: str, media: str, out_df: pd.DataFrame, target: datetime):
    try:
        ws_single = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws_single = sh.add_worksheet(title=sheet_name, rows=2000, cols=10)
        ws_single.update("A1", [["Día","Nº","Fecha","Usuarios","Páginas vistas"]])

    row = out_df[out_df["Nombre"]==media]
    if row.empty:
        u, p = None, None
    else:
        r = row.iloc[0]
        u = int(r["Navegadores Únicos"]) if pd.notna(r["Navegadores Únicos"]) else None
        p = int(r["Páginas Vistas"])     if pd.notna(r["Páginas Vistas"])     else None

    weekday = WEEKDAYS_ES[target.weekday()].capitalize()
    daynum  = target.day
    serial  = gs_date_serial(target.date())
    newrow  = [weekday, daynum, serial, u, p]

    vals = ws_single.get_all_values()
    if len(vals) > 1:
        header = vals[0]
        df = pd.DataFrame(vals[1:], columns=header)
        if "Fecha" in df.columns and (df["Fecha"] == str(serial)).any():
            idx = (df["Fecha"] == str(serial)).idxmax()
            ws_single.update(f"A{idx+2}:E{idx+2}", [newrow])
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

# ========================== FALLBACK: NO HAY DATOS ==========================
def write_no_data(target_dt: datetime):
    # Marca en OJD y añade filas "vacías" en comparativas
    row = [[target_dt.strftime("%Y-%m-%d"), "NO HAY DATOS", "", "", ""]]
    vals = ws.get_all_values()
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
    if not vals:
        ws.update("A1", [header] + row)
    else:
        df_old = pd.DataFrame(vals[1:], columns=vals[0])
        if not ((df_old["Fecha"] == row[0][0]) & (df_old["Nombre"] == "NO HAY DATOS")).any():
            ws.append_row(row[0], value_input_option="USER_ENTERED")

    weekday = WEEKDAYS_ES[target_dt.weekday()].capitalize()
    daynum  = target_dt.day
    serial  = gs_date_serial(target_dt.date())
    for sheet in PAIR_SHEETS.keys():
        try:
            ws_pair = sh.worksheet(sheet)
            ws_pair.append_row([weekday, daynum, serial, None, None, None, None, None, None, None, None],
                               value_input_option="USER_ENTERED")
        except gspread.exceptions.WorksheetNotFound:
            pass
    for sheet in SINGLE_SHEET.keys():
        try:
            ws_single = sh.worksheet(sheet)
            ws_single.append_row([weekday, daynum, serial, None, None], value_input_option="USER_ENTERED")
        except gspread.exceptions.WorksheetNotFound:
            pass

# ========================== PLAYWRIGHT (login + buscar) ==========================
def login_tm(page):
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    dbg(page, "01_login")

    # cookies (si aparece)
    for txt in ["ACEPTAR TODO","Aceptar todo","Aceptar cookies","RECHAZAR","Rechazar todo"]:
        try:
            btn = page.get_by_role("button", name=re.compile(txt, re.I))
            if btn.count(): btn.first.click(); break
        except: pass

    page.get_by_placeholder("Usuario").fill(OJD_USER)
    page.get_by_placeholder("Contraseña").fill(OJD_PASS)
    page.get_by_role("button", name=re.compile(r"Acceder", re.I)).click()
    page.wait_for_load_state("networkidle")
    dbg(page, "02_after_login")

    if page.locator("h1:has-text('Inicio de sesión')").count():
        raise RuntimeError("Login fallido (pantalla de login sigue visible).")

def open_tm_and_search(page, target_date: datetime):
    page.goto(TM_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    dbg(page, "03_tm")

    ymd      = target_date.strftime("%Y-%m-%d")
    ddmmyyyy = target_date.strftime("%d/%m/%Y")

    # Intenta campos típicos
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

    # Fallback: inyecta valor
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
    dbg(page, "04_date_set")

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

def read_effective_date_from_page(page, fallback: datetime) -> datetime:
    # 1) valor del input[type=date]
    try:
        v = page.eval_on_selector('input[type="date"]', "e => e && e.value")
        if v:  # 'YYYY-MM-DD'
            return datetime.strptime(v, "%Y-%m-%d")
    except:
        pass
    # 2) cualquier 'dd/mm/yyyy' en el texto
    try:
        txt = page.text_content("body") or ""
        m = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", txt)
        if m:
            d, mth, y = map(int, m.groups())
            return datetime(y, mth, d)
    except:
        pass
    return fallback

def extract_table(page) -> pd.DataFrame:
    try:
        page.wait_for_selector("table", timeout=30000)
    except PWTimeout:
        dbg(page, "05b_no_table")
        raise RuntimeError("No apareció la tabla tras Buscar.")
    html = page.content()
    tables = pd.read_html(html)
    if not tables: raise RuntimeError("No se encontraron tablas HTML.")
    df = pick_table(tables)
    if df is None or df.empty:
        raise RuntimeError("Tabla identificada vacía.")
    return df

# ========================== MAIN ==========================
def run():
    print("[START] ojd_export.py")
    if not guard_1215(): return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Login
        login_tm(page)

        # Fecha solicitada
        target = day_target()
        print(f"[INFO] Fecha solicitada (hoy-2): {target.strftime('%Y-%m-%d')}")
        open_tm_and_search(page, target)

        # Fecha efectiva que muestra la web
        effective = read_effective_date_from_page(page, target)
        if effective.date() != target.date():
            print(f"[WARN] La web muestra {effective.strftime('%Y-%m-%d')} (no {target.strftime('%Y-%m-%d')}). Usaremos la fecha efectiva.")
            target = effective

        # Tabla
        try:
            df = extract_table(page)
        except Exception as e:
            print(f"[INFO] No hay tabla/datos: {e}")
            write_no_data(target)
            dbg(page, "07_done_no_data")
            browser.close()
            return

        dbg(page, "06_table")
        print(f"[INFO] Filas en tabla: {len(df)}")

        # Columna del medio
        candidates = [c for c in df.columns if norm(c) in {
            "nombre","medio","site","sitio","dominio","brand","marca","titulo","name"
        }]
        media_col = candidates[0] if candidates else df.columns[0]
        print(f"[INFO] Columna medio: {media_col}")

        # Output normalizado
        out = shape_output(df, media_col, forced_date=target)
        print(f"[INFO] Filas tras filtro dominios: {len(out)}")

        if out.empty:
            print("[INFO] No hay datos para esa fecha. Escribimos marcador.")
            write_no_data(target)
            dbg(page, "07_done_no_data")
            browser.close()
            return

        # 1) OJD (histórico por medio)
        write_append_and_dedupe_types(out)

        # 2) Hojas comparativas (una fila / día)
        for sheet, (A, B) in PAIR_SHEETS.items():
            upsert_pair_sheet(sheet, A, B, out, target)

        # 3) Hoja MDB (un solo medio)
        for sheet, M in SINGLE_SHEET.items():
            upsert_single_sheet(sheet, M, out, target)

        dbg(page, "07_done")
        browser.close()

if __name__ == "__main__":
    run()

