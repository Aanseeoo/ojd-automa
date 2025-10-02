import os, re, time, pathlib, hashlib
from json import loads as json_loads
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from unidecode import unidecode
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ========================== CONFIG ==========================
SHEET_ID  = "1ra1VSpOZ6JuMp-S_MsqNbHEGr2n0VA702lbFsVBD-Os"  # Hoja base
SHEET_TAB = os.getenv("SHEET_TAB", "OJD")

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

# ========================== UTILS ==========================
DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)

def tz_now() -> datetime:
    return datetime.now(ZoneInfo("Europe/Madrid"))

def target_dt() -> datetime:
    """La fecha que necesitamos: hoy-2 en Europa/Madrid."""
    return tz_now() - timedelta(days=2)

def dmy(d: datetime) -> str:
    return d.strftime("%d/%m/%Y")

def ymd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def gs_date_serial(d: date) -> int:
    # Serial de Google Sheets (sistema 1899-12-30)
    return (d - date(1899, 12, 30)).days

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(str(s).lower()))

TARGET_NAMES = {
    "ultimahora":       "ULTIMAHORA.ES",
    "diariodemallorca": "DIARIODEMALLORCA.ES",
    "diariodeibiza":    "DIARIODEIBIZA.ES",
    "mallorcamagazin":  "MALLORCAMAGAZIN.COM",
    "mallorcazeitung":  "MALLORCAZEITUNG.COM",
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
ORDER_KEYS = ["ultimahora","diariodemallorca","diariodeibiza",
              "mallorcamagazin","mallorcazeitung","majorcadaily"]

def canonical_name(val: str) -> str | None:
    n = norm(val)
    for key, aliases in ALIASES.items():
        if any(norm(a) in n for a in aliases):
            return TARGET_NAMES[key]
    return None

def to_int(x):
    s = str(x).strip()
    if s.lower() in {"", "nan", "none", "null"}: return None
    s = s.replace(".", "").replace(",", "")
    return int(s) if s.isdigit() else None

def pick_table(tables):
    # buscamos la tabla con señales típicas
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

def shape_output(df: pd.DataFrame, media_col: str, forced_date: datetime) -> pd.DataFrame:
    def find_col(cands):
        for c in df.columns:
            nc = norm(c)
            if any(nc == x or x in nc for x in cands): return c
        return None
    navu_col    = find_col({"navegadoresunicos","usuariosunicos","usuarios","users"})
    visitas_col = find_col({"visitas","sesiones","sessions","visits"})
    pv_col      = find_col({"paginasvistas","pageviews","paginas","pv"})

    out = pd.DataFrame()
    out["Fecha"] = ymd(forced_date)
    out["Nombre"] = df[media_col].map(canonical_name)
    out["Navegadores Únicos"] = df[navu_col].map(to_int)    if navu_col    else None
    out["Visitas"]            = df[visitas_col].map(to_int) if visitas_col else None
    out["Páginas Vistas"]     = df[pv_col].map(to_int)      if pv_col      else None

    out = out.dropna(subset=["Nombre"]).reset_index(drop=True)
    order_index = {TARGET_NAMES[k]: i for i,k in enumerate(ORDER_KEYS)}
    out["__ord"] = out["Nombre"].map(lambda x: order_index.get(x, 999))
    out = out.sort_values("__ord").drop(columns="__ord")
    return out

def _to_serial_rows(rows):
    out = []
    for r in rows:
        rr = list(r)
        try:
            dt = datetime.strptime(str(rr[0]), "%Y-%m-%d").date()
            rr[0] = gs_date_serial(dt)
        except: pass
        for i in (2,3,4):
            try: rr[i] = int(rr[i]) if rr[i] not in ("", None, "nan") else ""
            except: pass
        out.append(rr)
    return out

def _write_ws_with_formats(target_ws, values):
    target_ws.clear()
    target_ws.update("A1", values)
    fmt_date = CellFormat(numberFormat=numberFormat(type="DATE", pattern="yyyy-mm-dd"))
    fmt_num  = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0"))
    try:
        format_cell_range(target_ws, "A2:A10000", fmt_date)
        format_cell_range(target_ws, "C2:E10000", fmt_num)
    except Exception as e:
        print(f"[FORMAT][WARN] {e}")

def write_append_and_dedupe_types(df_new: pd.DataFrame):
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
    vals = ws.get_all_values()
    if not vals:
        data = df_new.astype(object).where(pd.notna(df_new), "").values.tolist()
        _write_ws_with_formats(ws, [header] + _to_serial_rows(data)); return

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

def write_no_data(target: datetime):
    """Inserta 'NO HAY DATOS' para esa fecha (una sola fila) si no existía."""
    target_ymd = ymd(target)
    vals = ws.get_all_values()
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
    row = [target_ymd, "NO HAY DATOS", "", "", ""]
    if not vals:
        _write_ws_with_formats(ws, [header, row]); return
    df_old = pd.DataFrame(vals[1:], columns=vals[0]) if len(vals) > 1 else pd.DataFrame(columns=header)
    mask = (df_old.get("Fecha","") == target_ymd) & (df_old.get("Nombre","") == "NO HAY DATOS")
    if not mask.any():
        ws.append_row(row, value_input_option="USER_ENTERED")

# ========================== PLAYWRIGHT ==========================
def login_tm(page):
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    # cookies (si aparecen)
    for txt in ["ACEPTAR TODO","Aceptar todo","Aceptar cookies","RECHAZAR","Rechazar todo"]:
        try:
            b = page.get_by_role("button", name=re.compile(txt, re.I))
            if b.count(): b.first.click(); break
        except: pass
    page.get_by_placeholder("Usuario").fill(OJD_USER)
    page.get_by_placeholder("Contraseña").fill(OJD_PASS)
    page.get_by_role("button", name=re.compile(r"Acceder", re.I)).click()
    page.wait_for_load_state("networkidle")

def set_date_and_search(page, dt: datetime) -> bool:
    """
    Escribe la fecha en #datepicker (dd/mm/yyyy), verifica que quedó escrita
    y pulsa el botón Buscar (icono lupa). Devuelve True si parece haber recargado.
    """
    page.goto(TM_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    wanted = dmy(dt)
    inp = page.locator("#datepicker")
    # Espera a que exista el input
    inp.wait_for(state="visible", timeout=8000)

    # Borrar y escribir (doble estrategia: fill + select_all+type)
    try:
        inp.fill("")              # limpia
        inp.fill(wanted)          # escribe
        # por si queda basura, reescribe seleccionando todo:
        inp.press("Control+a")
        inp.type(wanted, delay=20)
    except PWTimeout:
        return False

    # Confirmar que quedó el valor correcto
    ok = False
    for _ in range(4):
        try:
            val = inp.input_value(timeout=2000)
            if val.strip() == wanted:
                ok = True
                break
            time.sleep(0.4)
        except:
            time.sleep(0.4)
    if not ok:
        print(f"[WARN] No se pudo fijar la fecha correcta. Quedó: '{inp.input_value()}'")
        # aún así seguimos, por si la web respeta el valor enviado.

    # Pulsar Buscar (botón con icono lupa)
    # selector robusto: botón submit o el que contenga .fa-search
    clicked = False
    for sel in [
        "button[type='submit']",
        "button:has(.fa-search)",
        "form button.btn"
    ]:
        loc = page.locator(sel)
        if loc.count():
            loc.first.click()
            clicked = True
            break
    if not clicked:
        # último recurso: Enter en el input
        inp.press("Enter")

    # Espera a que la tabla/all content se refresque
    page.wait_for_load_state("networkidle")
    time.sleep(1.0)  # margen

    return True

def table_fingerprint(page) -> str:
    try:
        tbl = page.locator("table").first
        if not tbl.count(): return ""
        html = tbl.inner_html()
        return hashlib.md5(html.encode("utf-8", errors="ignore")).hexdigest()
    except:
        return ""

def read_table(page) -> pd.DataFrame:
    html = page.content()
    try:
        tables = pd.read_html(html)
    except ValueError:
        return pd.DataFrame()
    if not tables: return pd.DataFrame()
    t = pick_table(tables)
    return t if t is not None else pd.DataFrame()

# ========================== MAIN ==========================
def run():
    print("[START] ojd_export.py")
    tgt = target_dt()
    print(f"[INFO] Fecha objetivo (hoy-2): {ymd(tgt)}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # 1) Login
        login_tm(page)

        # 2) Fijar fecha exacta en #datepicker y Buscar
        ok = set_date_and_search(page, tgt)
        if not ok:
            print("[ERR] No fue posible preparar el filtro de fecha.")
            write_no_data(tgt)
            browser.close()
            return

        # 3) Verificar que la página está en la fecha deseada (valor del input)
        try:
            val_now = page.locator("#datepicker").input_value(timeout=4000)
            print(f"[INFO] Datepicker ahora es: '{val_now}'")
        except:
            val_now = ""
        if val_now.strip() != dmy(tgt):
            print("[WARN] El datepicker no refleja hoy-2. Intento 2…")
            # Un segundo intento rápido
            set_date_and_search(page, tgt)
            try:
                val_now = page.locator("#datepicker").input_value(timeout=4000)
            except:
                val_now = ""
            print(f"[INFO] Datepicker tras reintento: '{val_now}'")

        # 4) Leer tabla
        fp_before = table_fingerprint(page)
        df = read_table(page)
        if df.empty:
            print("[INFO] No se pudo leer una tabla válida ⇒ NO HAY DATOS.")
            write_no_data(tgt)
            browser.close()
            return

        # 5) Determinar columna de medio
        candidates = [c for c in df.columns if norm(c) in {
            "nombre","medio","site","sitio","dominio","brand","marca","titulo","name"
        }]
        media_col = candidates[0] if candidates else df.columns[0]

        out = shape_output(df, media_col, tgt)
        if out.empty:
            print("[INFO] Tras filtro de medios, no hay filas ⇒ NO HAY DATOS.")
            write_no_data(tgt)
            browser.close()
            return

        # 6) Escribir en la base con formatos y dedupe
        write_append_and_dedupe_types(out)

        browser.close()
        print("[DONE] OK")

if __name__ == "__main__":
    run()
