import os, re, pathlib, time, hashlib
from json import loads as json_loads
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from unidecode import unidecode
from playwright.sync_api import sync_playwright

# ========================== CONFIG ==========================
SHEET_ID  = "1ra1VSpOZ6JuMp-S_MsqNbHEGr2n0VA702lbFsVBD-Os"  # Base
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

def day_target() -> datetime:
    return tz_now() - timedelta(days=2)  # hoy-2

def dmy(d: datetime) -> str:
    return d.strftime("%d/%m/%Y")
def ymd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def gs_date_serial(d: date) -> int:
    return (d - date(1899, 12, 30)).days  # <-- corregido

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
    vals = ws.get_all_values()
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
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

# ========================== NO HAY DATOS ==========================
def write_no_data(target_dt: datetime):
    row = [[ymd(target_dt), "NO HAY DATOS", "", "", ""]]
    vals = ws.get_all_values()
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
    if not vals:
        ws.update("A1", [header] + row)
    else:
        df_old = pd.DataFrame(vals[1:], columns=vals[0])
        if not ((df_old["Fecha"] == row[0][0]) & (df_old["Nombre"] == "NO HAY DATOS")).any():
            ws.append_row(row[0], value_input_option="USER_ENTERED")

# ========================== PLAYWRIGHT HELPERS ==========================
def login_tm(page):
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    # cookies
    for txt in ["ACEPTAR TODO","Aceptar todo","Aceptar cookies","RECHAZAR","Rechazar todo"]:
        try:
            b = page.get_by_role("button", name=re.compile(txt, re.I))
            if b.count(): b.first.click(); break
        except: pass
    page.get_by_placeholder("Usuario").fill(OJD_USER)
    page.get_by_placeholder("Contraseña").fill(OJD_PASS)
    page.get_by_role("button", name=re.compile(r"Acceder", re.I)).click()
    page.wait_for_load_state("networkidle")

def set_date_and_search(page, dt: datetime):
    page.goto(TM_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    # localizar input del día (intenta por label y, si no, primer input con date o texto)
    dia_input = None
    try:
        lab = page.locator("label:has-text('Día')").first
        if lab.count():
            cand = lab.locator("xpath=following::input[1]").first
            if cand.count(): dia_input = cand
    except: pass
    if not dia_input:
        x = page.locator("input[type='date']").first
        if x.count(): dia_input = x
    if not dia_input:
        dia_input = page.locator("input").first

    # estrategia doble: escribir dd/mm/yyyy y también value ISO (si es <input type=date>)
    dia_input.fill(dmy(dt))
    try:
        page.evaluate("""(iso) => {
            const dt = document.querySelector('input[type="date"]');
            if (dt) {
              dt.value = iso;
              dt.dispatchEvent(new Event('input',{bubbles:true}));
              dt.dispatchEvent(new Event('change',{bubbles:true}));
            }
        }""", ymd(dt))
    except: pass

    # click Buscar o Enter
    clicked = False
    for sel in ["button:has-text('Buscar')",
                "input[type='submit'][value*='Buscar']",
                "button[title*='Buscar']",
                'text=/^\\s*Buscar\\s*$/i']:
        if page.locator(sel).first.count():
            page.locator(sel).first.click(); clicked = True; break
    if not clicked:
        dia_input.press("Enter")

def table_fingerprint(page) -> str:
    try:
        if not page.locator("table").count(): return ""
        html = page.locator("table").first.inner_html()
        return hashlib.md5(html.encode("utf-8", errors="ignore")).hexdigest()
    except:
        return ""

def read_table(page) -> pd.DataFrame:
    html = page.content()
    tables = pd.read_html(html)
    if not tables: return pd.DataFrame()
    t = pick_table(tables)
    return t if t is not None else pd.DataFrame()

# ========================== MAIN ==========================
def run():
    print("[START] ojd_export.py")

    target = day_target()
    prev   = target - timedelta(days=1)  # hoy-3

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # 1) Login
        login_tm(page)

        # 2) Cargar prev (hoy-3) y sacar fingerprint + tabla
        set_date_and_search(page, prev)
        time.sleep(2.0)
        fp_prev = table_fingerprint(page)
        df_prev = read_table(page)

        # 3) Cargar target (hoy-2) y comparar fingerprint
        set_date_and_search(page, target)
        t0 = time.time()
        fp_tgt = ""
        while time.time() - t0 < 12:
            fp_tgt = table_fingerprint(page)
            if fp_tgt and fp_tgt != fp_prev:
                break
            time.sleep(0.6)

        if not fp_tgt or fp_tgt == fp_prev:
            print("[INFO] La tabla de hoy-2 coincide con la de hoy-3 ⇒ NO HAY DATOS.")
            write_no_data(target)
            browser.close()
            return

        df_tgt = read_table(page)
        if df_tgt.empty:
            print("[INFO] Tabla vacía para hoy-2 ⇒ NO HAY DATOS.")
            write_no_data(target)
            browser.close()
            return

        # 4) Construir salida
        candidates = [c for c in df_tgt.columns if norm(c) in {
            "nombre","medio","site","sitio","dominio","brand","marca","titulo","name"
        }]
        media_col = candidates[0] if candidates else df_tgt.columns[0]
        out = shape_output(df_tgt, media_col, target)

        if out.empty:
            print("[INFO] Tras filtro de medios, no hay filas ⇒ NO HAY DATOS.")
            write_no_data(target)
            browser.close()
            return

        # 5) Escribir a la base (con formatos)
        write_append_and_dedupe_types(out)

        browser.close()
        print("[DONE] OK")

if __name__ == "__main__":
    run()

