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

# Opcional: forzar fecha (dd/mm/yyyy), útil para pruebas o re-procesos
FORCE_DATE_STR = os.getenv("FORCE_DATE_DDMMYYYY", "").strip()

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

def default_target_dt() -> datetime:
    """Fecha por defecto: hoy-2 en Europa/Madrid."""
    return tz_now() - timedelta(days=2)

def dmy(d: datetime | date) -> str:
    return (d if isinstance(d, datetime) else datetime.combine(d, datetime.min.time())).strftime("%d/%m/%Y")

def ymd(d: datetime | date) -> str:
    return (d if isinstance(d, datetime) else datetime.combine(d, datetime.min.time())).strftime("%Y-%m-%d")

def parse_dmy(s: str) -> datetime | None:
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y").replace(tzinfo=ZoneInfo("Europe/Madrid"))
    except Exception:
        return None

def gs_date_serial(d: date) -> int:
    # Serial de Google Sheets (sistema 1899-12-30)
    return (d - date(1899, 12, 30)).days

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(str(s).lower()))

TARGET_NAMES = {
    "ultimahora":            "ULTIMAHORA.ES",
    "diariodemallorca":      "DIARIODEMALLORCA.ES",
    "diariodeibiza":         "DIARIODEIBIZA.ES",
    "mallorcamagazin":       "MALLORCAMAGAZIN.COM",
    "mallorcazeitung":       "MALLORCAZEITUNG.COM",
    "majorcadaily":          "MAJORCADAILYBULLETIN.COM",
    "majorcadailybulletin":  "MAJORCADAILYBULLETIN.COM",
    "periodicodeibiza":      "PERIODICODEIBIZA.ES",
    "lavozdeibiza":          "LAVOZDEIBIZA.COM",
}

ALIASES = {
    "ultimahora": {
        "ultimahora.es","ultimahora","ultima hora","última hora"
    },
    "diariodemallorca": {
        "diariodemallorca.es","diariodemallorca","diario de mallorca"
    },
    "diariodeibiza": {
        "diariodeibiza.es","diariodeibiza","diario de ibiza"
    },
    "mallorcamagazin": {
        "mallorcamagazin.es","mallorca magazin","mallorcamagazin.com"
    },
    "mallorcazeitung": {
        "mallorcazeitung.es","mallorca zeitung","mallorcazeitung.com"
    },
    "majorcadaily": {
        "majorcadailybulletin.es","majorcadaily","majorca daily bulletin",
        "majorca daily","majorcadailybulletin.com"
    },
    "majorcadailybulletin": {
        "majorcadailybulletin.es","majorcadailybulletin.com","majorca daily bulletin"
    },
    "periodicodeibiza": {
        "periodicodeibiza.es","periodico de ibiza","periódico de ibiza","periodicodeibiza"
    },
    "lavozdeibiza": {
        "lavozdeibiza.com","la voz de ibiza","voz de ibiza","lavozdeibiza"
    },
}

# Orden deseado (periodicodeibiza penúltimo, lavozdeibiza último)
ORDER_KEYS = [
    "ultimahora",
    "diariodemallorca",
    "diariodeibiza",
    "mallorcamagazin",
    "mallorcazeitung",
    "majorcadaily",
    "periodicodeibiza",  # penúltimo
    "lavozdeibiza"       # último
]

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

def shape_output(df: pd.DataFrame, media_col: str, data_date: datetime) -> pd.DataFrame:
    def find_col(cands):
        for c in df.columns:
            nc = norm(c)
            if any(nc == x or x in nc for x in cands): return c
        return None
    navu_col    = find_col({"navegadoresunicos","usuariosunicos","usuarios","users"})
    visitas_col = find_col({"visitas","sesiones","sessions","visits"})
    pv_col      = find_col({"paginasvistas","pageviews","paginas","pv"})

    out = pd.DataFrame()
    out["Fecha"] = ymd(data_date)
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

def _write_ws_with_formats_overwrite(target_ws, values):
    """Sobrescribe toda la hoja con cabecera+datos y aplica formatos."""
    target_ws.clear()
    target_ws.update("A1", values)
    fmt_date = CellFormat(numberFormat=numberFormat(type="DATE", pattern="yyyy-mm-dd"))
    fmt_num  = CellFormat(numberFormat=numberFormat(type="NUMBER", pattern="#,##0"))
    try:
        format_cell_range(target_ws, "A2:A10000", fmt_date)
        format_cell_range(target_ws, "C2:E10000", fmt_num)
    except Exception as e:
        print(f"[FORMAT][WARN] {e}")

def write_replace_all(df_new: pd.DataFrame):
    """Siempre sobreescribe con los datos de esta ejecución."""
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
    rows = df_new.astype(object).where(pd.notna(df_new), "").values.tolist()
    _write_ws_with_formats_overwrite(ws, [header] + _to_serial_rows(rows))

def write_no_data_overwrite(data_date: datetime):
    """Cuando no hay datos, sobreescribe con una sola fila 'NO HAY DATOS'."""
    header = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]
    row = [ymd(data_date), "NO HAY DATOS", "", "", ""]
    _write_ws_with_formats_overwrite(ws, [header, row])

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
    inp.wait_for(state="visible", timeout=8000)

    # Borrar y escribir (fill + select_all + type)
    try:
        inp.fill("")
        inp.fill(wanted)
        inp.press("Control+a")
        inp.type(wanted, delay=20)
    except PWTimeout:
        return False

    # Confirmar valor
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

    # Pulsar Buscar
    clicked = False
    for sel in ["button[type='submit']", "button:has(.fa-search)", "form button.btn"]:
        loc = page.locator(sel)
        if loc.count():
            loc.first.click()
            clicked = True
            break
    if not clicked:
        inp.press("Enter")

    page.wait_for_load_state("networkidle")
    time.sleep(1.0)
    return True

def read_final_date_from_page(page) -> datetime | None:
    """Lee el valor actual del #datepicker y lo convierte a datetime."""
    try:
        val = page.locator("#datepicker").input_value(timeout=4000).strip()
        return parse_dmy(val)
    except Exception:
        return None

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

    # Fecha objetivo base
    tgt = default_target_dt()
    # Si hay FORCE_DATE, usamos esa
    if FORCE_DATE_STR:
        forced = parse_dmy(FORCE_DATE_STR)
        if forced: tgt = forced
        print(f"[INFO] FORCE_DATE_DDMMYYYY='{FORCE_DATE_STR}' -> objetivo {ymd(tgt)}")
    else:
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
            write_no_data_overwrite(tgt)
            browser.close()
            return

        # 3) Leemos la **fecha real** que usa la página
        real_dt = read_final_date_from_page(page) or tgt
        print(f"[INFO] Fecha confirmada en página: {ymd(real_dt)}")

        # 4) Leer tabla
        df = read_table(page)
        if df.empty:
            print("[INFO] No se pudo leer una tabla válida ⇒ NO HAY DATOS.")
            write_no_data_overwrite(real_dt)
            browser.close()
            return

        # 5) Determinar columna de medio
        candidates = [c for c in df.columns if norm(c) in {
            "nombre","medio","site","sitio","dominio","brand","marca","titulo","name"
        }]
        media_col = candidates[0] if candidates else df.columns[0]

        out = shape_output(df, media_col, real_dt)
        if out.empty:
            print("[INFO] Tras filtro de medios, no hay filas ⇒ NO HAY DATOS.")
            write_no_data_overwrite(real_dt)
            browser.close()
            return

        # 6) **SOBREESCRIBIR** en la base con los datos actuales
        write_replace_all(out)

        browser.close()
        print("[DONE] OK")

if __name__ == "__main__":
    run()
