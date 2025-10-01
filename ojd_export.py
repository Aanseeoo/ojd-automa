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

HEADER = ["Fecha","Nombre","Navegadores Únicos","Visitas","Páginas Vistas"]

# ========================== UTILS ==========================
DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)

def tz_now() -> datetime:
    return datetime.now(ZoneInfo("Europe/Madrid"))

def day_target() -> datetime:
    return tz_now() - timedelta(days=2)  # hoy-2

def dmy(d: datetime) -> str:     # dd/mm/aaaa
    return d.strftime("%d/%m/%Y")

def dmy_dash(d: datetime) -> str:  # dd-mm-aaaa
    return d.strftime("%d-%m-%Y")

def ymd(d: datetime) -> str:     # aaaa-mm-dd
    return d.strftime("%Y-%m-%d")

def gs_date_serial(d: date) -> int:
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

# ---------- helpers de escritura ----------
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

def _read_or_init_df():
    vals = ws.get_all_values()
    if not vals:
        _write_ws_with_formats(ws, [HEADER])
        return pd.DataFrame(columns=HEADER)
    cols = vals[0]
    data = vals[1:]
    df = pd.DataFrame(data, columns=cols) if data else pd.DataFrame(columns=cols)
    for c in HEADER:
        if c not in df.columns:
            df[c] = ""
    df = df[HEADER]
    return df

def write_append_and_dedupe_types(df_new: pd.DataFrame):
    df_old = _read_or_init_df()
    for c in ("Navegadores Únicos","Visitas","Páginas Vistas"):
        if c in df_old.columns:
            df_old[c] = pd.to_numeric(
                df_old[c].astype(str).str.replace(".","",regex=False).str.replace(",","",regex=False),
                errors="coerce"
            ).astype("Int64")
    combo = pd.concat([df_old, df_new], ignore_index=True)
    combo = combo.drop_duplicates(subset=["Fecha","Nombre"], keep="last")

    order_index = {TARGET_NAMES[k]: i for i,k in enumerate(ORDER_KEYS)}
    combo["__ord"] = combo["Nombre"].map(lambda x: order_index.get(x, 999))
    combo = combo.sort_values(["Fecha","__ord"]).drop(columns="__ord")

    rows = combo.astype(object).where(pd.notna(combo), "").values.tolist()
    _write_ws_with_formats(ws, [HEADER] + _to_serial_rows(rows))

# ========================== NO HAY DATOS ==========================
def write_no_data(target_dt: datetime):
    df_old = _read_or_init_df()
    row = {"Fecha": ymd(target_dt), "Nombre": "NO HAY DATOS",
           "Navegadores Únicos": "", "Visitas": "", "Páginas Vistas": ""}
    mask = (df_old["Fecha"] == row["Fecha"]) & (df_old["Nombre"] == "NO HAY DATOS")
    if mask.any():
        print("[INFO] Aviso 'NO HAY DATOS' ya presente; no se duplica.")
        return
    df_new = pd.concat([df_old, pd.DataFrame([row])], ignore_index=True)
    rows = df_new.astype(object).where(pd.notna(df_new), "").values.tolist()
    _write_ws_with_formats(ws, [HEADER] + _to_serial_rows(rows))

# ========================== PLAYWRIGHT ==========================
def login_tm(page):
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    for txt in ["ACEPTAR TODO","Aceptar todo","Aceptar cookies","RECHAZAR","Rechazar todo"]:
        try:
            b = page.get_by_role("button", name=re.compile(txt, re.I))
            if b.count(): b.first.click(); break
        except: pass
    page.get_by_placeholder("Usuario").fill(OJD_USER)
    page.get_by_placeholder("Contraseña").fill(OJD_PASS)
    page.get_by_role("button", name=re.compile(r"Acceder", re.I)).click()
    page.wait_for_load_state("networkidle")

def _find_day_input(page):
    # intenta localizar el input alineado con la etiqueta "Día"
    try:
        lab = page.locator("label:has-text('Día')").first
        if lab.count():
            cand = lab.locator("xpath=following::input[1]").first
            if cand.count():
                return cand
    except: pass
    # prueba input type=date
    loc = page.locator("input[type='date']").first
    if loc.count(): return loc
    # último recurso: primer input visible
    return page.locator("input:visible").first

def _press_select_all_delete(inp):
    try:
        # Windows/Linux
        inp.press("Control+A"); inp.press("Delete")
    except:
        pass
    try:
        # macOS (por si el runner cambia)
        inp.press("Meta+A"); inp.press("Backspace")
    except:
        pass

def set_date_and_search(page, dt: datetime):
    """Ir a la lista, escribir fecha en el input y buscar.
       Escribe dd/mm/aaaa, si falla prueba dd-mm-aaaa y, como plan C,
       asigna ISO aaaa-mm-dd por JS.
    """
    page.goto(TM_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    dia_input = _find_day_input(page)
    dia_input.click()
    _press_select_all_delete(dia_input)

    # 1) dd/mm/aaaa
    dia_input.fill(dmy(dt))
    dia_input.blur()
    page.wait_for_timeout(150)

    # Si el control no acepta, probamos otra vez con guiones
    # Heurística: si el valor queda vacío, reintentamos
    try:
        val = dia_input.input_value()
    except:
        val = ""
    if not val:
        dia_input.click(); _press_select_all_delete(dia_input)
        dia_input.fill(dmy_dash(dt))
        dia_input.blur()
        page.wait_for_timeout(150)

    # 3) plan C: forzar por JS el ISO
    try:
        val = dia_input.input_value()
    except:
        val = ""
    if not val:
        try:
            page.evaluate("""(iso) => {
                const inp = document.querySelector('input[type="date"]') || document.querySelector('label:has-text("Día") ~ input') || document.querySelector('input');
                if (inp) {
                  inp.value = iso;
                  inp.dispatchEvent(new Event('input',{bubbles:true}));
                  inp.dispatchEvent(new Event('change',{bubbles:true}));
                }
            }""", ymd(dt))
        except:
            pass

    # Pulsar Buscar (o Enter como fallback)
    clicked = False
    try:
        btn = page.get_by_role("button", name=re.compile(r"\bBuscar\b", re.I))
        if btn.count():
            btn.first.click(); clicked = True
    except: pass
    if not clicked:
        for sel in ["button:has-text('Buscar')",
                    "input[type='submit'][value*='Buscar']",
                    "button[title*='Buscar']"]:
            loc = page.locator(sel).first
            if loc.count():
                loc.click(); clicked = True; break
    if not clicked:
        dia_input.press("Enter")

    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(600)

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

        login_tm(page)

        # fingerprint hoy-3 (para detectar si hoy-2 todavía muestra lo mismo)
        set_date_and_search(page, prev)
        time.sleep(2.0)
        fp_prev = table_fingerprint(page)

        # hoy-2
        set_date_and_search(page, target)
        t0 = time.time()
        fp_tgt = ""
        while time.time() - t0 < 12:
            fp_tgt = table_fingerprint(page)
            if fp_tgt and fp_tgt != fp_prev:
                break
            time.sleep(0.6)

        if not fp_tgt or fp_tgt == fp_prev:
            print("[INFO] La tabla de hoy-2 coincide con la de hoy-3 -> NO HAY DATOS.")
            write_no_data(target)
            browser.close()
            return

        df_tgt = read_table(page)
        if df_tgt.empty:
            print("[INFO] Tabla vacía para hoy-2 -> NO HAY DATOS.")
            write_no_data(target)
            browser.close()
            return

        # construir salida para los 6 medios
        candidates = [c for c in df_tgt.columns if norm(c) in {
            "nombre","medio","site","sitio","dominio","brand","marca","titulo","name"
        }]
        media_col = candidates[0] if candidates else df_tgt.columns[0]
        out = shape_output(df_tgt, media_col, target)

        if out.empty:
            print("[INFO] Tras filtro de medios, no hay filas -> NO HAY DATOS.")
            write_no_data(target)
            browser.close()
            return

        write_append_and_dedupe_types(out)
        browser.close()
        print("[DONE] OK")

if __name__ == "__main__":
    run()


