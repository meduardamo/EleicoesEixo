# enrich_relatorio_completo.py
import os
import re
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException


# =========================
# Config
# =========================
URL_LISTAR = "https://pesqele-divulgacao.tse.jus.br/app/pesquisa/listar.xhtml"
DETAIL_URL = "https://pesqele-divulgacao.tse.jus.br/app/pesquisa/detalhar.xhtml"

ID_ELEICAO_LABEL = "formPesquisa:eleicoes_label"
ID_ELEICAO_PANEL = "formPesquisa:eleicoes_panel"

ID_UF_LABEL = "formPesquisa:filtroUF_label"
ID_UF_PANEL = "formPesquisa:filtroUF_panel"

ID_BTN_PESQUISAR = "formPesquisa:idBtnPesquisar"
ID_TBODY = "formPesquisa:tabelaPesquisas_data"
ID_PAGINATOR = "formPesquisa:tabelaPesquisas_paginator_bottom"

CREDS_PATH = "credentials.json"

HEADER_ROW = 3
DATA_START_ROW = 4

SKIP_SHEETS = {"Dashboard"}

ELEICAO_TEXT = os.getenv("ELEICAO_TEXT", "Eleições Gerais 2026")

# Regras de recaptura
RECHECK_DAYS = int(os.getenv("RECHECK_DAYS", "3"))
PER_SHEET_LIMIT = int(os.getenv("PER_SHEET_LIMIT", "0") or "0") or None

# Drive
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "1AJMuqxwbKiRJxOZPS07SMZ_M-6222fK8")

# PDF
PDF_BUTTON_ID = "j_id_11:arquivoResultado"
PDF_DIR = os.getenv("PDF_DIR", "pdfs_relatorios")  # só temporário no runner

# Colunas que esse enrichment garante na planilha
NEEDED_COLS = [
    "numero_identificacao",
    "data_divulgacao",
    "uf_filtro",
    "pdf_relatorio_completo_drive",       # link do Drive
    "pdf_relatorio_completo_drive_id",    # id do arquivo no Drive
    "pdf_relatorio_completo_checado_em",  # timestamp ISO
]


# =========================
# Selenium helpers
# =========================
def make_driver(profile_dir: str = "./chrome-profile-pesqele", headless: bool = False) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")

    if headless or os.getenv("CI") or os.getenv("HEADLESS") == "1":
        opts.add_argument("--headless=new")

    if not os.getenv("CI"):
        opts.add_argument(f"--user-data-dir={os.path.abspath(profile_dir)}")

    if os.getenv("CI"):
        opts.binary_location = "/usr/bin/chromium-browser"
        return webdriver.Chrome(options=opts)

    return webdriver.Chrome(options=opts)


def wait_dom_ready(driver: webdriver.Chrome, timeout: int = 30) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )


def safe_click(driver: webdriver.Chrome, wait: WebDriverWait, by: By, value: str, timeout: int = 30):
    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value)))
    try:
        el.click()
    except Exception:
        driver.execute_script("arguments[0].click();", el)
    return el


def force_close_any_menu(driver: webdriver.Chrome):
    try:
        driver.switch_to.active_element.send_keys(Keys.ESCAPE)
    except Exception:
        pass
    try:
        driver.find_element(By.TAG_NAME, "body").click()
    except Exception:
        pass


def open_menu(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str) -> None:
    safe_click(driver, wait, By.ID, label_id)
    wait.until(EC.presence_of_element_located((By.ID, panel_id)))
    wait.until(EC.visibility_of_element_located((By.ID, panel_id)))


def select_one_menu_by_text(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str, text: str) -> None:
    open_menu(driver, wait, label_id, panel_id)

    panel = driver.find_element(By.ID, panel_id)
    item = panel.find_element(By.XPATH, f".//li[normalize-space()='{text}']")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
    try:
        item.click()
    except Exception:
        driver.execute_script("arguments[0].click();", item)

    force_close_any_menu(driver)


def click_and_wait_table_refresh(driver: webdriver.Chrome, wait: WebDriverWait, btn_id: str, tbody_id: str) -> None:
    old_tbody = None
    try:
        old_tbody = driver.find_element(By.ID, tbody_id)
    except Exception:
        pass

    btn = safe_click(driver, wait, By.ID, btn_id)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    try:
        btn.click()
    except Exception:
        driver.execute_script("arguments[0].click();", btn)

    if old_tbody is not None:
        try:
            wait.until(EC.staleness_of(old_tbody))
        except TimeoutException:
            pass

    wait.until(EC.presence_of_element_located((By.ID, tbody_id)))


def wait_list_page_ready(driver: webdriver.Chrome, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.ID, ID_TBODY)))
    wait.until(EC.presence_of_element_located((By.ID, ID_PAGINATOR)))


def wait_detail_page_ready(driver: webdriver.Chrome, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.ID, "print")))


def get_page_numbers(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str) -> List[int]:
    pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
    links = pag.find_elements(By.CSS_SELECTOR, "a.ui-paginator-page")

    nums = []
    for a in links:
        txt = (a.text or "").strip()
        if txt.isdigit():
            nums.append(int(txt))
    return sorted(set(nums))


def go_to_page(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str, tbody_id: str, page_num: int, max_tries: int = 6) -> None:
    last_err = None
    for _ in range(max_tries):
        try:
            pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
            a = pag.find_element(By.CSS_SELECTOR, f"a.ui-paginator-page[aria-label='Page {page_num}']")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)

            tbody_before = driver.find_element(By.ID, tbody_id)
            driver.execute_script("arguments[0].click();", a)

            try:
                wait.until(EC.staleness_of(tbody_before))
            except TimeoutException:
                pass

            wait.until(EC.presence_of_element_located((By.ID, tbody_id)))
            return

        except (StaleElementReferenceException, TimeoutException) as e:
            last_err = e
            time.sleep(0.5)

    raise last_err


def click_lupa_for_row(driver: webdriver.Chrome, wait: WebDriverWait, row_el) -> bool:
    try:
        lupa = row_el.find_element(By.CSS_SELECTOR, "a[id$=':detalhar']")
    except NoSuchElementException:
        try:
            lupa = row_el.find_element(By.CSS_SELECTOR, "a[title='Visualizar dados da pesquisa']")
        except NoSuchElementException:
            return False

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", lupa)
    try:
        driver.execute_script("arguments[0].click();", lupa)
    except Exception:
        try:
            lupa.click()
        except Exception:
            return False

    wait_detail_page_ready(driver, wait)
    return True


def open_detail_by_numero(driver: webdriver.Chrome, wait: WebDriverWait, numero: str) -> bool:
    numero = (numero or "").strip()
    if not numero:
        return False

    pages = get_page_numbers(driver, wait, ID_PAGINATOR)
    if not pages:
        pages = [1]

    for p in pages:
        if p != pages[0]:
            go_to_page(driver, wait, ID_PAGINATOR, ID_TBODY, p)

        tbody = driver.find_element(By.ID, ID_TBODY)
        trs = tbody.find_elements(By.XPATH, ".//tr")

        for tr in trs:
            tds = tr.find_elements(By.XPATH, "./td")
            if not tds:
                continue
            num_cell = (tds[0].text or "").strip()
            if num_cell == numero:
                return click_lupa_for_row(driver, wait, tr)

    return False


# =========================
# Relatório (sem URL): POST JSF e salva bytes
# =========================
def _slug(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9\-_\.]", "_", s)
    return s[:140] if len(s) > 140 else s


def _cookies_to_session(driver: webdriver.Chrome, sess: requests.Session) -> None:
    for c in driver.get_cookies():
        try:
            sess.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        except Exception:
            sess.cookies.set(c["name"], c["value"])


def _get_viewstate(driver: webdriver.Chrome) -> str:
    el = driver.find_element(By.NAME, "javax.faces.ViewState")
    return (el.get_attribute("value") or "").strip()


def baixar_relatorio_completo_no_detalhe(
    driver: webdriver.Chrome,
    numero_identificacao: str,
    out_dir: str = PDF_DIR,
    timeout: int = 60,
) -> Tuple[bool, str, str]:
    # verifica se tem botão e se está habilitado
    try:
        btn = driver.find_element(By.ID, PDF_BUTTON_ID)
    except Exception:
        return False, "", "botao_relatorio_nao_encontrado"

    if (btn.get_attribute("disabled") or "").strip() or (btn.get_attribute("aria-disabled") or "").strip().lower() == "true":
        return False, "", "botao_relatorio_desabilitado"

    # viewstate + cookies da sessão JSF
    try:
        viewstate = _get_viewstate(driver)
    except Exception:
        return False, "", "viewstate_nao_encontrado"

    sess = requests.Session()
    _cookies_to_session(driver, sess)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": DETAIL_URL,
        "Origin": "https://pesqele-divulgacao.tse.jus.br",
    }

    # payload mínimo (JSF/PrimeFaces)
    data = {
        "javax.faces.partial.ajax": "false",
        "javax.faces.source": PDF_BUTTON_ID,
        PDF_BUTTON_ID: PDF_BUTTON_ID,
        "javax.faces.ViewState": viewstate,
    }

    try:
        resp = sess.post(DETAIL_URL, headers=headers, data=data, timeout=timeout, allow_redirects=True)
    except Exception as e:
        return False, "", f"erro_request: {str(e)[:140]}"

    ct = (resp.headers.get("Content-Type") or "").lower()
    is_pdf = ("application/pdf" in ct) or (resp.content[:4] == b"%PDF")

    if not is_pdf:
        return False, "", f"nao_veio_pdf: ct={ct[:60]}"

    os.makedirs(out_dir, exist_ok=True)
    safe = _slug(numero_identificacao or "relatorio")
    out_path = os.path.abspath(os.path.join(out_dir, f"{safe}.pdf"))

    try:
        with open(out_path, "wb") as f:
            f.write(resp.content)
    except Exception as e:
        return False, "", f"erro_salvar_pdf: {str(e)[:140]}"

    return True, out_path, "ok"


# =========================
# Drive upload
# =========================
def drive_service(creds_path: str = CREDS_PATH):
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_pdf_to_drive(
    service,
    file_path: str,
    folder_id: str,
    desired_name: Optional[str] = None,
    replace_if_exists: bool = True,
) -> Tuple[str, str]:
    file_path = os.path.abspath(file_path)
    name = desired_name or os.path.basename(file_path)

    media = MediaFileUpload(file_path, mimetype="application/pdf", resumable=True)

    if replace_if_exists:
        q = f"'{folder_id}' in parents and name = '{name.replace('\'','\\\'')}' and trashed = false"
        res = service.files().list(q=q, fields="files(id,name)").execute()
        files = res.get("files", [])
        if files:
            fid = files[0]["id"]
            updated = service.files().update(
                fileId=fid,
                media_body=media,
                fields="id, webViewLink"
            ).execute()
            return updated["id"], updated.get("webViewLink", "")

    created = service.files().create(
        body={"name": name, "parents": [folder_id], "mimeType": "application/pdf"},
        media_body=media,
        fields="id, webViewLink"
    ).execute()

    return created["id"], created.get("webViewLink", "")


# =========================
# Sheets I/O
# =========================
def gspread_client(creds_path: str) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = os.getenv("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("SPREADSHEET_ID não definido (use Secret/env).")
    return gc.open_by_key(sid)


def ensure_header_has(ws: gspread.Worksheet, needed: List[str]) -> List[str]:
    header = ws.row_values(HEADER_ROW)
    header = [h.strip() for h in header if h is not None]

    if not header:
        ws.update(f"A{HEADER_ROW}", [needed])
        return needed

    missing = [c for c in needed if c not in header]
    if missing:
        ws.update(f"A{HEADER_ROW}", [header + missing])
        return header + missing

    return header


def col_idx(header: List[str], name: str) -> Optional[int]:
    try:
        return header.index(name) + 1
    except ValueError:
        return None


def build_rows(ws: gspread.Worksheet) -> Tuple[List[str], List[Dict[str, str]]]:
    values = ws.get_all_values()
    if len(values) < HEADER_ROW:
        return [], []

    header = [h.strip() for h in values[HEADER_ROW - 1]]
    rows = []

    for i in range(DATA_START_ROW - 1, len(values)):
        row_vals = values[i]
        d = {}
        for j, col in enumerate(header):
            d[col] = row_vals[j].strip() if j < len(row_vals) else ""
        d["__row_number"] = str(i + 1)
        rows.append(d)

    return header, rows


def update_cells_batch(ws: gspread.Worksheet, header: List[str], updates: List[Tuple[int, str, str]]) -> None:
    cells = []
    for row_number, col_name, value in updates:
        idx = col_idx(header, col_name)
        if not idx:
            continue
        cells.append(gspread.Cell(row_number, idx, value))
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


# =========================
# Regras de seleção (data_divulgacao + recaptura)
# =========================
def parse_iso_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return None
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))


def parse_iso_dt_to_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None
    y, mo, da = m.groups()
    return date(int(y), int(mo), int(da))


def should_check(row: Dict[str, str]) -> bool:
    today = date.today()

    # já tem link no drive: não reprocessa
    if (row.get("pdf_relatorio_completo_drive") or "").strip():
        return False

    # throttle: se já checou recentemente, espera
    checked = parse_iso_dt_to_date(row.get("pdf_relatorio_completo_checado_em") or "")
    if checked and (today - checked).days < RECHECK_DAYS:
        return False

    # se tem data_divulgacao, só tenta a partir dela
    data_div = parse_iso_date(row.get("data_divulgacao") or "")
    if data_div:
        return data_div <= today

    # sem data_divulgacao: tenta mesmo assim (vai ser mais caro)
    return True


# =========================
# Pipeline por worksheet
# =========================
def enrich_worksheet(ws: gspread.Worksheet, drv) -> None:
    title = ws.title
    if title in SKIP_SHEETS:
        return

    ensure_header_has(ws, NEEDED_COLS)
    header, rows = build_rows(ws)
    if not header:
        print(f"{title}: sem header")
        return

    candidates = []
    for r in rows:
        if not (r.get("numero_identificacao") or "").strip():
            continue
        if should_check(r):
            candidates.append(r)

    if not candidates:
        print(f"{title}: nada pra checar")
        return

    # prioriza quem tem data_divulgacao (mais provável ter PDF)
    def sort_key(r: Dict[str, str]) -> Tuple[int, str]:
        d = (r.get("data_divulgacao") or "").strip()
        return (0, d) if d else (1, "9999-99-99")

    candidates = sorted(candidates, key=sort_key)
    if PER_SHEET_LIMIT:
        candidates = candidates[:PER_SHEET_LIMIT]

    driver = make_driver(headless=(os.getenv("CI") or os.getenv("HEADLESS") == "1"))
    wait = WebDriverWait(driver, 30)

    updates: List[Tuple[int, str, str]] = []

    try:
        driver.get(URL_LISTAR)
        wait_dom_ready(driver)

        uf_text = (title or "").strip()

        select_one_menu_by_text(driver, wait, ID_ELEICAO_LABEL, ID_ELEICAO_PANEL, ELEICAO_TEXT)
        time.sleep(0.3)
        select_one_menu_by_text(driver, wait, ID_UF_LABEL, ID_UF_PANEL, uf_text)
        time.sleep(0.3)

        click_and_wait_table_refresh(driver, wait, ID_BTN_PESQUISAR, ID_TBODY)
        wait_list_page_ready(driver, wait)

        for i, r in enumerate(candidates, 1):
            numero = (r.get("numero_identificacao") or "").strip()
            row_number = int(r["__row_number"])

            print(f"{title}: {numero} ({i}/{len(candidates)})")

            drive_link = ""
            drive_id = ""

            ok_detail = False
            try:
                ok_detail = open_detail_by_numero(driver, wait, numero)
            except Exception:
                ok_detail = False

            if ok_detail:
                ok_pdf, local_path, msg = baixar_relatorio_completo_no_detalhe(driver, numero)
                if ok_pdf and local_path:
                    desired_name = f"{numero}.pdf"
                    try:
                        drive_id, drive_link = upload_pdf_to_drive(
                            drv,
                            file_path=local_path,
                            folder_id=DRIVE_FOLDER_ID,
                            desired_name=desired_name,
                            replace_if_exists=True
                        )
                    except Exception as e:
                        print(f"{title}: upload falhou para {numero}: {str(e)[:160]}")
                else:
                    # msg pode ser: desabilitado / não veio pdf / etc
                    pass

                driver.back()
                wait_dom_ready(driver)
                wait_list_page_ready(driver, wait)

            if drive_link:
                updates.append((row_number, "pdf_relatorio_completo_drive", drive_link))
            if drive_id:
                updates.append((row_number, "pdf_relatorio_completo_drive_id", drive_id))

            updates.append((row_number, "pdf_relatorio_completo_checado_em", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

            if len(updates) >= 150:
                update_cells_batch(ws, header, updates)
                updates = []

            time.sleep(0.35)

        if updates:
            update_cells_batch(ws, header, updates)

    finally:
        driver.quit()


def run_all() -> None:
    # Sheets
    gc = gspread_client(CREDS_PATH)
    ss = get_spreadsheet(gc)

    # Drive
    drv = drive_service(CREDS_PATH)

    for ws in ss.worksheets():
        if ws.title in SKIP_SHEETS:
            continue
        try:
            enrich_worksheet(ws, drv)
        except Exception as e:
            print(f"Erro em {ws.title}: {str(e)[:200]}")
            continue


if __name__ == "__main__":
    if not DRIVE_FOLDER_ID:
        raise SystemExit("DRIVE_FOLDER_ID não definido (coloque no Secret/env).")

    run_all()
    print("Enrichment de relatório completo finalizado.")
