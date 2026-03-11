import os
import re
import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
    NoSuchElementException,
)

import gspread
from google.oauth2.service_account import Credentials

URL_LISTAR = "https://pesqele-divulgacao.tse.jus.br/app/pesquisa/listar.xhtml"

ID_ELEICAO_LABEL = "formPesquisa:eleicoes_label"
ID_ELEICAO_PANEL = "formPesquisa:eleicoes_panel"

ID_UF_LABEL = "formPesquisa:filtroUF_label"
ID_UF_PANEL = "formPesquisa:filtroUF_panel"

ID_BTN_PESQUISAR = "formPesquisa:idBtnPesquisar"

ID_TBODY = "formPesquisa:tabelaPesquisas_data"
ID_PAGINATOR = "formPesquisa:tabelaPesquisas_paginator_bottom"

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1OEmfn_RyTgrkPenzXlc6qvySs8rbVV39qmuHoULwtjQ")
CREDS_PATH = "credentials.json"

# deixa 2 linhas livres (banner)
HEADER_ROW = 3
DATA_START_ROW = 4

COLS_BASE = [
    "numero_identificacao",
    "eleicao",
    "empresa_contratada",
    "data_registro",
    "abrangencia",
    "data_divulgacao",
    "cargos",
    "uf_filtro",
    "capturado_em",
]

SKIP_SHEETS = {"Dashboard"}


def make_driver(profile_dir: str = "./chrome-profile-pesqele", headless: bool = False) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")

    if headless or os.getenv("CI"):
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
        return el
    except ElementClickInterceptedException:
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


def select_one_menu_by_text(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    label_id: str,
    panel_id: str,
    text: str
) -> None:
    open_menu(driver, wait, label_id, panel_id)

    panel = driver.find_element(By.ID, panel_id)
    item = panel.find_element(By.XPATH, f".//li[normalize-space()='{text}']")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
    try:
        item.click()
    except Exception:
        driver.execute_script("arguments[0].click();", item)

    force_close_any_menu(driver)


def list_one_menu_items(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    label_id: str,
    panel_id: str
) -> List[str]:
    open_menu(driver, wait, label_id, panel_id)

    panel = driver.find_element(By.ID, panel_id)
    lis = panel.find_elements(By.CSS_SELECTOR, "li.ui-selectonemenu-item")

    items = []
    for li in lis:
        t = (li.text or "").strip()
        if not t:
            continue
        if t.lower() in {"selecione", "[selecione]"}:
            continue
        items.append(t)

    force_close_any_menu(driver)
    return items


def click_and_wait_table_refresh(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    btn_id: str,
    tbody_id: str
) -> None:
    try:
        old_tbody = driver.find_element(By.ID, tbody_id)
    except Exception:
        old_tbody = None

    # Re-busca o botão imediatamente antes de cada tentativa de clique
    for attempt in range(3):
        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, btn_id))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            break
        except StaleElementReferenceException:
            if attempt == 2:
                raise
            time.sleep(0.5)

    if old_tbody is not None:
        try:
            wait.until(EC.staleness_of(old_tbody))
        except TimeoutException:
            pass

    wait.until(EC.presence_of_element_located((By.ID, tbody_id)))


def dedup_by_numero(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in rows:
        k = (r.get("numero_identificacao") or "").strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def get_page_numbers(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str) -> List[int]:
    pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
    links = pag.find_elements(By.CSS_SELECTOR, "a.ui-paginator-page")

    nums = []
    for a in links:
        txt = (a.text or "").strip()
        if txt.isdigit():
            nums.append(int(txt))

    return sorted(set(nums))


def get_active_page(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str) -> Optional[int]:
    try:
        pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
        active = pag.find_elements(By.CSS_SELECTOR, "span.ui-paginator-page.ui-state-active")
        if not active:
            active = pag.find_elements(By.CSS_SELECTOR, "a.ui-paginator-page.ui-state-active")
        if not active:
            return None
        txt = (active[0].text or "").strip()
        return int(txt) if txt.isdigit() else None
    except Exception:
        return None


def go_to_page(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    paginator_id: str,
    tbody_id: str,
    page_num: int,
    max_tries: int = 6
) -> None:
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

        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException) as e:
            last_err = e
            time.sleep(0.5)

    raise last_err


def wait_list_page_ready(driver: webdriver.Chrome, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.ID, ID_TBODY)))
    wait.until(EC.presence_of_element_located((By.ID, ID_PAGINATOR)))


def wait_detail_page_ready(driver: webdriver.Chrome, wait: WebDriverWait):
    wait.until(EC.presence_of_element_located((By.ID, "print")))


def extract_field_by_label(driver: webdriver.Chrome, label_text: str) -> Optional[str]:
    xpaths = [
        f"//label[normalize-space()='{label_text}']/parent::td/following-sibling::td[1]",
        f"//label[contains(normalize-space(),'{label_text.rstrip(':')}')]/parent::td/following-sibling::td[1]",
        f"//td[normalize-space()='{label_text}']/following-sibling::td[1]",
    ]
    for xp in xpaths:
        try:
            el = driver.find_element(By.XPATH, xp)
            txt = (el.text or "").strip()
            if txt:
                return txt
        except NoSuchElementException:
            continue
    return None


def click_row_lupa_and_get_detail_fields(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    row_el,
    current_page: Optional[int],
) -> Dict[str, Optional[str]]:
    try:
        lupa = row_el.find_element(By.CSS_SELECTOR, "a[id$=':detalhar']")
    except NoSuchElementException:
        try:
            lupa = row_el.find_element(By.CSS_SELECTOR, "a[title='Visualizar dados da pesquisa']")
        except NoSuchElementException:
            return {"data_divulgacao": None, "cargos": None}

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", lupa)
    try:
        driver.execute_script("arguments[0].click();", lupa)
    except Exception:
        try:
            lupa.click()
        except Exception:
            return {"data_divulgacao": None, "cargos": None}

    wait_detail_page_ready(driver, wait)
    data_div = extract_field_by_label(driver, "Data de divulgação:")
    cargos = extract_field_by_label(driver, "Cargo(s):")

    driver.back()
    wait_dom_ready(driver)
    wait_list_page_ready(driver, wait)

    if current_page and current_page > 1:
        try:
            active = get_active_page(driver, wait, ID_PAGINATOR)
            if active != current_page:
                go_to_page(driver, wait, ID_PAGINATOR, ID_TBODY, current_page)
        except Exception:
            pass

    return {"data_divulgacao": data_div, "cargos": cargos}


def parse_current_table_with_details(driver: webdriver.Chrome, wait: WebDriverWait, tbody_id: str) -> List[Dict[str, str]]:
    tbody = driver.find_element(By.ID, tbody_id)
    rows = tbody.find_elements(By.XPATH, ".//tr")

    out: List[Dict[str, str]] = []
    current_page = get_active_page(driver, wait, ID_PAGINATOR)

    for r in rows:
        cols = [c.text.strip() for c in r.find_elements(By.XPATH, "./td")]
        if len(cols) < 5:
            continue

        try:
            details = click_row_lupa_and_get_detail_fields(driver, wait, r, current_page)
        except Exception:
            details = {"data_divulgacao": None, "cargos": None}

        out.append({
            "numero_identificacao": cols[0],
            "eleicao": cols[1],
            "empresa_contratada": cols[2],
            "data_registro": cols[3],
            "abrangencia": cols[4],
            "data_divulgacao": details.get("data_divulgacao"),
            "cargos": details.get("cargos"),
        })

        try:
            tbody = driver.find_element(By.ID, tbody_id)
            rows = tbody.find_elements(By.XPATH, ".//tr")
        except Exception:
            pass

    return out


def scrape_all_pages_current_query(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    paginator_id: str,
    tbody_id: str
) -> List[Dict[str, str]]:
    pages = get_page_numbers(driver, wait, paginator_id)
    if not pages:
        return dedup_by_numero(parse_current_table_with_details(driver, wait, tbody_id))

    all_rows: List[Dict[str, str]] = []
    for p in pages:
        go_to_page(driver, wait, paginator_id, tbody_id, p)
        all_rows.extend(parse_current_table_with_details(driver, wait, tbody_id))

    return dedup_by_numero(all_rows)


def sheet_safe(name: str) -> str:
    s = re.sub(r"[\[\]\:\*\?\/\\]", "-", name.strip())
    return s[:31] if len(s) > 31 else s


def gspread_client(creds_path: str) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    spreadsheet_id = os.getenv("SPREADSHEET_ID", SPREADSHEET_ID)
    return gc.open_by_key(spreadsheet_id)


def ensure_worksheet(ss: gspread.Spreadsheet, title: str, rows: int = 2000, cols: int = 30) -> gspread.Worksheet:
    title = sheet_safe(title)
    try:
        return ss.worksheet(title)
    except Exception:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def ensure_header(ws: gspread.Worksheet, header: List[str]) -> None:
    current = ws.row_values(HEADER_ROW)
    if current != header:
        ws.update(f"A{HEADER_ROW}", [header])


def get_existing_keys(ws: gspread.Worksheet, key_col_name: str = "numero_identificacao") -> set:
    header = ws.row_values(HEADER_ROW)
    if not header:
        return set()

    try:
        idx = header.index(key_col_name) + 1
    except ValueError:
        return set()

    col_vals = ws.col_values(idx)
    keys = set()

    for v in col_vals[DATA_START_ROW - 1:]:
        v = (v or "").strip()
        if v:
            keys.add(v)

    return keys


def parse_br_date_to_iso(s: str) -> str:
    s = (str(s) if s is not None else "").strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if not m:
        return ""
    d, mth, y = m.groups()
    return f"{y}-{mth}-{d}"


def parse_br_datetime_to_iso(s: str) -> str:
    s = (str(s) if s is not None else "").strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$", s)
    if not m:
        return ""
    d, mth, y, hh, mm, ss = m.groups()
    return f"{y}-{mth}-{d} {hh}:{mm}:{ss}"


def iso_date_sort_key(x: str) -> str:
    x = (str(x) if x is not None else "").strip()
    return x if re.match(r"^\d{4}-\d{2}-\d{2}$", x) else ""


def insert_new_rows_top(ws: gspread.Worksheet, df: pd.DataFrame, key_col_name: str = "numero_identificacao") -> int:
    if df is None or df.empty:
        return 0

    df = df.copy()

    for c in COLS_BASE:
        if c not in df.columns:
            df[c] = ""
    df = df[COLS_BASE].fillna("")

    df["data_registro"] = df["data_registro"].apply(parse_br_date_to_iso)
    df["data_divulgacao"] = df["data_divulgacao"].apply(parse_br_date_to_iso)
    df["capturado_em"] = df["capturado_em"].apply(parse_br_datetime_to_iso)

    ensure_header(ws, COLS_BASE)

    existing = get_existing_keys(ws, key_col_name=key_col_name)
    df_new = df[~df[key_col_name].astype(str).str.strip().isin(existing)].copy()

    if df_new.empty:
        return 0

    df_new["__sort"] = df_new["data_divulgacao"].apply(iso_date_sort_key)
    df_new = df_new.sort_values(by=["__sort", "numero_identificacao"], ascending=[False, False], kind="mergesort")
    df_new = df_new.drop(columns=["__sort"])

    values = df_new.astype(str).values.tolist()
    ws.insert_rows(values, row=DATA_START_ROW, value_input_option="USER_ENTERED")
    return len(df_new)


def run_one_scope(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    eleicao_text: str,
    uf_text: str,
    max_retries: int = 3
) -> pd.DataFrame:
    for attempt in range(max_retries):
        try:
            select_one_menu_by_text(driver, wait, ID_ELEICAO_LABEL, ID_ELEICAO_PANEL, eleicao_text)
            time.sleep(0.5)

            select_one_menu_by_text(driver, wait, ID_UF_LABEL, ID_UF_PANEL, uf_text)
            time.sleep(0.5)

            click_and_wait_table_refresh(driver, wait, ID_BTN_PESQUISAR, ID_TBODY)
            wait_list_page_ready(driver, wait)

            rows = scrape_all_pages_current_query(driver, wait, ID_PAGINATOR, ID_TBODY)
            df = pd.DataFrame(rows)

            df["uf_filtro"] = uf_text
            df["capturado_em"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            for c in COLS_BASE:
                if c not in df.columns:
                    df[c] = ""

            return df[COLS_BASE]

        except StaleElementReferenceException as e:
            if attempt < max_retries - 1:
                print(f"Tentativa {attempt + 1} falhou com stale element, tentando novamente...")
                time.sleep(2)
                driver.get(URL_LISTAR)
                wait_dom_ready(driver)
                time.sleep(1)
                continue
            raise e

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Tentativa {attempt + 1} falhou: {str(e)[:120]}, tentando novamente...")
                time.sleep(2)
                driver.get(URL_LISTAR)
                wait_dom_ready(driver)
                time.sleep(1)
                continue
            raise e


def run_to_google_sheets_insert_dedup(
    eleicao_text: str = "Eleições Gerais 2026",
    headless: bool = False
) -> None:
    gc = gspread_client(CREDS_PATH)
    ss = get_spreadsheet(gc)

    driver = make_driver(headless=headless)
    wait = WebDriverWait(driver, 30)

    try:
        driver.get(URL_LISTAR)
        wait_dom_ready(driver)

        if "BRASIL" not in SKIP_SHEETS:
            print("Processando BRASIL...")
            df_brasil = run_one_scope(driver, wait, eleicao_text=eleicao_text, uf_text="BRASIL")
            ws_brasil = ensure_worksheet(ss, "BRASIL", rows=2000, cols=max(30, len(COLS_BASE) + 5))
            novos = insert_new_rows_top(ws_brasil, df_brasil)
            print(f"BRASIL: {novos} registros novos inseridos")

        ufs = list_one_menu_items(driver, wait, ID_UF_LABEL, ID_UF_PANEL)
        ufs = [u for u in ufs if u.upper() not in {"BRASIL", "[SELECIONE]"}]

        for i, uf in enumerate(ufs, 1):
            if uf in SKIP_SHEETS:
                continue
            try:
                print(f"Processando {uf} ({i}/{len(ufs)})...")
                df_uf = run_one_scope(driver, wait, eleicao_text=eleicao_text, uf_text=uf)
                ws = ensure_worksheet(ss, uf, rows=2000, cols=max(30, len(COLS_BASE) + 5))
                novos = insert_new_rows_top(ws, df_uf)
                print(f"{uf}: {novos} registros novos inseridos")
                time.sleep(1)
            except Exception as e:
                print(f"Erro ao processar {uf}: {str(e)[:200]}")
                continue

    finally:
        driver.quit()


if __name__ == "__main__":
    eleicao = os.getenv("ELEICAO_TEXT", "Eleições Gerais 2026")
    headless = bool(os.getenv("CI", False))

    print(f"Iniciando scraper para: {eleicao}")
    print(f"Modo headless: {headless}")
    print(f"SPREADSHEET_ID: {os.getenv('SPREADSHEET_ID', SPREADSHEET_ID)}")

    run_to_google_sheets_insert_dedup(eleicao_text=eleicao, headless=headless)
    print("Atualização concluída (INSERT na linha 4; ISO + USER_ENTERED; Dashboard intocado).")
