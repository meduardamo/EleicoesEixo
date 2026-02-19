import os
import re
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

CREDS_PATH = "credentials.json"

HEADER_ROW = 3
DATA_START_ROW = 4

SKIP_SHEETS = {"Dashboard"}

NEEDED_COLS = [
    "detail_url",
    "pdf_relatorio_completo_url",
    "pdf_relatorio_completo_local",
    "pdf_relatorio_completo_checado_em",
]

FALLBACK_DAYS_AFTER_REGISTRO = 7
RECHECK_DAYS = 3

PDF_LABEL = "Visualizar arquivo relatório completo com o resultado da pesquisa"


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


def gspread_client(creds_path: str) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Defina SPREADSHEET_ID (env/secrets).")
    return gc.open_by_key(spreadsheet_id)


def iso_to_date(x: str) -> Optional[date]:
    x = (x or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", x):
        return None
    y, m, d = x.split("-")
    return date(int(y), int(m), int(d))


def iso_dt_to_date(x: str) -> Optional[date]:
    x = (x or "").strip()
    if not x:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", x)
    if not m:
        return None
    y, mo, da = m.groups()
    return date(int(y), int(mo), int(da))


def ensure_header_has(ws: gspread.Worksheet, needed_cols: List[str]) -> List[str]:
    header = ws.row_values(HEADER_ROW)
    header = [h.strip() for h in header if h is not None]

    if not header:
        ws.update(f"A{HEADER_ROW}", [["numero_identificacao", "data_divulgacao", "data_registro"] + needed_cols])
        return ws.row_values(HEADER_ROW)

    missing = [c for c in needed_cols if c not in header]
    if missing:
        new_header = header + missing
        ws.update(f"A{HEADER_ROW}", [new_header])
        return new_header

    return header


def col_idx(header: List[str], name: str) -> Optional[int]:
    try:
        return header.index(name) + 1
    except ValueError:
        return None


def build_rows_from_sheet(ws: gspread.Worksheet) -> Tuple[List[str], List[Dict[str, str]]]:
    values = ws.get_all_values()
    if len(values) < HEADER_ROW:
        return [], []

    header_row = values[HEADER_ROW - 1]
    header_row = [h.strip() for h in header_row]

    rows = []
    for i in range(DATA_START_ROW - 1, len(values)):
        row_vals = values[i]
        row_dict = {}
        for j, col in enumerate(header_row):
            row_dict[col] = row_vals[j].strip() if j < len(row_vals) else ""
        row_dict["__row_number"] = str(i + 1)
        rows.append(row_dict)

    return header_row, rows


def update_cells_batch(ws: gspread.Worksheet, header: List[str], updates: List[Tuple[int, str, str]]) -> None:
    cell_updates = []
    for row_number, col_name, value in updates:
        idx = col_idx(header, col_name)
        if not idx:
            continue
        cell_updates.append(gspread.Cell(row_number, idx, value))
    if cell_updates:
        ws.update_cells(cell_updates, value_input_option="USER_ENTERED")


def should_check_row(row: Dict[str, str]) -> bool:
    today = date.today()

    detail_url = (row.get("detail_url") or "").strip()
    if not detail_url:
        return False

    if (row.get("pdf_relatorio_completo_url") or "").strip():
        return False

    checked_at = iso_dt_to_date(row.get("pdf_relatorio_completo_checado_em") or "")
    if checked_at and (today - checked_at).days < RECHECK_DAYS:
        return False

    data_div = iso_to_date(row.get("data_divulgacao") or "")
    if data_div:
        return data_div <= today

    reg = iso_to_date(row.get("data_registro") or "")
    if reg:
        return (today - reg).days >= FALLBACK_DAYS_AFTER_REGISTRO

    return True


def is_disabled(el) -> bool:
    disabled_attr = (el.get_attribute("disabled") or "").strip().lower()
    aria_disabled = (el.get_attribute("aria-disabled") or "").strip().lower()
    if disabled_attr in {"true", "disabled"}:
        return True
    if aria_disabled == "true":
        return True
    return False


def find_relatorio_button(driver: webdriver.Chrome) -> Optional[object]:
    xps = [
        f"//span[contains(normalize-space(), '{PDF_LABEL}')]/ancestor::button[1]",
        f"//button[contains(normalize-space(), '{PDF_LABEL}')]",
    ]
    for xp in xps:
        try:
            return driver.find_element(By.XPATH, xp)
        except Exception:
            pass
    return None


def enable_network_capture(driver: webdriver.Chrome) -> None:
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Page.enable", {})
        driver.execute_cdp_cmd("Runtime.enable", {})
    except Exception:
        pass


def clear_browser_logs(driver: webdriver.Chrome) -> None:
    try:
        driver.get_log("performance")
    except Exception:
        pass


def click_relatorio(driver: webdriver.Chrome, btn) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    try:
        driver.execute_script("arguments[0].click();", btn)
    except Exception:
        btn.click()


def extract_pdf_url_from_performance_logs(driver: webdriver.Chrome, timeout: int = 15) -> str:
    deadline = time.time() + timeout
    best_url = ""

    while time.time() < deadline:
        try:
            logs = driver.get_log("performance")
        except Exception:
            logs = []

        for entry in logs:
            try:
                msg = entry.get("message", "")
                if not msg:
                    continue
                data = msg
                if isinstance(data, str):
                    import json
                    data = json.loads(data)
                message = data.get("message", {})
                method = message.get("method", "")
                params = message.get("params", {})
            except Exception:
                continue

            if method == "Network.responseReceived":
                response = params.get("response", {}) or {}
                mime = (response.get("mimeType") or "").lower()
                url = (response.get("url") or "").strip()
                if "application/pdf" in mime and url:
                    return url

                ct = ""
                headers = response.get("headers") or {}
                if isinstance(headers, dict):
                    ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()
                if "application/pdf" in ct and url:
                    return url

                # às vezes o mime não vem como pdf mas o URL sugere arquivo
                if url and (url.lower().endswith(".pdf") or "pdf" in url.lower()):
                    best_url = url

        time.sleep(0.3)

    return best_url


def download_with_session_cookies(driver: webdriver.Chrome, url: str, out_path: str) -> bool:
    if not url:
        return False

    sess = requests.Session()

    for c in driver.get_cookies():
        try:
            sess.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        except Exception:
            sess.cookies.set(c["name"], c["value"])

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Referer": driver.current_url,
    }

    try:
        resp = sess.get(url, headers=headers, timeout=60, allow_redirects=True)
        if resp.status_code != 200:
            return False

        ct = (resp.headers.get("Content-Type") or "").lower()
        if "pdf" not in ct and not resp.content[:4] == b"%PDF":
            return False

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception:
        return False


def extract_and_download_relatorio(driver: webdriver.Chrome, wait: WebDriverWait, detail_url: str, numero: str) -> Tuple[str, str]:
    driver.get(detail_url)
    wait_dom_ready(driver)
    try:
        wait.until(EC.presence_of_element_located((By.ID, "print")))
    except TimeoutException:
        pass

    btn = find_relatorio_button(driver)
    if not btn:
        return "", ""

    if is_disabled(btn):
        return "", ""

    enable_network_capture(driver)
    clear_browser_logs(driver)

    click_relatorio(driver, btn)

    pdf_url = extract_pdf_url_from_performance_logs(driver, timeout=18)

    # baixa o arquivo (mesmo se a url for “meia-boca”, tentamos)
    safe_num = re.sub(r"[^A-Za-z0-9\-_\.]", "_", (numero or "sem_numero"))
    out_path = os.path.abspath(os.path.join("pdfs_relatorios", f"{safe_num}.pdf"))

    ok = download_with_session_cookies(driver, pdf_url, out_path) if pdf_url else False
    return pdf_url, (out_path if ok else "")


def enrich_one_worksheet(ws: gspread.Worksheet, headless: bool = False, limit: Optional[int] = None) -> None:
    _ = ensure_header_has(ws, NEEDED_COLS)
    header, rows = build_rows_from_sheet(ws)

    candidates = [r for r in rows if should_check_row(r)]

    def sort_key(r: Dict[str, str]) -> Tuple[int, str]:
        d = r.get("data_divulgacao") or ""
        return (0, d) if d else (1, "9999-99-99")

    candidates = sorted(candidates, key=sort_key)
    if limit:
        candidates = candidates[:limit]

    if not candidates:
        print(f"{ws.title}: nada pra checar")
        return

    driver = make_driver(headless=headless)
    wait = WebDriverWait(driver, 30)

    updates: List[Tuple[int, str, str]] = []

    try:
        for i, r in enumerate(candidates, 1):
            numero = (r.get("numero_identificacao") or "").strip()
            detail_url = (r.get("detail_url") or "").strip()
            row_number = int(r["__row_number"])

            print(f"{ws.title}: relatorio completo {numero} ({i}/{len(candidates)})")

            pdf_url, local_path = ("", "")
            try:
                pdf_url, local_path = extract_and_download_relatorio(driver, wait, detail_url, numero)
            except Exception:
                pdf_url, local_path = ("", "")

            if pdf_url:
                updates.append((row_number, "pdf_relatorio_completo_url", pdf_url))
            if local_path:
                updates.append((row_number, "pdf_relatorio_completo_local", local_path))

            updates.append((row_number, "pdf_relatorio_completo_checado_em", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

            if len(updates) >= 150:
                update_cells_batch(ws, header, updates)
                updates = []

            time.sleep(0.4)

        if updates:
            update_cells_batch(ws, header, updates)

    finally:
        driver.quit()


def run_enrichment_all_sheets(headless: bool = False, per_sheet_limit: Optional[int] = None) -> None:
    gc = gspread_client(CREDS_PATH)
    ss = get_spreadsheet(gc)

    for ws in ss.worksheets():
        if ws.title in SKIP_SHEETS:
            continue
        try:
            enrich_one_worksheet(ws, headless=headless, limit=per_sheet_limit)
        except Exception as e:
            print(f"Erro em {ws.title}: {str(e)[:200]}")
            continue


if __name__ == "__main__":
    headless = bool(os.getenv("CI", False)) or os.getenv("HEADLESS") == "1"

    per_sheet_limit = os.getenv("PER_SHEET_LIMIT", "")
    per_sheet_limit = int(per_sheet_limit) if per_sheet_limit.strip().isdigit() else None

    print(f"Enriquecendo relatório completo (URL + download). HEADLESS={headless}")
    run_enrichment_all_sheets(headless=headless, per_sheet_limit=per_sheet_limit)
    print("Enriquecimento concluído.")
