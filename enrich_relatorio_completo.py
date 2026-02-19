import json
import os
import re
import time
from datetime import datetime
from typing import Optional, Tuple

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


PDF_LABEL = "Visualizar arquivo relatório completo com o resultado da pesquisa"


def _is_disabled(el) -> bool:
    disabled_attr = (el.get_attribute("disabled") or "").strip().lower()
    aria_disabled = (el.get_attribute("aria-disabled") or "").strip().lower()
    return disabled_attr in {"true", "disabled"} or aria_disabled == "true"


def _enable_network_capture(driver) -> None:
    # CDP: habilita Network (e afins) p/ capturar response com application/pdf
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Page.enable", {})
        driver.execute_cdp_cmd("Runtime.enable", {})
    except Exception:
        pass


def _clear_performance_logs(driver) -> None:
    # zera o buffer de logs p/ pegar só o que acontecer depois do clique
    try:
        driver.get_log("performance")
    except Exception:
        pass


def _find_relatorio_button(driver):
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


def _extract_pdf_url_from_performance_logs(driver, timeout: int = 18) -> str:
    deadline = time.time() + timeout
    best_url = ""

    while time.time() < deadline:
        try:
            logs = driver.get_log("performance")
        except Exception:
            logs = []

        for entry in logs:
            try:
                payload = json.loads(entry.get("message", "{}"))
                msg = payload.get("message", {})
                method = msg.get("method", "")
                params = msg.get("params", {})
            except Exception:
                continue

            if method != "Network.responseReceived":
                continue

            response = params.get("response", {}) or {}
            url = (response.get("url") or "").strip()
            if not url:
                continue

            mime = (response.get("mimeType") or "").lower()
            headers = response.get("headers") or {}
            ct = ""
            if isinstance(headers, dict):
                ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()

            if "application/pdf" in mime or "application/pdf" in ct:
                return url

            if url.lower().endswith(".pdf") or "pdf" in url.lower():
                best_url = url

        time.sleep(0.3)

    return best_url


def _download_with_session_cookies(driver, url: str, out_path: str) -> bool:
    if not url:
        return False

    sess = requests.Session()

    # reaproveita cookies do selenium (sessão JSF)
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


def try_get_relatorio_completo(
    driver,
    wait: WebDriverWait,
    download: bool = False,
    pdf_dir: str = "pdfs_relatorios",
    filename_hint: Optional[str] = None,
    click_timeout: int = 18,
) -> Tuple[str, str]:
    """
    Chame isso já dentro do detalhar.xhtml (após wait do #print).

    Retorna:
      (pdf_url, local_path)

    - pdf_url: URL capturada via Network (CDP/performance logs), se aparecer.
    - local_path: caminho do PDF salvo, se download=True e o download der certo.
    """

    btn = _find_relatorio_button(driver)
    if not btn:
        return "", ""

    if _is_disabled(btn):
        return "", ""

    _enable_network_capture(driver)
    _clear_performance_logs(driver)

    # clique no botão (JSF/PrimeFaces)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    try:
        driver.execute_script("arguments[0].click();", btn)
    except Exception:
        try:
            btn.click()
        except Exception:
            return "", ""

    pdf_url = _extract_pdf_url_from_performance_logs(driver, timeout=click_timeout)

    if not download or not pdf_url:
        return pdf_url, ""

    safe = re.sub(r"[^A-Za-z0-9\-_\.]", "_", (filename_hint or "relatorio"))
    out_path = os.path.abspath(os.path.join(pdf_dir, f"{safe}.pdf"))

    ok = _download_with_session_cookies(driver, pdf_url, out_path)
    return pdf_url, (out_path if ok else "")
