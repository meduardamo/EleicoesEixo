import os
import re
import time
import json
import hashlib
from datetime import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager
    _HAS_WDM = True
except Exception:
    _HAS_WDM = False


UFS = [
    "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go",
    "ma", "mg", "ms", "mt", "pa", "pb", "pe", "pi", "pr",
    "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to"
]

PRESIDENTE_URLS_DEFAULT = [
    "https://www.pollingdata.com.br/2026/presidente/br/t1_lula-flavio-sem-bolsonaros/"
]

WAIT_CSS = "div#dados-das-pesquisas"


def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "t", "yes", "y", "sim", "on")


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _slug(s: str) -> str:
    s = _norm_ws(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _sha1_short(s: str, n=10) -> str:
    return hashlib.sha1(str(s).encode("utf-8", errors="ignore")).hexdigest()[:n]


def parse_url_meta(url: str):
    u = url.strip()

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>governador)/(?P<uf>[a-z]{2})/.*?_t(?P<turno>\d)\.html",
        u, re.I
    )
    if m:
        return {
            "ano": int(m.group("ano")),
            "cargo": "governador",
            "uf": m.group("uf").upper(),
            "turno": f"t{m.group('turno')}",
        }

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>presidente)/(?P<uf>br)/(?P<turno>t\d)",
        u, re.I
    )
    if m:
        return {
            "ano": int(m.group("ano")),
            "cargo": "presidente",
            "uf": "BR",
            "turno": m.group("turno").lower(),
        }

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>senador)/(?P<uf>[a-z]{2})/(?P<turno>t\d)/?$",
        u, re.I
    )
    if m:
        return {
            "ano": int(m.group("ano")),
            "cargo": "senador",
            "uf": m.group("uf").upper(),
            "turno": m.group("turno").lower(),
        }

    return {"ano": None, "cargo": None, "uf": None, "turno": None}


def parsear_pesquisa(texto):
    nome, id_pesquisa, data = "", "", ""
    linhas = [l.strip() for l in str(texto).strip().split("\n") if l.strip()]
    for linha in linhas:
        if re.match(r"^\(\d+\)$", linha):
            continue
        match_data = re.search(r"(\d{4}-\d{2}-\d{2})", linha)
        if match_data:
            data = match_data.group(1)
            antes = linha[:linha.index(data)].strip()
            if antes:
                id_pesquisa = antes
        else:
            nome = re.sub(r"\s*\(\d+\)\s*$", "", linha).strip()
    return _norm_ws(nome), _norm_ws(id_pesquisa), _norm_ws(data)


def parsear_pct(valor):
    v = str(valor).strip()
    if not v or v in ("-", "NaN%", "nan%", "NaN", "nan", ""):
        return None
    try:
        return float(v.replace("%", "").replace(",", ".").strip())
    except Exception:
        return None


def parsear_candidato_partido(col_header):
    m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", str(col_header).strip())
    if m:
        return _norm_ws(m.group(1)), _norm_ws(m.group(2))
    return _norm_ws(col_header), ""


def inferir_confianca(erro_conf):
    s = str(erro_conf or "")
    m = re.search(r"(\d{2,3})\s*%\s*\)", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def inferir_margem_erro(erro_conf):
    s = str(erro_conf or "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", s)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def gerar_poll_id(uf, instituto, id_pesquisa, data_campo, cargo, turno, raw_block_hash):
    uf = uf.upper()
    data_campo = _norm_ws(data_campo)
    instituto_slug = _slug(instituto)

    if id_pesquisa and id_pesquisa.lower() not in ("sem registro", "sem_registro", "semregistro", "nan"):
        return f"{uf}|{cargo}|{turno}|{id_pesquisa}|{data_campo}"

    return f"{uf}|{cargo}|{turno}|{instituto_slug}|{data_campo}|{raw_block_hash}"


def gerar_scenario_id(poll_id, scenario_label):
    return f"{poll_id}|{_norm_ws(scenario_label)}"


def urls_governador_2026_t1(ufs):
    return [f"https://www.pollingdata.com.br/2026/governador/{uf}/2026_governador_{uf}_t1.html" for uf in ufs]


def urls_senado_2026_t1(ufs):
    return [f"https://www.pollingdata.com.br/2026/senador/{uf}/t1/" for uf in ufs]


def montar_urls(incluir_governador: bool, incluir_senado: bool, incluir_presidente: bool):
    urls = []
    if incluir_governador:
        urls += urls_governador_2026_t1(UFS)
    if incluir_senado:
        urls += urls_senado_2026_t1(UFS)
    if incluir_presidente:
        urls += list(PRESIDENTE_URLS_DEFAULT)
    return urls


def criar_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if _HAS_WDM:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service()

    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def expandir_todos(driver, secao, max_clicks=120):
    i = 0
    while True:
        btns = secao.find_elements(By.CSS_SELECTOR, "button.rt-expander-button")
        fechados = [b for b in btns if b.get_attribute("aria-expanded") == "false"]
        if not fechados:
            break
        driver.execute_script("arguments[0].click();", fechados[0])
        time.sleep(0.8)
        i += 1
        if i >= max_clicks:
            break


def extrair_tabela_react(secao):
    headers = []
    for el in secao.find_elements(By.CSS_SELECTOR, "div.rt-thead .rt-th"):
        inner = el.find_elements(By.CSS_SELECTOR, ".rt-text-content, .rt-sort-header")
        text = inner[0].text.strip() if inner else el.text.strip()
        text = text.replace("\n", " ").strip()
        if text:
            headers.append(text)

    rows_data = []
    for group in secao.find_elements(By.CSS_SELECTOR, "div.rt-tbody div.rt-tr-group"):
        for row in group.find_elements(By.CSS_SELECTOR, "div.rt-tr"):
            cells = row.find_elements(By.CSS_SELECTOR, "div.rt-td")
            if not cells:
                continue
            vals = [c.text.strip() for c in cells]
            if any(vals):
                rows_data.append(vals)

    if not rows_data:
        return None

    n_cols = max(len(r) for r in rows_data)
    if len(headers) < n_cols:
        headers += [f"Col_{i}" for i in range(len(headers), n_cols)]
    headers = headers[:n_cols]

    return pd.DataFrame(rows_data, columns=headers)


def scrape_url(driver, url: str, horario_raspagem: str):
    meta = parse_url_meta(url)
    ano = meta["ano"]
    cargo = meta["cargo"]
    uf = meta["uf"]
    turno = meta["turno"]

    if not cargo or not uf or not turno:
        print(f"[-] URL não reconhecida: {url}")
        return None, None

    print(f"[+] {cargo.upper()} {uf} {turno} -> {url}")
    driver.get(url)

    time.sleep(10)  # aguarda carregamento inicial

    try:
        wait = WebDriverWait(driver, 40)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS)))
    except Exception:
        print(f"  [-] timeout (sem container)")
        return None, None

    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    df_raw = extrair_tabela_react(secao)
    if df_raw is None or df_raw.empty:
        print(f"  [-] sem tabela")
        return None, None

    col_pesquisa = df_raw.columns.tolist()[0]
    df_raw[col_pesquisa] = df_raw[col_pesquisa].replace("", pd.NA).ffill()

    parsed = df_raw[col_pesquisa].apply(parsear_pesquisa)
    df_raw["instituto"] = parsed.apply(lambda x: x[0])
    df_raw["registro_tse"] = parsed.apply(lambda x: x[1])
    df_raw["data_campo"] = parsed.apply(lambda x: x[2])
    df_raw["_block_hash"] = df_raw[col_pesquisa].apply(lambda x: _sha1_short(_norm_ws(x), 10))
    df_raw = df_raw.drop(columns=[col_pesquisa])

    if "Cenários" not in df_raw.columns:
        df_raw["Cenários"] = ""

    meta_expected = {"Modo Pesquisa", "Entrevistas", "Erro (Confiança)", "Cenários"}
    cols_meta = [c for c in df_raw.columns if c in meta_expected] + ["instituto", "registro_tse", "data_campo", "_block_hash"]
    cols_meta = [c for c in cols_meta if c in df_raw.columns]

    cols_cand = [c for c in df_raw.columns if c not in cols_meta]
    cols_cand = [
        c for c in cols_cand
        if re.search(r"\([A-Za-z]{2,}\)", str(c)) or str(c).lower().strip() in ("não válido", "nao valido")
    ]

    pesquisas_rows = []
    resultados_rows = []

    for _, row in df_raw.iterrows():
        instituto = _norm_ws(row.get("instituto", ""))
        registro_tse = _norm_ws(row.get("registro_tse", ""))
        data_campo = _norm_ws(row.get("data_campo", ""))
        modo = _norm_ws(row.get("Modo Pesquisa", ""))
        entrevistas_raw = _norm_ws(row.get("Entrevistas", ""))
        erro_conf = _norm_ws(row.get("Erro (Confiança)", ""))
        scenario_label = _norm_ws(row.get("Cenários", "")) or "NA"
        block_hash = _norm_ws(row.get("_block_hash", ""))

        poll_id = gerar_poll_id(uf, instituto, registro_tse, data_campo, cargo, turno, block_hash)
        scenario_id = gerar_scenario_id(poll_id, scenario_label)

        amostra = None
        try:
            if entrevistas_raw and str(entrevistas_raw).strip().isdigit():
                amostra = int(str(entrevistas_raw).strip())
        except Exception:
            amostra = None

        margem = inferir_margem_erro(erro_conf)
        confianca = inferir_confianca(erro_conf)

        pesquisas_rows.append({
            "scenario_id": scenario_id,
            "poll_id": poll_id,
            "ano": ano,
            "uf": uf,
            "cargo": cargo,
            "turno": turno,
            "instituto": instituto,
            "registro_tse": registro_tse,
            "data_campo": data_campo,
            "modo": modo,
            "amostra": amostra,
            "margem_erro": margem,
            "confianca": confianca,
            "scenario_label": scenario_label,
            "fonte_url": url,
            "horario_raspagem": horario_raspagem,
            "conferida": "",  # coluna para marcação manual
        })

        for col in cols_cand:
            colname = _norm_ws(col)
            pct = parsear_pct(row.get(col, ""))

            if pct is None:
                continue

            if colname.lower() in ("não válido", "nao valido"):
                candidato = "Não válido"
                partido = ""
                tipo = "nao_valido"
            else:
                candidato, partido = parsear_candidato_partido(colname)
                tipo = "candidato"

            resultados_rows.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "ano": ano,
                "uf": uf,
                "cargo": cargo,
                "turno": turno,
                "data_campo": data_campo,
                "instituto": instituto,
                "registro_tse": registro_tse,
                "scenario_label": scenario_label,
                "candidato": candidato,
                "partido": partido,
                "tipo": tipo,
                "percentual": pct,
                "fonte_url": url,
                "horario_raspagem": horario_raspagem,
            })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    print(f"  [+] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


def gs_client_from_env():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não definido (GitHub secret).")

    creds_dict = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def garantir_aba(spreadsheet, nome_aba, rows=2000, cols=20):
    try:
        return spreadsheet.worksheet(nome_aba)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=nome_aba, rows=rows, cols=cols)


def _aba_vazia(values):
    if not values:
        return True
    if len(values) == 1 and (not values[0] or all(str(x).strip() == "" for x in values[0])):
        return True
    if len(values) == 1 and len(values[0]) == 1 and str(values[0][0]).strip() == "":
        return True
    return False


def dedup_e_salvar_por_chave(aba, df_novo: pd.DataFrame, key_col: str):
    if key_col not in df_novo.columns:
        raise RuntimeError(f"df_novo não tem coluna de chave: '{key_col}'")

    antes = len(df_novo)
    df_novo = df_novo.drop_duplicates(subset=[key_col], keep="first").reset_index(drop=True)
    if len(df_novo) < antes:
        print(f"  [dedup interno] removidas {antes - len(df_novo)} linhas duplicadas na coleta")

    values = aba.get_all_values()

    if _aba_vazia(values):
        data_to_write = [df_novo.columns.tolist()] + df_novo.fillna("").astype(str).values.tolist()
        aba.clear()
        aba.update(data_to_write)
        print(f"  [aba vazia] {len(df_novo)} linhas gravadas")
        return len(df_novo), 0

    header_existente = values[0]

    if key_col not in header_existente:
        print(f"  [AVISO] coluna '{key_col}' não encontrada no header existente. Reescrevendo aba.")
        data_to_write = [df_novo.columns.tolist()] + df_novo.fillna("").astype(str).values.tolist()
        aba.clear()
        aba.update(data_to_write)
        return len(df_novo), 0

    idx_key = header_existente.index(key_col)
    existing_keys = {
        row[idx_key]
        for row in values[1:]
        if len(row) > idx_key and row[idx_key].strip()
    }

    df_add = df_novo[~df_novo[key_col].astype(str).isin(existing_keys)].reset_index(drop=True)

    if df_add.empty:
        print(f"  [sem novidades] todas as {len(existing_keys)} chaves já existem")
        return 0, len(existing_keys)

    if header_existente != df_add.columns.tolist():
        colunas_novas = [c for c in df_add.columns if c not in header_existente]
        if colunas_novas:
            header_final = header_existente + colunas_novas
            aba.update([header_final], range_name="A1")
            print(f"  [schema] {len(colunas_novas)} coluna(s) nova(s) adicionada(s) ao header: {colunas_novas}")
        else:
            header_final = header_existente

        df_add = df_add.reindex(columns=header_final, fill_value="")

    rows_to_append = df_add.fillna("").astype(str).values.tolist()
    aba.append_rows(rows_to_append, value_input_option="RAW", insert_data_option="INSERT_ROWS")

    print(f"  [insert] {len(df_add)} linha(s) nova(s) | {len(existing_keys)} já existiam")
    return len(df_add), len(existing_keys)


def main():
    sheet_id = (os.getenv("SPREADSHEET_ID_POLLING", "") or "").strip()
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID_POLLING não definido (GitHub secret).")

    incluir_governador = env_bool("INCLUIR_GOVERNADOR", True)
    incluir_senado = env_bool("INCLUIR_SENADO", False)
    incluir_presidente = env_bool("INCLUIR_PRESIDENTE", False)

    urls = montar_urls(incluir_governador, incluir_senado, incluir_presidente)
    if not urls:
        print("[-] Nenhuma URL selecionada. Ajuste INCLUDE_*.")
        return

    horario_raspagem = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    print("[+] Conectando ao Google Sheets...")
    gc = gs_client_from_env()
    sh = gc.open_by_key(sheet_id)

    aba_pesquisas = garantir_aba(sh, "pesquisas", rows=5000, cols=20)
    aba_resultados = garantir_aba(sh, "resultados", rows=20000, cols=25)

    print("[+] Iniciando Chrome...")
    driver = criar_driver()

    all_p = []
    all_r = []

    try:
        for url in urls:
            df_p, df_r = scrape_url(driver, url, horario_raspagem)
            if df_p is not None and not df_p.empty:
                all_p.append(df_p)
            if df_r is not None and not df_r.empty:
                all_r.append(df_r)
    finally:
        driver.quit()

    print("[+] Salvando...")

    if all_p:
        df_p_all = pd.concat(all_p, ignore_index=True)
        novos, exist = dedup_e_salvar_por_chave(aba_pesquisas, df_p_all, key_col="scenario_id")
        print(f"[+] pesquisas: {novos} novas linhas | {exist} já existiam")
    else:
        print("[-] pesquisas: nada coletado")

    if all_r:
        df_r_all = pd.concat(all_r, ignore_index=True)
        df_r_all["_dedup_key"] = (
            df_r_all["scenario_id"].astype(str)
            + "|" + df_r_all["tipo"].astype(str)
            + "|" + df_r_all["candidato"].astype(str)
        )
        novos, exist = dedup_e_salvar_por_chave(aba_resultados, df_r_all, key_col="_dedup_key")
        print(f"[+] resultados: {novos} novas linhas | {exist} já existiam")
    else:
        print("[-] resultados: nada coletado")

    print("[+] OK")


if __name__ == "__main__":
    main()
