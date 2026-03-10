import os
import re
import time
import json
import hashlib
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

# --- CONFIGURAÇÕES ---
UFS = [
    "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go",
    "ma", "mg", "ms", "mt", "pa", "pb", "pe", "pi", "pr",
    "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to"
]

PRESIDENTE_URLS_DEFAULT = [
    "https://www.pollingdata.com.br/2026/presidente/br/t1_lula-flavio-sem-bolsonaros/"
]

WAIT_CSS = "div#dados-das-pesquisas"

# --- UTILITÁRIOS ---
def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v == "": return default
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
    # Governador
    m = re.search(r"/(?P<ano>\d{4})/(?P<cargo>governador)/(?P<uf>[a-z]{2})/.*?_t(?P<turno>\d)\.html", u, re.I)
    if m:
        return {"ano": int(m.group("ano")), "cargo": "governador", "uf": m.group("uf").upper(), "turno": f"t{m.group('turno')}"}
    # Presidente
    m = re.search(r"/(?P<ano>\d{4})/(?P<cargo>presidente)/(?P<uf>br)/(?P<turno>t\d)", u, re.I)
    if m:
        return {"ano": int(m.group("ano")), "cargo": "presidente", "uf": "BR", "turno": m.group("turno").lower()}
    # Senador
    m = re.search(r"/(?P<ano>\d{4})/(?P<cargo>senador)/(?P<uf>[a-z]{2})/(?P<turno>t\d)/?$", u, re.I)
    if m:
        return {"ano": int(m.group("ano")), "cargo": "senador", "uf": m.group("uf").upper(), "turno": m.group("turno").lower()}
    return {"ano": None, "cargo": None, "uf": None, "turno": None}

def parsear_pesquisa(texto):
    nome, id_pesquisa, data = "", "", ""
    linhas = [l.strip() for l in str(texto).strip().split("\n") if l.strip()]
    for linha in linhas:
        if re.match(r"^\(\d+\)$", linha): continue
        match_data = re.search(r"(\d{4}-\d{2}-\d{2})", linha)
        if match_data:
            data = match_data.group(1)
            antes = linha[:linha.index(data)].strip()
            if antes: id_pesquisa = antes
        else:
            nome = re.sub(r"\s*\(\d+\)\s*$", "", linha).strip()
    return _norm_ws(nome), _norm_ws(id_pesquisa), _norm_ws(data)

def parsear_pct(valor):
    v = str(valor).strip()
    if not v or v in ("-", "NaN%", "nan%", "NaN", "nan", ""): return None
    try:
        return float(v.replace("%", "").replace(",", ".").strip())
    except Exception: return None

def parsear_candidato_partido(col_header):
    m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", str(col_header).strip())
    if m: return _norm_ws(m.group(1)), _norm_ws(m.group(2))
    return _norm_ws(col_header), ""

def inferir_confianca(erro_conf):
    s = str(erro_conf or "")
    m = re.search(r"(\d{2,3})\s*%\s*\)", s)
    return int(m.group(1)) if m else None

def inferir_margem_erro(erro_conf):
    s = str(erro_conf or "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", s)
    return float(m.group(1)) if m else None

def gerar_poll_id(uf, instituto, id_pesquisa, data_campo, cargo, turno, raw_block_hash):
    uf, data_campo, instituto_slug = uf.upper(), _norm_ws(data_campo), _slug(instituto)
    if id_pesquisa and id_pesquisa.lower() not in ("sem registro", "sem_registro", "semregistro", "nan"):
        return f"{uf}|{cargo}|{turno}|{id_pesquisa}|{data_campo}"
    return f"{uf}|{cargo}|{turno}|{instituto_slug}|{data_campo}|{raw_block_hash}"

def gerar_scenario_id(poll_id, scenario_label):
    return f"{poll_id}|{_norm_ws(scenario_label)}"

# --- SELENIUM CORE ---
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

def expandir_todos(driver, secao, max_clicks=150):
    i = 0
    while True:
        btns = secao.find_elements(By.CSS_SELECTOR, "button.rt-expander-button")
        fechados = [b for b in btns if b.get_attribute("aria-expanded") == "false"]
        if not fechados: break
        driver.execute_script("arguments[0].click();", fechados[0])
        time.sleep(0.8) # Ritmo crucial para o React não quebrar
        i += 1
        if i >= max_clicks: break

def extrair_tabela_react(secao):
    headers = []
    for el in secao.find_elements(By.CSS_SELECTOR, "div.rt-thead .rt-th"):
        inner = el.find_elements(By.CSS_SELECTOR, ".rt-text-content, .rt-sort-header")
        text = inner[0].text.strip() if inner else el.text.strip()
        text = text.replace("\n", " ").strip()
        if text: headers.append(text)

    rows_data = []
    for group in secao.find_elements(By.CSS_SELECTOR, "div.rt-tbody div.rt-tr-group"):
        for row in group.find_elements(By.CSS_SELECTOR, "div.rt-tr"):
            cells = row.find_elements(By.CSS_SELECTOR, "div.rt-td")
            if not cells: continue
            vals = [c.text.strip() for c in cells]
            if any(vals): rows_data.append(vals)

    if not rows_data: return None
    n_cols = max(len(r) for r in rows_data)
    if len(headers) < n_cols:
        headers += [f"Col_{i}" for i in range(len(headers), n_cols)]
    return pd.DataFrame(rows_data, columns=headers[:n_cols])

def scrape_url(driver, url: str):
    meta = parse_url_meta(url)
    if not meta["cargo"]:
        print(f"[-] URL não reconhecida: {url}")
        return None, None

    print(f"[+] {meta['cargo'].upper()} {meta['uf']} {meta['turno']} -> {url}")
    driver.get(url)
    
    # ESPERA CRUCIAL: 10 segundos para carregar as pesquisas mais recentes via React
    time.sleep(10)

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
    
    # Re-localiza para evitar erros após manipulação do DOM
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

    if "Cenários" not in df_raw.columns: df_raw["Cenários"] = ""

    meta_expected = {"Modo Pesquisa", "Entrevistas", "Erro (Confiança)", "Cenários"}
    cols_meta = [c for c in df_raw.columns if c in meta_expected] + ["instituto", "registro_tse", "data_campo", "_block_hash"]
    cols_meta = [c for c in cols_meta if c in df_raw.columns]

    cols_cand = [c for c in df_raw.columns if c not in cols_meta]
    cols_cand = [c for c in cols_cand if re.search(r"\([A-Za-z]{2,}\)", str(c)) or str(c).lower().strip() in ("não válido", "nao valido")]

    pesquisas_rows, resultados_rows = [], []

    for _, row in df_raw.iterrows():
        # Dados da Pesquisa
        poll_id = gerar_poll_id(meta["uf"], row["instituto"], row["registro_tse"], row["data_campo"], meta["cargo"], meta["turno"], row["_block_hash"])
        scenario_label = _norm_ws(row.get("Cenários", "")) or "NA"
        scenario_id = gerar_scenario_id(poll_id, scenario_label)

        pesquisas_rows.append({
            "scenario_id": scenario_id, "poll_id": poll_id, "ano": meta["ano"], "uf": meta["uf"],
            "cargo": meta["cargo"], "turno": meta["turno"], "instituto": row["instituto"],
            "registro_tse": row["registro_tse"], "data_campo": row["data_campo"],
            "modo": _norm_ws(row.get("Modo Pesquisa", "")), "amostra": row.get("Entrevistas", ""),
            "margem_erro": inferir_margem_erro(row.get("Erro (Confiança)", "")),
            "confianca": inferir_confianca(row.get("Erro (Confiança)", "")),
            "scenario_label": scenario_label, "fonte_url": url
        })

        # Resultados por Candidato
        for col in cols_cand:
            pct = parsear_pct(row.get(col, ""))
            if pct is None: continue

            if _norm_ws(col).lower() in ("não válido", "nao valido"):
                candidato, partido, tipo, cand_norm = "Não válido", "", "nao_valido", "nao-valido"
            else:
                candidato, partido = parsear_candidato_partido(col)
                tipo, cand_norm = "candidato", _slug(candidato)

            resultados_rows.append({
                "scenario_id": scenario_id, "poll_id": poll_id, "ano": meta["ano"], "uf": meta["uf"],
                "cargo": meta["cargo"], "turno": meta["turno"], "data_campo": row["data_campo"],
                "instituto": row["instituto"], "scenario_label": scenario_label,
                "candidato": candidato, "candidato_norm": cand_norm, "partido": partido,
                "tipo": tipo, "percentual": pct, "fonte_url": url
            })

    return pd.DataFrame(pesquisas_rows), pd.DataFrame(resultados_rows)

# --- GOOGLE SHEETS INTEGRATION ---
def gs_client_from_env():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw: raise RuntimeError("GOOGLE_CREDENTIALS_JSON não definido.")
    creds_dict = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=scopes))

def garantir_aba(spreadsheet, nome_aba, rows=2000, cols=20):
    try: return spreadsheet.worksheet(nome_aba)
    except gspread.exceptions.WorksheetNotFound: return spreadsheet.add_worksheet(title=nome_aba, rows=rows, cols=cols)

def dedup_e_salvar_por_chave(aba, df_novo: pd.DataFrame, key_col: str):
    df_novo = df_novo.drop_duplicates(subset=[key_col], keep="first").reset_index(drop=True)
    values = aba.get_all_values()

    # Se a aba estiver vazia
    if not values or (len(values) == 1 and not values[0][0]):
        aba.update([df_novo.columns.tolist()] + df_novo.fillna("").astype(str).values.tolist())
        return len(df_novo), 0

    header = values[0]
    if key_col not in header:
        print(f"[AVISO] Coluna {key_col} não encontrada. Reescrevendo aba.")
        aba.clear()
        aba.update([df_novo.columns.tolist()] + df_novo.fillna("").astype(str).values.tolist())
        return len(df_novo), 0

    idx_key = header.index(key_col)
    existing_keys = {row[idx_key] for row in values[1:] if len(row) > idx_key and row[idx_key].strip()}
    
    df_add = df_novo[~df_novo[key_col].astype(str).isin(existing_keys)].reset_index(drop=True)
    if df_add.empty: return 0, len(existing_keys)

    # Alinha colunas e faz Append
    df_add = df_add.reindex(columns=header, fill_value="")
    aba.append_rows(df_add.fillna("").astype(str).values.tolist(), value_input_option="RAW")
    return len(df_add), len(existing_keys)

# --- MAIN ---
def main():
    sheet_id = (os.getenv("SPREADSHEET_ID_POLLING", "") or "").strip()
    if not sheet_id: raise RuntimeError("SPREADSHEET_ID_POLLING não definido.")

    urls = []
    if env_bool("INCLUIR_GOVERNADOR", True):
        urls += [f"https://www.pollingdata.com.br/2026/governador/{uf}/2026_governador_{uf}_t1.html" for uf in UFS]
    if env_bool("INCLUIR_PRESIDENTE", False):
        urls += PRESIDENTE_URLS_DEFAULT

    if not urls: return print("[-] Nenhuma URL selecionada.")

    print("[+] Conectando ao Google Sheets...")
    gc = gs_client_from_env()
    sh = gc.open_by_key(sheet_id)
    aba_pesquisas = garantir_aba(sh, "pesquisas", rows=10000, cols=20)
    aba_resultados = garantir_aba(sh, "resultados", rows=100000, cols=25)

    print("[+] Iniciando Chrome...")
    driver = criar_driver()
    all_p, all_r = [], []

    try:
        for url in urls:
            df_p, df_r = scrape_url(driver, url)
            if df_p is not None: all_p.append(df_p)
            if df_r is not None: all_r.append(df_r)
    finally:
        driver.quit()

    if all_p:
        df_p_all = pd.concat(all_p, ignore_index=True)
        n, e = dedup_e_salvar_por_chave(aba_pesquisas, df_p_all, "scenario_id")
        print(f"[+] Pesquisas: {n} novas | {e} já existiam")

    if all_r:
        df_r_all = pd.concat(all_r, ignore_index=True)
        df_r_all["_dedup_key"] = df_r_all["scenario_id"].astype(str) + "|" + df_r_all["tipo"] + "|" + df_r_all["candidato_norm"]
        n, e = dedup_e_salvar_por_chave(aba_resultados, df_r_all, "_dedup_key")
        print(f"[+] Resultados: {n} novas | {e} já existiam")

    print("[+] Concluído com sucesso.")

if __name__ == "__main__":
    main()
