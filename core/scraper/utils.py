
import re
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from core.scraper.config import ARCHIVE_OUT, LEGACY_OUT, PROFILE_DIR, WAIT_TIME, PAGE_TIMEOUT, VERSION_MAIN, TODAY

def clean_numeric(text):
    try:
        return float(re.sub(r"[^0-9.\-]", "", text))
    except:
        return None

def create_browser():
    opts = uc.ChromeOptions()
    opts.page_load_strategy = 'eager'
    opts.add_argument(f"--user-data-dir={PROFILE_DIR}")
    opts.add_argument("--window-size=1200,800")
    return uc.Chrome(options=opts, version_main=VERSION_MAIN)

def scrape_ivhv(ticker, driver):
    url = f"https://researchtools.fidelity.com/ftgw/mloptions/goto/ivIndex?symbol={ticker}"
    try:
        driver.get(url)
        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.volatility-index-data-table table"))
        )
    except Exception:
        return {"Ticker": ticker, "timestamp": datetime.now().isoformat(), "Date": TODAY, "Error": "Page Load Fail"}

    tables = driver.find_elements(By.CSS_SELECTOR, "div.volatility-index-data-table table")
    soup_iv = BeautifulSoup(tables[0].get_attribute("outerHTML"), "lxml") if len(tables) >= 1 else None
    soup_hv = BeautifulSoup(tables[1].get_attribute("outerHTML"), "lxml") if len(tables) >= 2 else None

    data = {"Ticker": ticker, "timestamp": datetime.now().isoformat(), "Date": TODAY, "Error": None}
    terms = ["7_D","14_D","21_D","30_D","60_D","90_D","120_D","150_D","180_D","270_D","360_D","720_D","1080_D"]

    if soup_iv:
        for term, tr in zip(terms, soup_iv.find_all("tr")[2:]):
            tds = tr.find_all("td")
            if len(tds) >= 8:
                data[f"IV_{term}_Call"] = clean_numeric(tds[0].text)
                data[f"IV_{term}_Put"] = clean_numeric(tds[2].text)

    if soup_hv:
        for tr in soup_hv.find_all("tr")[1:]:
            th = tr.find("th")
            td = tr.find_all("td")
            if th and td:
                label = th.text.strip().replace(" ", "_").replace("Days", "D")
                data[f"HV_{label}_Cur"] = clean_numeric(td[0].text)

    # Add error flag if no meaningful data was extracted
    if not any(k.startswith("IV_") or k.startswith("HV_") for k in data.keys()):
        data["Error"] = "No Data Extracted"

    return data

def save_result(result):
    if "Error" not in result:
        result["Error"] = None  # ensure consistency

    df = pd.DataFrame([result])
    for path in filter(None, [ARCHIVE_OUT, LEGACY_OUT]):  # skip None safely
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                existing = pd.read_csv(path)
                existing = existing[existing["Ticker"] != result["Ticker"]]  # drop old entry for this ticker
                combined = pd.concat([existing, df], ignore_index=True)
            else:
                combined = df
            combined.drop_duplicates(subset=["Ticker", "Date"], keep="last").to_csv(path, index=False)
        except Exception as e:
            print(f"[❌] Failed to save to {path}: {e}")

def load_tickers(file_override=None):
    from core.scraper.config import DEFAULT_TICKER_CSV
    path = Path(file_override) if file_override else DEFAULT_TICKER_CSV
    if not path.exists():
        raise FileNotFoundError(f"Ticker CSV not found: {path}")
    return pd.read_csv(path, usecols=["Ticker"]).dropna()["Ticker"].astype(str).tolist()
