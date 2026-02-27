
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, shutil
import time, random, re
from typing import Optional, Tuple, List, Dict
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs, FeatureNotFound

# -----------------------------------------------------------------------------
# Constantes et sélecteurs
# -----------------------------------------------------------------------------
SITE_BASE = 'https://sn.coinafrique.com'
CATEGORIES = {
    'Chiens': '/categorie/chiens',
    'Moutons': '/categorie/moutons',
    'Poules-Lapins-Pigeons': '/categorie/poules-lapins-et-pigeons',
    'Autres animaux': '/categorie/autres-animaux',
}
PAGE_PATTERNS = ['{base}{path}?page={n}', '{base}{path}/{n}']

PRICE = re.compile(r'(\d[\d\s\.,]*)', re.I)
BAD_IMG_TOKENS = ['/static/images/countries/', '/static/flags/', '/svg', 'data:image']

DETAIL = {
    'Chiens': {'title': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
               'addr':  '.hide-on-med-and-down [data-address] span', 'img': 'div.col:nth-of-type(1) img.ad__card-img'},
    'Moutons': {'title': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
               'addr':  '.hide-on-med-and-down [data-address] span', 'img': 'div.col:nth-of-type(1) img.ad__card-img'},
    'Poules-Lapins-Pigeons': {'title': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
               'addr':  '.hide-on-med-and-down [data-address] span', 'img': 'div.col:nth-of-type(1) img.ad__card-img'},
    'Autres animaux': {'title': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
               'addr':  '.hide-on-med-and-down [data-address] span', 'img': 'div.col:nth-of-type(2) img.ad__card-img'},
}

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/122.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Referer': 'https://sn.coinafrique.com/',
}

# -----------------------------------------------------------------------------
# Anti-caches wdm / selenium manager (utile en Cloud après anciennes builds)
# -----------------------------------------------------------------------------
for p in [os.path.expanduser("~/.wdm"), os.path.expanduser("~/.cache/selenium")]:
    try:
        if os.path.exists(p):
            shutil.rmtree(p, ignore_errors=True)
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Selenium: Chrome/Chromium via binaires système (sans webdriver-manager)
# -----------------------------------------------------------------------------
def create_driver(headless: bool = True):
    """
    Streamlit Cloud (Debian bookworm) :
      - packages.txt doit installer: chromium, chromium-driver
      - on force l'usage des binaires système, pas de download externe
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService

    import shutil as _sh

    chrome_bin = (
        os.environ.get("CHROME_PATH")
        or _sh.which("chromium")
        or _sh.which("chromium-browser")
        or _sh.which("google-chrome")
        or "/usr/bin/chromium"
    )
    chromedriver = _sh.which("chromedriver") or "/usr/bin/chromedriver"

    if not (chromedriver and os.path.exists(chromedriver)):
        raise RuntimeError("chromedriver système introuvable. Vérifie packages.txt (chromium + chromium-driver).")

    options = ChromeOptions()
    if headless:
        try:
            options.add_argument("--headless=new")
        except Exception:
            options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--blink-settings=imagesEnabled=false")  # bloque les images pour accélérer
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )

    if chrome_bin and os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    service = ChromeService(executable_path=chromedriver)
    return webdriver.Chrome(service=service, options=options)

# -----------------------------------------------------------------------------
# Utils Requests/Parsing
# -----------------------------------------------------------------------------
def _norm_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    if u.startswith('//'):
        return 'https:' + u
    if u.startswith('/'):
        return urljoin(SITE_BASE, u)
    return u

def _requests_session_from_selenium_cookies(
    cookies: List[dict],
    pool_connections: int = 20,
    pool_maxsize: int = 50,
    verify: bool = True
) -> requests.Session:
    """Session requests optimisée (pool HTTP) + cookies Selenium (compat urllib3 v1/v2)."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    s.headers.update(HEADERS)
    s.verify = verify

    retry_kwargs = dict(
        total=2, backoff_factor=0.2,
        status_forcelist=(429, 500, 502, 503, 504),
    )
    try:
        retry = Retry(allowed_methods=frozenset(['GET', 'HEAD']), **retry_kwargs)
    except TypeError:
        retry = Retry(method_whitelist=frozenset(['GET', 'HEAD']), **retry_kwargs)

    adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    for c in cookies:
        domain = c.get('domain') or 'sn.coinafrique.com'
        s.cookies.set(c['name'], c['value'], domain=domain)
    return s

def _parse_detail_html(html: str, category: str) -> Dict[str, Optional[str]]:
    """Extraction depuis la page DÉTAIL (fallback parser si lxml absent)."""
    try:
        soup = bs(html, 'lxml')
    except FeatureNotFound:
        soup = bs(html, 'html.parser')

    sel = DETAIL.get(category, {})

    def txt(css: Optional[str]) -> Optional[str]:
        if not css:
            return None
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else None

    title = txt(sel.get('title'))
    price_raw = txt(sel.get('price'))
    address_raw = txt(sel.get('addr'))

    # image
    image_url = None
    img_sel = sel.get('img')
    if img_sel:
        img = soup.select_one(img_sel)
        if img:
            for attr in ('data-src', 'data-lazy', 'data-original', 'srcset', 'src'):
                v = img.get(attr)
                if not v:
                    continue
                if attr == 'srcset' and ' ' in v:
                    v = v.split(' ')[0]
                image_url = v
                break

    image_url = _norm_url(image_url)
    if image_url and any(tok in image_url for tok in BAD_IMG_TOKENS):
        image_url = None

    if not price_raw:
        m = PRICE.search(html or '')
        price_raw = m.group(1) if m else None

    return {
        'title': title,
        'price_raw': price_raw,
        'address_raw': address_raw,
        'image_url': image_url,
    }

# -----------------------------------------------------------------------------
# SQLite (enregistrement avec index unique sur link)
# -----------------------------------------------------------------------------
def ensure_table_sqlite(conn, table: str = "annonces"):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            category TEXT,
            title TEXT,
            price_raw TEXT,
            address_raw TEXT,
            image_url TEXT,
            link TEXT,
            page INTEGER
        );
    """)
    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_link_unique ON {table}(link);")
    conn.commit()

def save_df_to_sqlite(df: pd.DataFrame, db_path: str = "coinafrique.db", table: str = "annonces") -> tuple[int, int]:
    if df is None or df.empty:
        return (0, 0)
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        ensure_table_sqlite(conn, table)
        before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        cols = ['source','category','title','price_raw','address_raw','image_url','link','page']
        rows = []
        for _, r in df[cols].fillna("").iterrows():
            rows.append((
                r['source'], r['category'], r['title'], r['price_raw'],
                r['address_raw'], r['image_url'], r['link'],
                int(r['page']) if str(r['page']).isdigit() else 0
            ))
        conn.executemany(
            f"INSERT OR IGNORE INTO {table} "
            f"(source, category, title, price_raw, address_raw, image_url, link, page) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?);", rows
        )
        conn.commit()
        after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return (max(after - before, 0), len(df))
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# Scraper hybride (LISTE rapide / DÉTAIL parallèle)
# -----------------------------------------------------------------------------
def scrape_category_to_df(
    category: str,
    start_page: int,
    end_page:   int,
    list_only:  bool = True,
    visit_detail: bool = True,          # pris en compte si list_only=False
    max_workers: int = 12,              # threads détails
    sleep: Tuple[float, float] = (0.12, 0.35),
    headless: bool = True,
    verify_ssl: bool = True,
) -> pd.DataFrame:
    """
    Charge chaque page LISTE avec Selenium et:
      - list_only=True  : extrait Nom/Prix/Adresse/Image/Lien via JS (ultra-rapide)
      - list_only=False : récupère les LIENS puis visite les DÉTAILS (requests+BS4 en parallèle)

    Retourne un DataFrame colonnes: source, category, title, price_raw, address_raw, image_url, link, page
    """
    assert category in CATEGORIES, f"Catégorie inconnue: {category}"

    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    WAIT_SEC = 8
    driver = create_driver(headless=headless)

    def wait_list_ready():
        WebDriverWait(driver, WAIT_SEC).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.ad__card-description a[href]'))
        )

    all_rows: List[Dict] = []
    try:
        for p in range(start_page, end_page + 1):
            page_ok = False
            # essaie les 2 patterns de pagination
            for url in (
                PAGE_PATTERNS[0].format(base=SITE_BASE, path=CATEGORIES[category], n=p),
                PAGE_PATTERNS[1].format(base=SITE_BASE, path=CATEGORIES[category], n=p),
            ):
                try:
                    driver.get(url)
                    wait_list_ready()
                    page_ok = True
                    break
                except Exception:
                    continue
            # fallback: sans pagination (utile pour p=1 si le site change)
            if not page_ok and p == 1:
                try:
                    driver.get(urljoin(SITE_BASE, CATEGORIES[category]))
                    wait_list_ready()
                    page_ok = True
                except Exception:
                    pass
            if not page_ok:
                time.sleep(random.uniform(*sleep))
                continue

            # ---- Mode LISTE ultra-rapide (JS) ----
            if list_only:
                js = """
                const cards = Array.from(document.querySelectorAll('div.col.s6.m4.l3'));
                function pickImg(el){
                  const img = el.querySelector('img.ad__card-img') || el.querySelector('a.card-image img');
                  if(!img) {
                    const a = el.querySelector('a.card-image');
                    if(a && a.style && a.style.backgroundImage){
                      const m = a.style.backgroundImage.match(/url\\(['"]?(.*?)['"]?\\)/);
                      return m ? m[1] : null;
                    }
                    return null;
                  }
                  return img.getAttribute('data-src') || img.getAttribute('data-lazy') ||
                         img.getAttribute('data-original') || (img.getAttribute('srcset')||'').split(' ')[0] ||
                         img.getAttribute('src');
                }
                return cards.map(c => {
                  const name  = (c.querySelector('p.ad__card-description')?.innerText||'').trim();
                  const price = (c.querySelector('p.ad__card-price')?.innerText||'').trim();
                  const addr  = (c.querySelector('p.ad__card-location span')?.innerText||'').trim();
                  const a     =  c.querySelector('.ad__card-description a[href], a.card-image[href]');
                  const link  = a ? a.href : null;
                  let   img   = pickImg(c);
                  return {name, price, addr, link, img};
                });
                """
                try:
                    items = driver.execute_script(js) or []
                except Exception:
                    items = []

                for it in items:
                    img = _norm_url(it.get('img') or None)
                    if img and any(t in img for t in BAD_IMG_TOKENS):
                        img = None
                    all_rows.append({
                        'source': 'coinafrique-sn',
                        'category': category,
                        'title': it.get('name') or None,
                        'price_raw': it.get('price') or None,
                        'address_raw': it.get('addr') or None,
                        'image_url': img,
                        'link': it.get('link') or None,
                        'page': p,
                    })
                time.sleep(random.uniform(*sleep))
                continue  # page suivante

            # ---- Mode DÉTAIL (requests + BS4) ----
            anchors = driver.find_elements(By.CSS_SELECTOR, '.ad__card-description a[href]')
            raw_links = []
            for a in anchors:
                href = a.get_attribute('href') or ''
                if '/annonce/' in href:
                    raw_links.append(href)
            links = list(dict.fromkeys(raw_links))
            if not links:
                time.sleep(random.uniform(*sleep))
                continue

            # Session requests avec cookies Selenium
            cookies = driver.get_cookies()
            rs = _requests_session_from_selenium_cookies(
                cookies, pool_connections=32, pool_maxsize=64, verify=verify_ssl
            )

            from concurrent.futures import ThreadPoolExecutor, as_completed

            def fetch_detail(href: str) -> Dict[str, Optional[str]]:
                s = requests.Session()
                s.headers.update(HEADERS)
                s.verify = verify_ssl
                for c in rs.cookies:
                    name = getattr(c, 'name', None) or getattr(c, 'key', None)
                    value = getattr(c, 'value', None)
                    domain = getattr(c, 'domain', None) or 'sn.coinafrique.com'
                    if name and value:
                        s.cookies.set(name, value, domain=domain)
                try:
                    r = s.get(href, timeout=12)
                    r.raise_for_status()
                    det = _parse_detail_html(r.text, category)
                    return {
                        'title': det.get('title'),
                        'price_raw': det.get('price_raw'),
                        'address_raw': det.get('address_raw'),
                        'image_url': det.get('image_url'),
                        'link': href
                    }
                except Exception:
                    return {'title': None, 'price_raw': None, 'address_raw': None, 'image_url': None, 'link': href}

            rows_detail: List[Dict] = []
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(fetch_detail, href) for href in links]
                for fu in as_completed(futures):
                    det = fu.result() or {}
                    rows_detail.append({
                        'source': 'coinafrique-sn',
                        'category': category,
                        'title': det.get('title'),
                        'price_raw': det.get('price_raw'),
                        'address_raw': det.get('address_raw'),
                        'image_url': det.get('image_url'),
                        'link': det.get('link'),
                        'page': p,
                    })
            all_rows.extend(rows_detail)
            time.sleep(random.uniform(*sleep))

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # DataFrame + dédup sur link
    df = pd.DataFrame(all_rows)
    if not df.empty and 'link' in df.columns:
        df = df.drop_duplicates(subset=['link']).reset_index(drop=True)
    return df

# -----------------------------------------------------------------------------
# Exemple d’usage direct (à commenter si utilisé comme module)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Test rapide : 2 pages Moutons, mode LISTE (rapide), headless
    cat = "Moutons"
    df = scrape_category_to_df(cat, start_page=1, end_page=2, list_only=True, headless=True)
    print(df.head(10))
    print(f"Total lignes: {len(df)}")
    if not df.empty:
        inserted, total = save_df_to_sqlite(df, db_path="coinafrique.db", table="annonces")
        print(f"SQLite -> insérées: {inserted}/{total}")
