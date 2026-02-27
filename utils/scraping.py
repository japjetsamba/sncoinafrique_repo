
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, shutil
import time, random, re
from typing import Optional, Tuple, List, Dict

import requests
from bs4 import BeautifulSoup as bs, FeatureNotFound
from urllib.parse import urljoin

from .db import insert_raw_many

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

# -----------------------------------------------------------------------------
# Headers HTTP pour requests
# -----------------------------------------------------------------------------
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
# PURGE des caches wdm/selenium (utile en Cloud)
# -----------------------------------------------------------------------------
for p in [
    os.path.expanduser("~/.wdm"),             # cache webdriver-manager
    os.path.expanduser("~/.cache/selenium"),  # cache Selenium Manager
]:
    try:
        if os.path.exists(p):
            shutil.rmtree(p, ignore_errors=True)
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Utils Selenium : drivers système (Chrome/Firefox)
# -----------------------------------------------------------------------------
def _find_chrome_binary() -> Optional[str]:
    candidates = [
        os.environ.get("CHROME_PATH"),
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None

def _find_chromedriver() -> Optional[str]:
    candidates = [
        os.environ.get("CHROMEDRIVER_PATH"),
        shutil.which("chromedriver"),
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None

def _find_geckodriver() -> Optional[str]:
    candidates = [
        os.environ.get("GECKODRIVER_PATH"),
        shutil.which("geckodriver"),
        "/usr/bin/geckodriver",
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None

def create_driver(headless: bool = True):
    """
    Essaie Chrome (chromium + chromedriver système), sinon fallback Firefox (geckodriver).
    Aucun webdriver-manager.
    """
    from selenium import webdriver

    # --- Tentative Chrome/Chromium ---
    try:
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService

        ch_bin = _find_chrome_binary()
        ch_drv = _find_chromedriver()
        if ch_drv:
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
            options.add_argument("--blink-settings=imagesEnabled=false")
            options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            )
            if ch_bin:
                options.binary_location = ch_bin
            service = ChromeService(executable_path=ch_drv)
            return webdriver.Chrome(service=service, options=options)
    except Exception:
        pass

    # --- Fallback Firefox ---
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.firefox.service import Service as FirefoxService

    gecko = _find_geckodriver()
    if not gecko:
        raise RuntimeError(
            "Aucun driver système détecté. "
            "Installe soit chromium+chromedriver, soit firefox-esr+geckodriver."
        )
    options = FirefoxOptions()
    options.headless = headless
    options.set_preference("general.useragent.override",
                           "Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0")
    service = FirefoxService(executable_path=gecko)
    return webdriver.Firefox(service=service, options=options)

# -----------------------------------------------------------------------------
# Utils Requests
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

    # Compat urllib3 v1/v2 pour allowed_methods/method_whitelist
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
    """Extraction depuis la page DÉTAIL (fallback parser)"""
    try:
        soup = bs(html, 'lxml')
    except FeatureNotFound:
        soup = bs(html, 'html.parser')

    sel = DETAIL.get(category, {})

    def txt(css: str) -> Optional[str]:
        el = soup.select_one(css) if css else None
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
# 💡 SCRAPER HYBRIDE ACCÉLÉRÉ
#   - list_only=True  : extraction LISTE (Nom/Prix/Adresse/Image/Lien) en JS (ultra-rapide)
#   - list_only=False : visite DÉTAIL concurrente via requests+BS4 (max_workers threads)
# -----------------------------------------------------------------------------
def bs4_scrape_insert(
    category: str,
    start_page: int,
    end_page: int,
    sleep: Tuple[float, float] = (0.12, 0.35),  # micro-pause (un peu augmentée)
    visit_detail: bool = True,
    list_only: bool = True,       # vitesse max par défaut
    max_workers: int = 12,
    headless: bool = True,
    verify_ssl: bool = True,
    debug_dump: bool = False,
) -> int:
    """
    1) Selenium charge la PAGE LISTE.
       - list_only=True  : extrait Nom/Prix/Adresse/Image/Lien → insertion directe (pas de détail).
       - list_only=False : récupère LIENS + COOKIES → détails en parallèle via requests+BS4.
    2) Insertion DB : mêmes colonnes que la version Selenium (insert_raw_many).
    """
    assert category in CATEGORIES, f"Catégorie inconnue: {category}"
    total_inserted = 0

    # -- Selenium pour charger la LISTE une fois par page
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    WAIT_SEC = 8
    driver = create_driver(headless=headless)

    def wait_list_ready():
        WebDriverWait(driver, WAIT_SEC).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.ad__card-description a[href]'))
        )

    try:
        for p in range(start_page, end_page + 1):
            # 1) Ouvrir la page LISTE (2 schémas de pagination)
            page_ok = False
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
            if not page_ok:
                # tente sans pagination (utile pour p=1 si le site a changé)
                try:
                    driver.get(urljoin(SITE_BASE, CATEGORIES[category]))
                    wait_list_ready()
                    page_ok = True
                except Exception:
                    pass
            if not page_ok:
                # page inaccessible, on passe à la suivante
                time.sleep(random.uniform(*sleep))
                continue

            # 2A) MODE ULTRA-RAPIDE : extraction LISTE par JavaScript (une seule passe)
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

                rows_to_db: List[Dict] = []
                for it in items:
                    img = _norm_url(it.get('img') or None)
                    if img and any(t in img for t in BAD_IMG_TOKENS):
                        img = None
                    rows_to_db.append({
                        'source': 'coinafrique-sn',
                        'category': category,
                        'title': it.get('name') or None,
                        'price_raw': it.get('price') or None,
                        'address_raw': it.get('addr') or None,
                        'image_url': img,
                        'link': it.get('link') or None,
                        'page': p,
                    })
                if rows_to_db:
                    inserted = insert_raw_many(rows_to_db)
                    total_inserted += inserted

                time.sleep(random.uniform(*sleep))
                continue  # page suivante

            # 2B) MODE DÉTAIL CONCURRENT : on récupère les liens puis on parallélise requests+BS4
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
            rs = _requests_session_from_selenium_cookies(cookies, pool_connections=32, pool_maxsize=64, verify=verify_ssl)

            # DÉTAILS en parallèle
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def fetch_detail(href: str) -> Dict[str, Optional[str]]:
                s = requests.Session()
                s.headers.update(HEADERS)
                s.verify = verify_ssl
                for c in rs.cookies:
                    # c peut être un Cookie morsel ou objet Cookie, on sécurise l'accès
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

            rows_to_db: List[Dict] = []
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(fetch_detail, href) for href in links]
                for fu in as_completed(futures):
                    det = fu.result() or {}
                    rows_to_db.append({
                        'source': 'coinafrique-sn',
                        'category': category,
                        'title': det.get('title'),
                        'price_raw': det.get('price_raw'),
                        'address_raw': det.get('address_raw'),
                        'image_url': det.get('image_url'),
                        'link': det.get('link'),
                        'page': p,
                    })

            if rows_to_db:
                inserted = insert_raw_many(rows_to_db)
                total_inserted += inserted
            time.sleep(random.uniform(*sleep))

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return total_inserted

# Compat : alias
selenium_scrape_insert = bs4_scrape_insert
