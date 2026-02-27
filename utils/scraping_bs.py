
# -*- coding: utf-8 -*-
from __future__ import annotations
import time, random, re
from typing import Optional, Tuple, List, Dict
import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin

from .db import insert_raw_many


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
    'Chiens': {'nom': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
               'addr':  '.hide-on-med-and-down [data-address] span', 'img': 'div.col:nth-of-type(1) img.ad__card-img'},
    'Moutons': {'nom': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
               'addr':  '.hide-on-med-and-down [data-address] span', 'img': 'div.col:nth-of-type(1) img.ad__card-img'},
    'Poules-Lapins-Pigeons': {'nom': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
               'addr':  '.hide-on-med-and-down [data-address] span', 'img': 'div.col:nth-of-type(1) img.ad__card-img'},
    'Autres animaux': {'nom': '.hide-on-med-and-down h1', 'price': '.hide-on-med-and-down p.price',
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
# Utils
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
    """Session requests optimisée (pool HTTP) + cookies Selenium."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    s.headers.update(HEADERS)
    s.verify = verify

    # Pool + retries légers (évite surcoûts des connexions)
    retry = Retry(
        total=2, backoff_factor=0.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET', 'HEAD'])
    )
    adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)

    for c in cookies:
        domain = c.get('domain') or 'sn.coinafrique.com'
        s.cookies.set(c['name'], c['value'], domain=domain)
    return s

def _parse_detail_html(html: str, category: str) -> Dict[str, Optional[str]]:
    
    soup = bs(html, 'lxml')
    sel = DETAIL.get(category, {})

    def txt(css: str) -> Optional[str]:
        el = soup.select_one(css) if css else None
        return el.get_text(strip=True) if el else None

    nom = txt(sel.get('title'))
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

    # fallback regex prix
    if not price_raw:
        m = PRICE.search(html or '')
        price_raw = m.group(1) if m else None

    return {
        'nom': nom,
        'price_raw': price_raw,
        'address_raw': address_raw,
        'image_url': image_url,
    }

# -----------------------------------------------------------------------------
# 💡 SCRAPER HYBRIDE ACCÉLÉRÉ
#   - list_only=True  : extraction directe (Nom/Prix/Adresse/Image/Lien) côté LISTE via JS (ultra-rapide)
#   - list_only=False : visite DÉTAIL concurrente avec requests+BS4 (max_workers threads)
# -----------------------------------------------------------------------------
def bs4_scrape_insert(
    category: str,
    start_page: int,
    end_page: int,
    sleep: Tuple[float, float] = (0.02, 0.06),  #  micro-sommeil par défaut
    visit_detail: bool = True,
    list_only: bool = True,       #  ACTIVE ce mode pour la vitesse maximale
    max_workers: int = 12,        #  nb. threads pour les détails (si list_only=False)
    headless: bool = True,
    verify_ssl: bool = True,      # True = vérif SSL requests (bonnes pratiques)
    debug_dump: bool = False,
) -> int:
   
    assert category in CATEGORIES, f"Catégorie inconnue: {category}"
    total_inserted = 0

    # -- Selenium pour charger la LISTE une seule fois par page
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager

    WAIT_SEC = 6
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1600,1200')
    options.add_argument('--disable-extensions')
    # Bloquer images côté liste
    try:
        options.add_argument('--blink-settings=imagesEnabled=false')
    except Exception:
        pass

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    def wait_list_ready():
        WebDriverWait(driver, WAIT_SEC).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.ad__card-description a[href]'))
        )

    try:
        for p in range(start_page, end_page + 1):
            # 1) Ouvrir la page LISTE (2 schémas pagination)
            links: List[str] = []
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
                continue

            # 2A) MODE ULTRA-RAPIDE : extraction LISTE par JavaScript (une seule passe)
            if list_only:
                # JS rapide qui lit toute la carte d’un coup (moins d’allers/retours Selenium)
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
                items = driver.execute_script(js) or []

                # Normaliser + filtrer images
                rows_to_db: List[Dict] = []
                for it in items:
                    img = it.get('img')
                    if img:
                        if img.startswith('//'):
                            img = 'https:' + img
                        elif img.startswith('/'):
                            img = urljoin(SITE_BASE, img)
                        if any(t in img for t in BAD_IMG_TOKENS):
                            img = None

                    rows_to_db.append({
                        'source': 'coinafrique-sn',
                        'category': category,
                        'nom': it.get('name') or None,
                        'price_raw': it.get('price') or None,
                        'address_raw': it.get('addr') or None,
                        'image_url': img,
                        'link': it.get('link') or None,
                        'page': p,
                    })

                inserted = insert_raw_many(rows_to_db)
                total_inserted += inserted
                # micro-pause
                time.sleep(random.uniform(*sleep))
                continue  # page suivante

            # 2B) MODE DÉTAIL CONCURRENT : on récupère les liens puis on parallélise requests+BS4
            anchors = driver.find_elements(By.CSS_SELECTOR, '.ad__card-description a[href]')
            raw_links = []
            for a in anchors:
                href = a.get_attribute('href') or ''
                if '/annonce/' in href:
                    raw_links.append(href)

            # dédup
            links = list(dict.fromkeys(raw_links))
            if not links:
                continue

            # Session requests avec cookies Selenium
            cookies = driver.get_cookies()
            rs = _requests_session_from_selenium_cookies(cookies, pool_connections=32, pool_maxsize=64, verify=verify_ssl)

            # DÉTAILS en parallèle
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def fetch_detail(href: str) -> Dict[str, Optional[str]]:
                # session par thread (évite les effets de bord)
                s = requests.Session()
                s.headers.update(HEADERS)
                s.verify = verify_ssl
                # copier les cookies
                for c in rs.cookies:
                    s.cookies.set(c.name, c.value, domain=c.domain or 'sn.coinafrique.com')
                try:
                    r = s.get(href, timeout=12)
                    r.raise_for_status()
                    det = _parse_detail_html(r.text, category)
                    return {
                        'nom': det.get('title'),
                        'price_raw': det.get('price_raw'),
                        'address_raw': det.get('address_raw'),
                        'image_url': det.get('image_url'),
                        'link': href
                    }
                except Exception:
                    return {'nom': None, 'price_raw': None, 'address_raw': None, 'image_url': None, 'link': href}

            rows_to_db: List[Dict] = []
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(fetch_detail, href) for href in links]
                for fu in as_completed(futures):
                    det = fu.result() or {}
                    rows_to_db.append({
                        'source': 'coinafrique-sn',
                        'category': category,
                        'nom': det.get('title'),
                        'price_raw': det.get('price_raw'),
                        'address_raw': det.get('address_raw'),
                        'image_url': det.get('image_url'),
                        'link': det.get('link'),
                        'page': p,
                    })

            inserted = insert_raw_many(rows_to_db)
            total_inserted += inserted
            time.sleep(random.uniform(*sleep))

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return total_inserted

# Compatibilité : si l'app appelle encore l'ancien nom
selenium_scrape_insert = bs4_scrape_insert
