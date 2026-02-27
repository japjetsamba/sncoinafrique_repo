
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import utils.scraping as scraping
import utils.db as dbutils
import utils.cleaning as cleaning
import utils.charts as charts

st.set_page_config(page_title="Animals Data Collection – CoinAfrique SN", page_icon="🐾", layout="wide")

# CSS orange
css_path = Path('assets/theme.css')
if css_path.exists():
    st.markdown('<style>' + css_path.read_text(encoding='utf-8') + '</style>', unsafe_allow_html=True)

SCRAPE_URLS = {
    'Chiens': 'https://sn.coinafrique.com/categorie/chiens',
    'Moutons': 'https://sn.coinafrique.com/categorie/moutons',
    'Poules-Lapins-Pigeons': 'https://sn.coinafrique.com/categorie/poules-lapins-pigeons',
    'Autres animaux': 'https://sn.coinafrique.com/categorie/autres-animaux',
}
DEFAULT_PAGES = 2

DATA_DIR = Path('data')
WS_DIR = DATA_DIR / 'webscraper_csv'
CLEAN_DIR = DATA_DIR / 'cleaned'
RAW_DIR = DATA_DIR / 'raw'
for p in (WS_DIR, CLEAN_DIR, RAW_DIR): p.mkdir(parents=True, exist_ok=True)

st.sidebar.image('assets/logo.png', width=120)
st.sidebar.title('Menu')
menu = st.sidebar.selectbox('Choisir une page',
    ('Accueil','Scraper','Web Scraper (CSV brut)','Dashboard (nettoyé)','Feedback'), index=0
)

def sync_cleaned_from_ws():
    WS_EXPECTED = {'chiens': WS_DIR / 'chiens.csv','moutons': WS_DIR / 'moutons.csv','poules_lapins_pigeons': WS_DIR / 'poules_lapins_pigeons.csv','autres_animaux': WS_DIR / 'autres_animaux.csv'}
    CLEAN_TARGETS = {'chiens': CLEAN_DIR / 'chiens_clean.csv','moutons': CLEAN_DIR / 'moutons_clean.csv','poules_lapins_pigeons': CLEAN_DIR / 'poules_lapins_pigeons_clean.csv','autres_animaux': CLEAN_DIR / 'autres_animaux_clean.csv'}
    results = {}
    for key, ws_path in WS_EXPECTED.items():
        clean_path = CLEAN_TARGETS[key]
        results[key] = {'ws': ws_path, 'clean': clean_path, 'status': 'skipped'}
        if not ws_path.exists():
            results[key]['status'] = 'missing_raw'; continue
        need_refresh = (not clean_path.exists()) or (ws_path.stat().st_mtime > clean_path.stat().st_mtime)
        if need_refresh:
            try:
                df_raw = pd.read_csv(ws_path)
                if df_raw.empty: results[key]['status'] = 'raw_empty'; continue
                df_clean = cleaning.basic_cleaning(df_raw.copy(), dropna_thresh=0.7, drop_duplicates=True)
                df_clean.to_csv(clean_path, index=False, encoding='utf-8')
                results[key]['status'] = 'cleaned'
            except Exception as e:
                results[key]['status'] = f'error: {e}'
        else:
            results[key]['status'] = 'up_to_date'
    return results

def show_home():
    st.header("BIENVENUE DANS NOTRE APPLICATION")
    st.write(
        "Cette application scrape des annonces CoinAfrique SN, enregistre en base SQL, "
        "permet d'afficher les CSV bruts et propose un dashboard après nettoyage."
    )
    st.markdown("""
    **Fonctionnalités :**
    - **Scraping** sur plusieurs pages 
    - **Affichage** des données brutes collectées via *Web Scraper* (CSV)
    - **Dashboard** basé sur les **données nettoyées**
    - **Feedback** via formulaires **KoBo** et **Google Forms**
    """)

def show_scraper():
    st.header('SCRAPER ET ENREGISTREMENT DIRECT EN BASE')
    st.write(
        "Veuillez choisir une catégorie dans la liste, ensuite cliquez sur Lancer le scraping afin qu'il scrape et enregistre dans la base dedonnées SQL. "
        "Si vous avez déjà eu à scraper, vous pouvez sélectionner la catégorie et afficher les données existantes."
    )
    c1, c2 = st.columns(2)
    with c1: category = st.selectbox('Catégorie', list(SCRAPE_URLS.keys()), index=0)
    with c2: pages = st.slider('Pages', 1, 10, DEFAULT_PAGES)

    if st.button('Lancer le scraping et enregistrer en DB', type='primary'):
        with st.spinner('Scraping + insert/update (BeautifulSoup)…'):
            try:
                stats = scraping.bs4_scrape_insert(category, 1, int(pages), visit_detail=False)
                # 🔒 Normalisation de secours (au cas où un ancien module renverrait un int)
                if not isinstance(stats, dict):
                    stats = {'inserted': int(stats or 0), 'updated': 0, 'errors': 0}
                st.success(f"Terminé — {stats.get('inserted',0)} insérées, "
                           f"{stats.get('updated',0)} mises à jour, "
                           f"{stats.get('errors',0)} erreurs.")
            except Exception as e:
                st.error(f'Erreur : {e}')

    if st.button('Afficher les données en DB'):
        df_db = dbutils.fetch_all_raw()
        if df_db is None or df_db.empty:
            st.info('La base est vide.')
        else:
            df_cat = df_db[df_db['category'] == category].copy() if 'category' in df_db.columns else df_db.copy()
            if 'title' in df_cat.columns:
                df_cat = df_cat.rename(columns={'title': 'details' if category == 'Autres animaux' else 'Nom'})
            st.dataframe(df_cat.head(300), use_container_width=True)

def show_ws_csv():
    st.header('WEB SCRAPER')
    st.caption('Cliquez sur une catégorie pour afficher les CSV bruts (collectés avec l’extension Web Scraper).')
    FILE_MAP = {'Chiens':'chiens.csv','Moutons':'moutons.csv','Poules-Lapins-Pigeons':'poules_lapins_pigeons.csv','Autres animaux':'autres_animaux.csv'}
    if 'ws_choice_file' not in st.session_state:
        st.session_state.ws_choice_file = None
    c1,c2,c3,c4 = st.columns(4)
    with c1:
        if st.button('Chiens', use_container_width=True): st.session_state.ws_choice_file = FILE_MAP['Chiens']
    with c2:
        if st.button('Moutons', use_container_width=True): st.session_state.ws_choice_file = FILE_MAP['Moutons']
    with c3:
        if st.button('Poules-Lapins-Pigeons', use_container_width=True): st.session_state.ws_choice_file = FILE_MAP['Poules-Lapins-Pigeons']
    with c4:
        if st.button('Autres animaux', use_container_width=True): st.session_state.ws_choice_file = FILE_MAP['Autres animaux']
    if not st.session_state.ws_choice_file:
        st.info("En attente d'une sélection…")
        return
    path = WS_DIR / st.session_state.ws_choice_file
    if not path.exists():
        st.error(f'Fichier introuvable : {path.name}')
        return
    try:
        df = pd.read_csv(path)
    except Exception as e:
        st.error(f'Lecture impossible : {e}')
        return
    st.subheader(f'Aperçu — {path.name}')
    st.write(f'**Taille** : {df.shape[0]} lignes × {df.shape[1]} colonnes')
    st.dataframe(df.head(100), use_container_width=True)


def show_dashboard():
    st.header('DASHBOARD (DONNÉES NETTOYÉES)')
    st.caption('Diagrammes construits à partir des CSV nettoyés (Web Scraper → nettoyage).')
    _ = sync_cleaned_from_ws()
    paths = {'Chiens': CLEAN_DIR/'chiens_clean.csv', 'Moutons': CLEAN_DIR/'moutons_clean.csv', 'Poules-Lapins-Pigeons': CLEAN_DIR/'poules_lapins_pigeons_clean.csv', 'Autres animaux': CLEAN_DIR/'autres_animaux_clean.csv'}
    frames = []
    for cat,p in paths.items():
        if p.exists():
            try:
                df0 = pd.read_csv(p); df0['category']=cat; frames.append(df0)
            except Exception:
                pass
    if not frames:
        st.warning("Aucun CSV nettoyé. Déposez d'abord des bruts en Option Web Scraper.")
        return
    clean_all = pd.concat(frames, ignore_index=True)
    clean_all = cleaning.basic_cleaning(clean_all, dropna_thresh=0.0, drop_duplicates=False)
    c1,c2 = st.columns(2); c3,c4 = st.columns(2)
    with c1: st.plotly_chart(charts.chart_price_hist(clean_all), use_container_width=True)
    with c2: st.plotly_chart(charts.chart_price_by_category(clean_all), use_container_width=True)
    with c3: st.plotly_chart(charts.chart_top_cities(clean_all), use_container_width=True)
    with c4: st.plotly_chart(charts.chart_price_bins(clean_all), use_container_width=True)


def show_feedback():
    st.header('FEEDBACK')
    st.caption('Partagez votre avis via KoBo ou Google Forms.')
    c = st.columns(3)[1]
    with c:
        st.link_button('Formulaire KoboCollect', 'https://ee.kobotoolbox.org/x/y7oeqeWT', use_container_width=True)
        st.write('')
        st.link_button('Formulaire Google Forms', 'https://docs.google.com/forms/d/e/1FAIpQLScz0D9zMk3VA10yUPXLIB76yYQFZsNy9CfsQOAjjkgY-JYeSQ/viewform?usp=publish-editor', use_container_width=True)

if menu == 'Accueil':
    show_home()
elif menu == 'Scraper':
    show_scraper()
elif menu == 'Web Scraper (CSV brut)':
    show_ws_csv()
elif menu == 'Dashboard (nettoyé)':
    show_dashboard()
elif menu == 'Feedback':
    show_feedback()