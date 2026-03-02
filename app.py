
# -*- coding: utf-8 -*-
import os
import sys
import sqlite3
from pathlib import Path

import streamlit as st
import pandas as pd

# Assure l'import local des modules utils/*
sys.path.insert(0, str(Path(__file__).resolve().parent))

import utils.scraping_bs as scraping
import utils.cleaning as cleaning
import utils.charts as charts
# On évite d'utiliser utils.db ici pour l'affichage pour rester agnostique du chemin
# import utils.db as dbutils

# -----------------------------------------------------------------------------
# Config Streamlit
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Animals Data Collection – CoinAfrique SN",
    page_icon="🐾",
    layout="wide"
)

# CSS (thème orange)
css_path = Path('assets/theme.css')
if css_path.exists():
    st.markdown('<style>' + css_path.read_text(encoding='utf-8') + '</style>', unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Paramètres DB centralisés : st.secrets -> env -> défauts
# -----------------------------------------------------------------------------
DB_PATH  = st.secrets.get("DB_PATH", os.environ.get("DB_PATH", "coinafrique.db"))
DB_TABLE = st.secrets.get("DB_TABLE", os.environ.get("DB_TABLE", "annonces"))

# Flag DEBUG (ne rien afficher par défaut pour l'utilisateur final)
DEBUG = str(st.secrets.get("DEBUG", os.environ.get("DEBUG", "0"))).strip() in ("1", "true", "True", "YES", "yes")

# Affichage d'un rappel des paramètres DB (uniquement en mode DEBUG)
if DEBUG:
    st.sidebar.caption(f"💾 DB: `{DB_PATH}` · Table: `{DB_TABLE}`")

# -----------------------------------------------------------------------------
# Constantes & répertoires
# -----------------------------------------------------------------------------
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
for p in (WS_DIR, CLEAN_DIR, RAW_DIR):
    p.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# Fonctions utilitaires locales
# -----------------------------------------------------------------------------
def load_db(db_path: str = DB_PATH, table: str = DB_TABLE, category: str | None = None, limit: int = 500) -> pd.DataFrame:
    """
    Lecture de la DB SQLite en s’assurant d’utiliser le même chemin que le scraper.
    Renvoie un DataFrame vide si le fichier ou la table n’existent pas.
    """
    db_file = Path(db_path)
    if not db_file.exists():
        return pd.DataFrame()

    with sqlite3.connect(str(db_file)) as conn:
        # Vérification de la table
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,)
        ).fetchone()
        if not exists:
            return pd.DataFrame()

        if category:
            # category provient d'une liste fermée (sélecteur)
            q = f"SELECT * FROM {table} WHERE category = ? ORDER BY id DESC LIMIT ?;"
            df = pd.read_sql_query(q, conn, params=(category, int(limit)))
        else:
            q = f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?;"
            df = pd.read_sql_query(q, conn, params=(int(limit),))

    return df

def harmonize_columns_for_display(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """
    - Si 'title' existe, renomme en:
        * 'details' pour 'Autres animaux'
        * 'Nom' pour les autres catégories
    - Réordonne de façon lisible si possibles colonnes présentes
    """
    if df is None or df.empty:
        return df

    df2 = df.copy()
    if 'title' in df2.columns:
        if category == 'Autres animaux':
            df2 = df2.rename(columns={'title': 'details'})
        else:
            df2 = df2.rename(columns={'title': 'Nom'})

    # Ordre de colonnes recommandé
    preferred = ['id', 'category', 'Nom', 'details', 'price_raw', 'address_raw', 'image_url', 'link', 'page', 'source']
    cols = [c for c in preferred if c in df2.columns] + [c for c in df2.columns if c not in preferred]
    df2 = df2[cols]
    return df2

def sync_cleaned_from_ws():
    WS_EXPECTED = {
        'chiens': WS_DIR / 'chiens.csv',
        'moutons': WS_DIR / 'moutons.csv',
        'poules_lapins_pigeons': WS_DIR / 'poules_lapins_pigeons.csv',
        'autres_animaux': WS_DIR / 'autres_animaux.csv'
    }
    CLEAN_TARGETS = {
        'chiens': CLEAN_DIR / 'chiens_clean.csv',
        'moutons': CLEAN_DIR / 'moutons_clean.csv',
        'poules_lapins_pigeons': CLEAN_DIR / 'poules_lapins_pigeons_clean.csv',
        'autres_animaux': CLEAN_DIR / 'autres_animaux_clean.csv'
    }
    results = {}
    for key, ws_path in WS_EXPECTED.items():
        clean_path = CLEAN_TARGETS[key]
        results[key] = {'ws': ws_path, 'clean': clean_path, 'status': 'skipped'}
        if not ws_path.exists():
            results[key]['status'] = 'missing_raw'
            continue
        need_refresh = (not clean_path.exists()) or (ws_path.stat().st_mtime > clean_path.stat().st_mtime)
        if need_refresh:
            try:
                df_raw = pd.read_csv(ws_path)
                if df_raw.empty:
                    results[key]['status'] = 'raw_empty'
                    continue
                df_clean = cleaning.basic_cleaning(df_raw.copy(), dropna_thresh=0.7, drop_duplicates=True)
                df_clean.to_csv(clean_path, index=False, encoding='utf-8')
                results[key]['status'] = 'cleaned'
            except Exception as e:
                results[key]['status'] = f'error: {e}'
        else:
            results[key]['status'] = 'up_to_date'
    return results

# -----------------------------------------------------------------------------
# Pages
# -----------------------------------------------------------------------------
st.sidebar.image('assets/logo.png', width=120)
st.sidebar.title('Menu')
menu = st.sidebar.selectbox(
    'Choisir une page',
    ('Accueil', 'Scraper', 'Web Scraper (CSV brut)', 'Dashboard (nettoyé)', 'Feedback'),
    index=0
)

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
        "Choisissez une catégorie, puis cliquez sur **Lancer le scraping** pour scraper et enregistrer dans la base SQL. "
        "Ensuite, cliquez sur **Afficher les données en DB** pour voir les lignes enregistrées."
    )

    c1, c2 = st.columns(2)
    with c1:
        category = st.selectbox('Catégorie', list(SCRAPE_URLS.keys()), index=0)
    with c2:
        pages = st.slider('Pages', 1, 10, DEFAULT_PAGES)

    # Lancer le scraping et insertion DB
    if st.button('Lancer le scraping et enregistrer en DB', type='primary'):
        with st.spinner('Scraping + insert (BeautifulSoup via Selenium pour la liste)…'):
            try:
                inserted = scraping.bs4_scrape_insert(
                    category=category,
                    start_page=1,
                    end_page=int(pages),
                    list_only=True,       # ultra-rapide; bascule à False pour visiter les détails
                    visit_detail=False,    # ignoré si list_only=True
                    headless=True,
                    db_path=DB_PATH,       # ✅ même DB que l’affichage
                    table=DB_TABLE,        # ✅ même table que l’affichage
                )
                st.success(f"Terminé — {inserted} nouvelles lignes insérées (INSERT OR IGNORE).")
            except Exception as e:
                st.error(f'Erreur : {e}')

    # Afficher les données de la DB (même source)
    if st.button('Afficher les données en DB'):
        df_db = load_db(DB_PATH, DB_TABLE, category=category, limit=500)
        if df_db is None or df_db.empty:
            # Messages d'aide si vide
            db_file = Path(DB_PATH)
            if not db_file.exists():
                st.info(f"La base est vide ou introuvable à ce chemin : `{DB_PATH}`.")
            else:
                st.info("La table est vide ou n'a pas encore été créée dans la DB.")
                # (En DEBUG uniquement) lister les tables disponibles
                if DEBUG:
                    try:
                        with sqlite3.connect(str(db_file)) as conn:
                            tables = conn.execute(
                                "SELECT name FROM sqlite_master WHERE type='table';"
                            ).fetchall()
                        if tables:
                            st.caption("Tables présentes : " + ", ".join(t[0] for t in tables))
                    except Exception:
                        pass
        else:
            df_disp = harmonize_columns_for_display(df_db, category)
            st.success(f"{len(df_disp)} lignes chargées depuis `{DB_TABLE}` (catégorie: {category}).")
            st.dataframe(df_disp.head(300), use_container_width=True)

def show_ws_csv():
    st.header('WEB SCRAPER')
    st.caption('Cliquez sur une catégorie pour afficher les CSV bruts (collectés avec l’extension Web Scraper).')
    FILE_MAP = {
        'Chiens': 'chiens.csv',
        'Moutons': 'moutons.csv',
        'Poules-Lapins-Pigeons': 'poules_lapins_pigeons.csv',
        'Autres animaux': 'autres_animaux.csv'
    }
    if 'ws_choice_file' not in st.session_state:
        st.session_state.ws_choice_file = None

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button('Chiens', use_container_width=True):
            st.session_state.ws_choice_file = FILE_MAP['Chiens']
    with c2:
        if st.button('Moutons', use_container_width=True):
            st.session_state.ws_choice_file = FILE_MAP['Moutons']
    with c3:
        if st.button('Poules-Lapins-Pigeons', use_container_width=True):
            st.session_state.ws_choice_file = FILE_MAP['Poules-Lapins-Pigeons']
    with c4:
        if st.button('Autres animaux', use_container_width=True):
            st.session_state.ws_choice_file = FILE_MAP['Autres animaux']

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

    paths = {
        'Chiens': CLEAN_DIR / 'chiens_clean.csv',
        'Moutons': CLEAN_DIR / 'moutons_clean.csv',
        'Poules-Lapins-Pigeons': CLEAN_DIR / 'poules_lapins_pigeons_clean.csv',
        'Autres animaux': CLEAN_DIR / 'autres_animaux_clean.csv'
    }
    frames = []
    for cat, p in paths.items():
        if p.exists():
            try:
                df0 = pd.read_csv(p)
                df0['category'] = cat
                frames.append(df0)
            except Exception:
                pass

    if not frames:
        st.warning("Aucun CSV nettoyé. Déposez d'abord des bruts en Option Web Scraper.")
        return

    clean_all = pd.concat(frames, ignore_index=True)
    clean_all = cleaning.basic_cleaning(clean_all, dropna_thresh=0.0, drop_duplicates=False)

    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)
    with c1:
        st.plotly_chart(charts.chart_price_hist(clean_all), use_container_width=True)
    with c2:
        st.plotly_chart(charts.chart_price_by_category(clean_all), use_container_width=True)
    with c3:
        st.plotly_chart(charts.chart_top_cities(clean_all), use_container_width=True)
    with c4:
        st.plotly_chart(charts.chart_price_bins(clean_all), use_container_width=True)

def show_feedback():
    st.header('FEEDBACK')
    st.caption('Partagez votre avis via KoBo ou Google Forms.')
    c = st.columns(3)[1]
    with c:
        st.link_button('Formulaire KoboCollect', 'https://ee.kobotoolbox.org/x/y7oeqeWT', use_container_width=True)
        st.write('')
        st.link_button(
            'Formulaire Google Forms',
            'https://docs.google.com/forms/d/e/1FAIpQLScz0D9zMk3VA10yUPXLIB76yYQFZsNy9CfsQOAjjkgY-JYeSQ/viewform?usp=publish-editor',
            use_container_width=True
        )

# -----------------------------------------------------------------------------
# Routing
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# (Optionnel) Diagnostic rapide pour Streamlit Cloud - seulement en DEBUG
# -----------------------------------------------------------------------------
if DEBUG:
    with st.expander("🛠️ Diagnostic (optionnel)"):
        st.caption(f"cwd: {os.getcwd()}")
        try:
            st.caption("Fichiers dans le répertoire courant :")
            st.code("\n".join(os.listdir(".")))
        except Exception:
            pass
        st.caption(f"DB_PATH utilisé: {DB_PATH}")
        st.caption(f"DB_TABLE utilisée: {DB_TABLE}")
