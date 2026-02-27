# Animals Data Collection – CoinAfrique SN 

- Scraper **requests + BeautifulSoup** (rapide) → insertion **SQLite** (`db/app.db`).
- Onglet **Web Scraper (CSV brut)** : les fichier scrapés via web scraper `data/webscraper_csv/`.
- Onglet **Dashboard (nettoyé)** : nettoie et visualise automatiquement les csv.
- Onglet **Feedback** pour prendre en compte les avis

## Lancer
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```
