# -*- coding: utf-8 -*-
import pandas as pd
import plotly.express as px

def _ensure_category(df: pd.DataFrame) -> pd.DataFrame:
    if 'category' not in df.columns:
        df = df.copy(); df['category'] = 'Inconnu'
    return df

def chart_price_hist(df: pd.DataFrame):
    return px.histogram(df, x='price_cfa', nbins=40, title='Distribution des prix (CFA)')

def chart_price_by_category(df: pd.DataFrame):
    df = _ensure_category(df)
    g = df.groupby('category', dropna=False)['price_cfa'].median().reset_index()
    g = g.sort_values('price_cfa', ascending=False)
    return px.bar(g, x='category', y='price_cfa', title='Prix médian par catégorie (CFA)')

def chart_top_cities(df: pd.DataFrame, topn: int = 15):
    g = df['city'].fillna('N/A').value_counts().reset_index().head(topn)
    g.columns = ['city','count']
    return px.bar(g, x='city', y='count', title=f'Top {topn} villes (compte annonces)')

def chart_price_bins(df: pd.DataFrame):
    if df['price_cfa'].notna().any():
        bins = [0, 50000, 100000, 200000, 300000, 500000, 1000000, df['price_cfa'].max()]
    else:
        bins = [0,1]
    labels = ['<=50k','50-100k','100-200k','200-300k','300-500k','500k-1M','>1M']
    try:
        s = pd.cut(df['price_cfa'], bins=bins, labels=labels, include_lowest=True)
    except Exception:
        s = pd.Series(['N/A']*len(df))
    g = s.value_counts().reindex(labels).reset_index()
    g.columns = ['bin','count']
    return px.bar(g, x='bin', y='count', title='Répartition par tranches de prix (CFA)')
