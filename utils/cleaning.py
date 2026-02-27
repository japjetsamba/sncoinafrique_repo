# -*- coding: utf-8 -*-
import re
import pandas as pd

PRICE_RE = re.compile(r'(\d[\d\s\.,]*)', re.I)

def basic_cleaning(df_raw: pd.DataFrame, dropna_thresh: float = 0.0, drop_duplicates: bool = False) -> pd.DataFrame:
    df = df_raw.copy()

    # prix -> price_cfa
    price_candidates = [c for c in df.columns if str(c).strip().lower() in ('price_cfa','price','prix','price_raw')]
    price_col = price_candidates[0] if price_candidates else None
    def _to_int(txt):
        if txt is None:
            return None
        s = str(txt)
        m = PRICE_RE.search(s)
        if not m:
            return None
        digits = m.group(1).replace(' ','').replace(' ','').replace(',','').replace('.','')
        try:
            return int(digits)
        except Exception:
            return None
    df['price_cfa'] = df[price_col].apply(_to_int) if price_col is not None else None

    # adresse -> city
    addr_candidates = [c for c in df.columns if str(c).strip().lower() in ('address_raw','adresse','address','location','ad__card-location')]
    addr_col = addr_candidates[0] if addr_candidates else None
    def extract_city(addr):
        if addr is None:
            return None
        s = str(addr)
        for sep in ['•','-',' ',',','/']:
            if sep in s:
                return s.split(sep)[0].strip()
        return s.strip()
    df['city'] = df[addr_col].apply(extract_city) if addr_col is not None else None

    # titre -> title_len
    title_candidates = [c for c in df.columns if str(c).strip().lower() in ('title','nom','name','details','detail','ad__card-description')]
    title_col = title_candidates[0] if title_candidates else None
    df['title_len'] = df[title_col].apply(lambda x: len(str(x)) if x is not None else 0) if title_col is not None else 0

    if drop_duplicates:
        df = df.drop_duplicates()
    if dropna_thresh and dropna_thresh > 0:
        df = df.dropna(thresh=int(df.shape[1]*dropna_thresh))

    return df
