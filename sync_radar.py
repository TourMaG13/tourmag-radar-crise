#!/usr/bin/env python3
"""
Radar Crise Moyen-Orient — Script de synchronisation
Toutes les 2h via GitHub Actions :
  1. Parse le flux RSS TourMaG (rubrique crise golfe)
  2. Catégorise chaque article par thématique (mots-clés)
  3. Récupère les données financières (Brent, EUR/USD, actions tourisme)
  4. Écrit tout dans Firestore
"""

import json
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore


# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

RSS_URL = os.getenv(
    "RSS_URL",
    "https://www.tourmag.com/xml/syndication.rss?t=crise+golfe"
)

CONFLICT_START_DATE = os.getenv("CONFLICT_START_DATE", "2025-10-01")

FINANCE_SYMBOLS = {
    "brent":    {"symbol": "BZ=F",   "label": "Brent (baril)",     "currency": "$",  "sector": "commodity"},
    "eurusd":   {"symbol": "EURUSD=X","label": "EUR / USD",        "currency": "",   "sector": "forex"},
    "AF.PA":    {"symbol": "AF.PA",   "label": "Air France-KLM",   "currency": "€",  "sector": "aerien"},
    "TUI1.DE":  {"symbol": "TUI1.DE", "label": "TUI Group",        "currency": "€",  "sector": "to"},
    "AC.PA":    {"symbol": "AC.PA",   "label": "Accor",            "currency": "€",  "sector": "hotellerie"},
    "BKNG":     {"symbol": "BKNG",    "label": "Booking Holdings", "currency": "$",  "sector": "ota"},
    "CCL":      {"symbol": "CCL",     "label": "Carnival Corp",    "currency": "$",  "sector": "croisiere"},
    "AMS.MC":   {"symbol": "AMS.MC",  "label": "Amadeus IT",       "currency": "€",  "sector": "tech"},
    "AIR.PA":   {"symbol": "AIR.PA",  "label": "Airbus",           "currency": "€",  "sector": "aerien"},
    "RYA.IR":   {"symbol": "RYA.IR",  "label": "Ryanair",          "currency": "€",  "sector": "aerien"},
}

KEYWORDS_PATH = Path(__file__).parent / "keywords.json"


# ──────────────────────────────────────────────
# Firebase init
# ──────────────────────────────────────────────

def init_firebase():
    """Initialise Firebase avec le service account depuis les secrets GitHub."""
    sa_json = os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa_json:
        print("ERREUR : variable FIREBASE_SERVICE_ACCOUNT manquante")
        sys.exit(1)

    sa_dict = json.loads(sa_json)
    cred = credentials.Certificate(sa_dict)
    firebase_admin.initialize_app(cred)
    return firestore.client()


# ──────────────────────────────────────────────
# Chargement des mots-clés
# ──────────────────────────────────────────────

def load_keywords():
    """Charge le dictionnaire de mots-clés depuis keywords.json."""
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ──────────────────────────────────────────────
# Parsing RSS
# ──────────────────────────────────────────────

def clean_xml(raw_text):
    """
    Nettoie le XML brut du flux RSS pour corriger les problèmes courants :
    - & non échappés (& tout seul, pas suivi d'un nom d'entité valide)
    - Caractères de contrôle interdits en XML
    - Entités HTML non standard
    """
    import re

    # Supprimer les caractères de contrôle (sauf tab, newline, carriage return)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', raw_text)

    # Corriger les & non échappés :
    # Un & valide est suivi de #x...; ou #...; ou nom; (entité XML)
    # Tout & qui n'est pas suivi de ce pattern doit devenir &amp;
    text = re.sub(r'&(?!(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);)', '&amp;', text)

    return text


def parse_rss():
    """
    Parse le flux RSS et retourne la liste des articles.
    Récupère d'abord le XML brut, le nettoie, puis le passe à feedparser.
    """
    import requests

    # Récupérer le flux brut
    try:
        response = requests.get(RSS_URL, timeout=30, headers={
            "User-Agent": "TourMaG-Radar/1.0"
        })
        response.raise_for_status()
        raw_xml = response.text
        print(f"RSS : flux récupéré ({len(raw_xml)} caractères)")
    except Exception as e:
        print(f"ERREUR fetch RSS : {e}")
        return []

    # Nettoyer le XML
    cleaned_xml = clean_xml(raw_xml)

    # Parser avec feedparser
    feed = feedparser.parse(cleaned_xml)

    if feed.bozo and not feed.entries:
        print(f"ERREUR parsing RSS après nettoyage : {feed.bozo_exception}")
        # Tenter un fallback : parser tel quel au cas où
        feed = feedparser.parse(raw_xml)
        if not feed.entries:
            print("ERREUR : aucun article même sans nettoyage")
            return []
        print(f"Fallback : {len(feed.entries)} articles trouvés sans nettoyage")

    articles = []
    for entry in feed.entries:
        pub_date = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

        # Extraire l'image si présente (enclosure ou media)
        image_url = ""
        if hasattr(entry, "enclosures") and entry.enclosures:
            for enc in entry.enclosures:
                if enc.get("type", "").startswith("image"):
                    image_url = enc.get("href", "")
                    break
        if not image_url and hasattr(entry, "media_content"):
            for media in entry.media_content:
                if media.get("medium") == "image" or media.get("type", "").startswith("image"):
                    image_url = media.get("url", "")
                    break
        if not image_url and hasattr(entry, "media_thumbnail"):
            for thumb in entry.media_thumbnail:
                image_url = thumb.get("url", "")
                break

        articles.append({
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "description": entry.get("summary", entry.get("description", "")),
            "pub_date": pub_date,
            "image_url": image_url,
            "author": entry.get("author", ""),
        })

    print(f"RSS : {len(articles)} articles trouvés")
    return articles


# ──────────────────────────────────────────────
# Catégorisation par mots-clés
# ──────────────────────────────────────────────

def categorize_article(article, keywords_data):
    """
    Catégorise un article en scannant titre + description.
    Retourne la catégorie avec le plus de matchs, ou 'general' si aucun.
    Retourne aussi la liste des pays détectés.
    """
    text = (article["title"] + " " + article["description"]).lower()

    # Scoring par thématique
    scores = {}
    thematic_keys = [k for k in keywords_data.keys() if k != "countries_detect"]

    for cat_key in thematic_keys:
        cat_data = keywords_data[cat_key]
        score = 0
        for kw in cat_data["keywords"]:
            if kw.lower() in text:
                score += 1
        if score > 0:
            scores[cat_key] = score

    # Catégorie = celle avec le meilleur score, ou 'general'
    if scores:
        category = max(scores, key=scores.get)
    else:
        category = "general"

    # Détection des pays
    countries = []
    countries_data = keywords_data.get("countries_detect", {})
    for country_key, country_keywords in countries_data.items():
        if country_key.startswith("_"):
            continue
        for kw in country_keywords:
            if kw.lower() in text:
                countries.append(country_key)
                break

    return category, countries


# ──────────────────────────────────────────────
# Données financières
# ──────────────────────────────────────────────

def fetch_finance_data():
    """
    Récupère les cours actuels et l'historique depuis la date de début du conflit.
    Retourne un dict par symbole avec : current_price, change_pct, history[].
    """
    results = {}

    symbols_list = [v["symbol"] for v in FINANCE_SYMBOLS.values()]

    for key, config in FINANCE_SYMBOLS.items():
        sym = config["symbol"]
        try:
            ticker = yf.Ticker(sym)

            # Historique depuis début du conflit
            hist = ticker.history(start=CONFLICT_START_DATE)

            if hist.empty:
                print(f"  Finance : pas de données pour {sym}")
                continue

            current_price = float(hist["Close"].iloc[-1])
            start_price = float(hist["Close"].iloc[0])
            change_pct = round(((current_price - start_price) / start_price) * 100, 2)

            # Historique journalier (date + close) pour sparklines
            history = []
            for date, row in hist.iterrows():
                history.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "close": round(float(row["Close"]), 2)
                })

            results[key] = {
                "symbol": sym,
                "label": config["label"],
                "currency": config["currency"],
                "sector": config["sector"],
                "current_price": round(current_price, 4 if config["sector"] == "forex" else 2),
                "start_price": round(start_price, 4 if config["sector"] == "forex" else 2),
                "change_pct": change_pct,
                "history": history,
                "last_update": datetime.now(timezone.utc).isoformat(),
            }

            print(f"  Finance : {config['label']} = {current_price:.2f} ({change_pct:+.2f}%)")

        except Exception as e:
            print(f"  Finance ERREUR {sym} : {e}")

    return results


# ──────────────────────────────────────────────
# Écriture Firestore
# ──────────────────────────────────────────────

def generate_article_id(link):
    """Génère un ID unique et stable à partir du lien de l'article."""
    return hashlib.md5(link.encode("utf-8")).hexdigest()[:16]


def sync_articles(db, articles, keywords_data):
    """
    Synchronise les articles dans Firestore.
    Ne met à jour que les nouveaux articles (détection par ID/lien).
    """
    articles_ref = db.collection("articles")
    new_count = 0

    for article in articles:
        if not article["link"]:
            continue

        doc_id = generate_article_id(article["link"])
        doc_ref = articles_ref.document(doc_id)

        # Vérifier si l'article existe déjà
        if doc_ref.get().exists:
            continue

        # Catégoriser
        category, countries = categorize_article(article, keywords_data)

        # Préparer le document
        doc_data = {
            "title": article["title"],
            "link": article["link"],
            "description": article["description"],
            "image_url": article["image_url"],
            "author": article["author"],
            "pub_date": article["pub_date"],
            "category": category,
            "countries": countries,
            "created_at": firestore.SERVER_TIMESTAMP,
        }

        doc_ref.set(doc_data)
        new_count += 1
        print(f"  + [{category}] {article['title'][:60]}...")

    print(f"Articles : {new_count} nouveaux sur {len(articles)} dans le flux")
    return new_count


def sync_finance(db, finance_data):
    """Écrit les données financières dans Firestore."""
    market_ref = db.collection("market_data")

    for key, data in finance_data.items():
        market_ref.document(key).set(data)

    print(f"Finance : {len(finance_data)} symboles mis à jour")


def update_config(db, new_articles_count):
    """Met à jour le document de config avec le timestamp de dernière sync."""
    db.collection("config").document("radar").set({
        "last_sync": datetime.now(timezone.utc).isoformat(),
        "conflict_start_date": CONFLICT_START_DATE,
        "rss_url": RSS_URL,
        "last_new_articles": new_articles_count,
    }, merge=True)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    print("=" * 50)
    print(f"Radar Crise — Sync {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    # Init
    db = init_firebase()
    keywords_data = load_keywords()

    # 1. Parse RSS
    print("\n--- RSS ---")
    articles = parse_rss()

    # 2. Sync articles dans Firestore
    if articles:
        print("\n--- Articles → Firestore ---")
        new_count = sync_articles(db, articles, keywords_data)
    else:
        new_count = 0
        print("Aucun article à traiter")

    # 3. Données financières
    print("\n--- Finance ---")
    finance_data = fetch_finance_data()

    if finance_data:
        print("\n--- Finance → Firestore ---")
        sync_finance(db, finance_data)

    # 4. Update config
    update_config(db, new_count)

    print("\n" + "=" * 50)
    print("Sync terminée")
    print("=" * 50)


if __name__ == "__main__":
    main()
