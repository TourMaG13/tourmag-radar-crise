#!/usr/bin/env python3
"""
Radar Crise Moyen-Orient — Script de synchronisation
Toutes les 2h via GitHub Actions :
  1. Parse le flux RSS TourMaG (ou scrape le HTML en fallback)
  2. Catégorise chaque article en 6 thématiques orientées action
  3. Récupère les données financières (Brent, EUR/USD, actions tourisme)
  4. Scrape les alertes France Diplomatie (MAE) par pays
  5. Écrit tout dans Firestore
"""

import json
import hashlib
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
import yfinance as yf
from bs4 import BeautifulSoup
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

# Slugs France Diplomatie par pays
MAE_COUNTRY_SLUGS = {
    "israel":          "israel-palestine",
    "liban":           "liban",
    "iran":            "iran",
    "irak":            "irak",
    "syrie":           "syrie",
    "jordanie":        "jordanie",
    "egypte":          "egypte",
    "turquie":         "turquie",
    "arabie_saoudite": "arabie-saoudite",
    "emirats":         "emirats-arabes-unis",
    "qatar":           "qatar",
    "oman":            "oman",
    "bahrein":         "bahrein",
    "koweit":          "koweit",
    "yemen":           "yemen",
    "chypre":          "chypre",
    "grece":           "grece",
}

MAE_COUNTRY_LABELS = {
    "israel": "Israël / Palestine",
    "liban": "Liban",
    "iran": "Iran",
    "irak": "Irak",
    "syrie": "Syrie",
    "jordanie": "Jordanie",
    "egypte": "Égypte",
    "turquie": "Turquie",
    "arabie_saoudite": "Arabie Saoudite",
    "emirats": "Émirats Arabes Unis",
    "qatar": "Qatar",
    "oman": "Oman",
    "bahrein": "Bahreïn",
    "koweit": "Koweït",
    "yemen": "Yémen",
    "chypre": "Chypre",
    "grece": "Grèce",
}

MAE_BASE_URL = "https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/conseils-par-pays-destination/"

KEYWORDS_PATH = Path(__file__).parent / "keywords.json"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


# ──────────────────────────────────────────────
# Firebase init
# ──────────────────────────────────────────────

def init_firebase():
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
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ──────────────────────────────────────────────
# Parsing RSS / HTML
# ──────────────────────────────────────────────

def clean_xml(raw_text):
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', raw_text)
    text = re.sub(r'&(?!(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);)', '&amp;', text)
    return text


def parse_html_fallback(html_bytes):
    """Parse la page HTML TourMaG quand le serveur refuse le RSS."""
    soup = BeautifulSoup(html_bytes, "html.parser")
    results = soup.find_all("div", class_="result")

    if not results:
        print("HTML fallback : aucun div.result trouvé")
        return []

    articles = []
    for div in results:
        h3 = div.find("h3", class_="titre")
        if not h3:
            continue
        a_tag = h3.find("a", href=True)
        if not a_tag:
            continue

        title = a_tag.get_text(strip=True)
        link = a_tag["href"]
        if link.startswith("/"):
            link = "https://www.tourmag.com" + link

        author = ""
        pub_date = None
        rubrique = div.find("div", class_="rubrique")
        if rubrique:
            author_tag = rubrique.find("a", rel="author")
            if author_tag:
                author = author_tag.get_text(strip=True)
            date_match = re.search(r"(\d{2}/\d{2}/\d{4})", rubrique.get_text())
            if date_match:
                try:
                    pub_date = datetime.strptime(date_match.group(1), "%d/%m/%Y")
                    pub_date = pub_date.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

        description = ""
        texte_div = div.find("div", class_="texte")
        if texte_div:
            desc_a = texte_div.find("a")
            if desc_a:
                description = desc_a.get_text(strip=True)

        image_url = ""
        img = div.find("img")
        if img:
            image_url = img.get("src", img.get("data-src", ""))

        articles.append({
            "title": title,
            "link": link,
            "description": description,
            "pub_date": pub_date,
            "image_url": image_url,
            "author": author,
        })

    print(f"HTML fallback : {len(articles)} articles extraits")
    return articles


def parse_rss():
    """Parse le flux RSS, avec fallback HTML si le serveur bloque."""
    try:
        response = requests.get(RSS_URL, timeout=30, headers=BROWSER_HEADERS)
        response.raise_for_status()
        raw_bytes = response.content
        content_type = response.headers.get("Content-Type", "inconnu")

        print(f"RSS : {len(raw_bytes)} octets reçus (Content-Type: {content_type})")

        is_html = b"<!DOCTYPE" in raw_bytes[:500] or b"<html" in raw_bytes[:500].lower()

        if is_html:
            print("RSS : le serveur a renvoyé du HTML — bascule en scraping")
            return parse_html_fallback(raw_bytes)

        starts_with_xml = raw_bytes.lstrip()[:5] in (b"<?xml", b"<rss ", b"<feed")
        if not starts_with_xml:
            print(f"RSS : contenu inattendu")
            return []

        feed = feedparser.parse(raw_bytes)

        if feed.entries:
            print(f"RSS : {len(feed.entries)} articles trouvés (XML)")
        elif feed.bozo:
            print(f"RSS : parsing XML échoué ({feed.bozo_exception}), tentative nettoyage...")
            cleaned = clean_xml(raw_bytes.decode("utf-8", errors="replace"))
            feed = feedparser.parse(cleaned)
            if feed.entries:
                print(f"RSS : {len(feed.entries)} articles après nettoyage")
            else:
                print("RSS : XML irrécupérable")
                return []
        else:
            return []

    except requests.exceptions.RequestException as e:
        print(f"ERREUR fetch RSS : {e}")
        return []

    articles = []
    for entry in feed.entries:
        pub_date = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

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

    print(f"RSS : {len(articles)} articles parsés")
    return articles


# ──────────────────────────────────────────────
# Catégorisation par mots-clés
# ──────────────────────────────────────────────

def categorize_article(article, keywords_data):
    text = (article["title"] + " " + article["description"]).lower()

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

    category = max(scores, key=scores.get) if scores else "general"

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
    results = {}

    for key, config in FINANCE_SYMBOLS.items():
        sym = config["symbol"]
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(start=CONFLICT_START_DATE)

            if hist.empty:
                print(f"  Finance : pas de données pour {sym}")
                continue

            current_price = float(hist["Close"].iloc[-1])
            start_price = float(hist["Close"].iloc[0])
            change_pct = round(((current_price - start_price) / start_price) * 100, 2)

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
# Scraping France Diplomatie (MAE)
# ──────────────────────────────────────────────

def scrape_mae_alerts():
    """
    Scrape les conseils aux voyageurs depuis diplomatie.gouv.fr.
    Extrait le niveau d'alerte et le résumé pour chaque pays.
    """
    results = {}

    # Niveaux d'alerte du plus grave au moins grave
    ALERT_LEVELS = [
        ("formellement déconseillé", "formellement_deconseille", "red"),
        ("déconseillé sauf raison impérative", "deconseille_sauf_ri", "orange"),
        ("déconseillé sauf raison", "deconseille_sauf_ri", "orange"),
        ("vigilance renforcée", "vigilance_renforcee", "yellow"),
        ("vigilance normale", "vigilance_normale", "green"),
    ]

    for country_key, slug in MAE_COUNTRY_SLUGS.items():
        url = f"{MAE_BASE_URL}{slug}/"
        try:
            response = requests.get(url, timeout=15, headers=BROWSER_HEADERS)

            if response.status_code != 200:
                print(f"  MAE {country_key} : HTTP {response.status_code}")
                results[country_key] = {
                    "country": country_key,
                    "label": MAE_COUNTRY_LABELS.get(country_key, country_key),
                    "level": "indisponible",
                    "level_code": "unknown",
                    "color": "gray",
                    "summary": "Information indisponible",
                    "url": url,
                    "last_update_mae": "",
                    "last_scraped": datetime.now(timezone.utc).isoformat(),
                }
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            text = soup.get_text()

            # Extraire le niveau d'alerte
            level_label = "Non déterminé"
            level_code = "unknown"
            level_color = "gray"

            for level_text, code, color in ALERT_LEVELS:
                if level_text in text.lower():
                    level_label = level_text.capitalize()
                    level_code = code
                    level_color = color
                    break

            # Extraire la meta description (résumé)
            meta_desc = soup.find("meta", attrs={"name": "description"})
            summary = ""
            if meta_desc:
                summary = meta_desc.get("content", "").strip()

            # Si la meta desc est générique, chercher le premier paragraphe pertinent
            if not summary or "ministère" in summary.lower():
                for p in soup.find_all("p"):
                    t = p.get_text(strip=True)
                    if any(k in t.lower() for k in ["déconseillé", "vigilance", "quitter", "se rendre", "invités à"]):
                        if 20 < len(t) < 500:
                            summary = t
                            break

            # Extraire la date de dernière mise à jour
            last_update = ""
            update_match = re.search(
                r'Dernière mise à jour[^\d]*(\d{1,2}\s+\w+\s+\d{4})',
                text.replace('\n', ' ')
            )
            if update_match:
                last_update = update_match.group(1).strip()

            results[country_key] = {
                "country": country_key,
                "label": MAE_COUNTRY_LABELS.get(country_key, country_key),
                "level": level_label,
                "level_code": level_code,
                "color": level_color,
                "summary": summary[:500] if summary else "",
                "url": url,
                "last_update_mae": last_update,
                "last_scraped": datetime.now(timezone.utc).isoformat(),
            }

            print(f"  MAE {country_key} : {level_label} (MàJ: {last_update or 'n/a'})")

        except Exception as e:
            print(f"  MAE ERREUR {country_key} : {e}")
            results[country_key] = {
                "country": country_key,
                "label": MAE_COUNTRY_LABELS.get(country_key, country_key),
                "level": "Erreur de récupération",
                "level_code": "error",
                "color": "gray",
                "summary": str(e)[:200],
                "url": url,
                "last_update_mae": "",
                "last_scraped": datetime.now(timezone.utc).isoformat(),
            }

    return results


# ──────────────────────────────────────────────
# Écriture Firestore
# ──────────────────────────────────────────────

def generate_article_id(link):
    return hashlib.md5(link.encode("utf-8")).hexdigest()[:16]


def sync_articles(db, articles, keywords_data):
    articles_ref = db.collection("articles")
    new_count = 0

    for article in articles:
        if not article["link"]:
            continue

        doc_id = generate_article_id(article["link"])
        doc_ref = articles_ref.document(doc_id)

        if doc_ref.get().exists:
            continue

        category, countries = categorize_article(article, keywords_data)

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
    market_ref = db.collection("market_data")
    for key, data in finance_data.items():
        market_ref.document(key).set(data)
    print(f"Finance : {len(finance_data)} symboles mis à jour")


def sync_mae_alerts(db, mae_data):
    """Écrit les alertes MAE dans Firestore."""
    mae_ref = db.collection("mae_alerts")
    for key, data in mae_data.items():
        mae_ref.document(key).set(data)
    print(f"MAE : {len(mae_data)} pays mis à jour")


def update_config(db, new_articles_count):
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

    db = init_firebase()
    keywords_data = load_keywords()

    # 1. Parse RSS / HTML
    print("\n--- RSS ---")
    articles = parse_rss()

    # 2. Sync articles
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

    # 4. Alertes MAE
    print("\n--- France Diplomatie (MAE) ---")
    mae_data = scrape_mae_alerts()
    if mae_data:
        print("\n--- MAE → Firestore ---")
        sync_mae_alerts(db, mae_data)

    # 5. Update config
    update_config(db, new_count)

    print("\n" + "=" * 50)
    print("Sync terminée")
    print("=" * 50)


if __name__ == "__main__":
    main()
