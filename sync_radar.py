#!/usr/bin/env python3
"""
Radar Crise Moyen-Orient — Script de synchronisation v3
Toutes les 2h via GitHub Actions :
  1. Parse le flux RSS TourMaG (ou scrape le HTML en fallback)
  2. Catégorise chaque article en 6 thématiques
  3. Récupère les données financières
  4. Scrape les alertes France Diplomatie avec gestion des zones partielles
  5. Génère une synthèse automatique ("L'essentiel")
  6. Écrit tout dans Firestore
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

RSS_URL = os.getenv("RSS_URL", "https://www.tourmag.com/xml/syndication.rss?t=crise+golfe")
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

MAE_COUNTRY_SLUGS = {
    "israel": "israel-palestine", "liban": "liban", "iran": "iran",
    "irak": "irak", "syrie": "syrie", "jordanie": "jordanie",
    "egypte": "egypte", "turquie": "turquie", "arabie_saoudite": "arabie-saoudite",
    "emirats": "emirats-arabes-unis", "qatar": "qatar", "oman": "oman",
    "bahrein": "bahrein", "koweit": "koweit", "yemen": "yemen",
    "chypre": "chypre", "grece": "grece",
}

MAE_COUNTRY_LABELS = {
    "israel": "Israël / Palestine", "liban": "Liban", "iran": "Iran",
    "irak": "Irak", "syrie": "Syrie", "jordanie": "Jordanie",
    "egypte": "Égypte", "turquie": "Turquie", "arabie_saoudite": "Arabie Saoudite",
    "emirats": "Émirats Arabes Unis", "qatar": "Qatar", "oman": "Oman",
    "bahrein": "Bahreïn", "koweit": "Koweït", "yemen": "Yémen",
    "chypre": "Chypre", "grece": "Grèce",
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
    cred = credentials.Certificate(json.loads(sa_json))
    firebase_admin.initialize_app(cred)
    return firestore.client()


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
    soup = BeautifulSoup(html_bytes, "html.parser")
    results = soup.find_all("div", class_="result")
    if not results:
        print("HTML fallback : aucun div.result trouvé")
        return []

    articles = []
    for div in results:
        h3 = div.find("h3", class_="titre")
        if not h3: continue
        a_tag = h3.find("a", href=True)
        if not a_tag: continue

        title = a_tag.get_text(strip=True)
        link = a_tag["href"]
        if link.startswith("/"): link = "https://www.tourmag.com" + link

        author, pub_date = "", None
        rubrique = div.find("div", class_="rubrique")
        if rubrique:
            at = rubrique.find("a", rel="author")
            if at: author = at.get_text(strip=True)
            dm = re.search(r"(\d{2}/\d{2}/\d{4})", rubrique.get_text())
            if dm:
                try: pub_date = datetime.strptime(dm.group(1), "%d/%m/%Y").replace(tzinfo=timezone.utc)
                except ValueError: pass

        description = ""
        td = div.find("div", class_="texte")
        if td:
            da = td.find("a")
            if da: description = da.get_text(strip=True)

        image_url = ""
        img = div.find("img")
        if img: image_url = img.get("src", img.get("data-src", ""))

        articles.append({"title": title, "link": link, "description": description,
                         "pub_date": pub_date, "image_url": image_url, "author": author})

    print(f"HTML fallback : {len(articles)} articles extraits")
    return articles


def parse_rss():
    try:
        response = requests.get(RSS_URL, timeout=30, headers=BROWSER_HEADERS)
        response.raise_for_status()
        raw_bytes = response.content
        content_type = response.headers.get("Content-Type", "inconnu")
        print(f"RSS : {len(raw_bytes)} octets reçus (Content-Type: {content_type})")

        is_html = b"<!DOCTYPE" in raw_bytes[:500] or b"<html" in raw_bytes[:500].lower()
        if is_html:
            print("RSS : HTML reçu — bascule en scraping")
            return parse_html_fallback(raw_bytes)

        if not raw_bytes.lstrip()[:5] in (b"<?xml", b"<rss ", b"<feed"):
            print("RSS : contenu inattendu")
            return []

        feed = feedparser.parse(raw_bytes)
        if feed.entries:
            print(f"RSS : {len(feed.entries)} articles (XML)")
        elif feed.bozo:
            print(f"RSS : XML échoué, nettoyage...")
            feed = feedparser.parse(clean_xml(raw_bytes.decode("utf-8", errors="replace")))
            if not feed.entries:
                print("RSS : irrécupérable")
                return []
        else:
            return []

    except requests.exceptions.RequestException as e:
        print(f"ERREUR RSS : {e}")
        return []

    articles = []
    for entry in feed.entries:
        pub_date = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

        image_url = ""
        for attr in ["enclosures", "media_content", "media_thumbnail"]:
            if hasattr(entry, attr):
                items = getattr(entry, attr)
                for item in (items if isinstance(items, list) else [items]):
                    url = item.get("href", item.get("url", ""))
                    if url:
                        image_url = url
                        break
            if image_url: break

        articles.append({
            "title": entry.get("title", ""), "link": entry.get("link", ""),
            "description": entry.get("summary", entry.get("description", "")),
            "pub_date": pub_date, "image_url": image_url, "author": entry.get("author", ""),
        })
    return articles


# ──────────────────────────────────────────────
# Catégorisation
# ──────────────────────────────────────────────

def categorize_article(article, keywords_data):
    text = (article["title"] + " " + article["description"]).lower()
    scores = {}
    for cat_key in [k for k in keywords_data if k != "countries_detect"]:
        score = sum(1 for kw in keywords_data[cat_key]["keywords"] if kw.lower() in text)
        if score > 0: scores[cat_key] = score

    category = max(scores, key=scores.get) if scores else "general"

    countries = []
    for ck, ckws in keywords_data.get("countries_detect", {}).items():
        if ck.startswith("_"): continue
        if any(kw.lower() in text for kw in ckws):
            countries.append(ck)

    return category, countries


# ──────────────────────────────────────────────
# Données financières
# ──────────────────────────────────────────────

def fetch_finance_data():
    results = {}
    for key, cfg in FINANCE_SYMBOLS.items():
        try:
            hist = yf.Ticker(cfg["symbol"]).history(start=CONFLICT_START_DATE)
            if hist.empty: continue
            cur = float(hist["Close"].iloc[-1])
            start = float(hist["Close"].iloc[0])
            chg = round(((cur - start) / start) * 100, 2)
            history = [{"date": d.strftime("%Y-%m-%d"), "close": round(float(r["Close"]), 2)} for d, r in hist.iterrows()]
            is_fx = cfg["sector"] == "forex"
            results[key] = {
                "symbol": cfg["symbol"], "label": cfg["label"], "currency": cfg["currency"],
                "sector": cfg["sector"], "current_price": round(cur, 4 if is_fx else 2),
                "start_price": round(start, 4 if is_fx else 2), "change_pct": chg,
                "history": history, "last_update": datetime.now(timezone.utc).isoformat(),
            }
            print(f"  Finance : {cfg['label']} = {cur:.2f} ({chg:+.2f}%)")
        except Exception as e:
            print(f"  Finance ERREUR {cfg['symbol']} : {e}")
    return results


# ──────────────────────────────────────────────
# Scraping MAE — avec gestion zones partielles
# ──────────────────────────────────────────────

def scrape_mae_alerts():
    results = {}

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
            resp = requests.get(url, timeout=15, headers=BROWSER_HEADERS)
            if resp.status_code != 200:
                print(f"  MAE {country_key} : HTTP {resp.status_code}")
                results[country_key] = _mae_fallback(country_key, url, "HTTP " + str(resp.status_code))
                continue

            soup = BeautifulSoup(resp.content, "html.parser")
            text = soup.get_text()
            text_lower = text.lower()

            # Détecter TOUS les niveaux présents (pour les zones partielles)
            levels_found = []
            for level_text, code, color in ALERT_LEVELS:
                if level_text in text_lower:
                    levels_found.append((level_text, code, color))

            if levels_found:
                # Le niveau le plus grave
                main_level = levels_found[0]
                level_label = main_level[0].capitalize()
                level_code = main_level[1]
                level_color = main_level[2]

                # Détecter les zones partielles : si plusieurs niveaux, c'est partiel
                is_partial = len(levels_found) > 1
                if is_partial:
                    other_levels = [l[0] for l in levels_found[1:]]
                    level_label += f" (certaines zones : {other_levels[0]})"
            else:
                level_label = "Non déterminé"
                level_code = "unknown"
                level_color = "gray"
                is_partial = False

            # Résumé enrichi : extraire les 2-3 premiers paragraphes pertinents
            summary_parts = []
            for p in soup.find_all("p"):
                t = p.get_text(strip=True)
                if len(t) < 20: continue
                if any(k in t.lower() for k in [
                    "déconseillé", "vigilance", "quitter", "se rendre",
                    "invités à", "recommandé", "risque", "sécurité",
                    "éviter", "limiter", "prudence", "frappes"
                ]):
                    summary_parts.append(t)
                    if len(summary_parts) >= 3:
                        break

            summary = " ".join(summary_parts)[:800] if summary_parts else ""

            # Si pas de résumé, prendre la meta description
            if not summary:
                meta = soup.find("meta", attrs={"name": "description"})
                if meta:
                    s = meta.get("content", "").strip()
                    if s and "ministère" not in s.lower():
                        summary = s[:500]

            # Date de dernière mise à jour
            last_update = ""
            um = re.search(r'Dernière mise à jour[^\d]*(\d{1,2}\s+\w+\s+\d{4})', text.replace('\n', ' '))
            if um: last_update = um.group(1).strip()

            results[country_key] = {
                "country": country_key,
                "label": MAE_COUNTRY_LABELS.get(country_key, country_key),
                "level": level_label,
                "level_code": level_code,
                "color": level_color,
                "is_partial": is_partial,
                "summary": summary,
                "url": url,
                "last_update_mae": last_update,
                "last_scraped": datetime.now(timezone.utc).isoformat(),
            }
            print(f"  MAE {country_key} : {level_label} {'(partiel)' if is_partial else ''} (MàJ: {last_update or 'n/a'})")

        except Exception as e:
            print(f"  MAE ERREUR {country_key} : {e}")
            results[country_key] = _mae_fallback(country_key, url, str(e)[:200])

    return results


def _mae_fallback(country_key, url, error_msg):
    return {
        "country": country_key,
        "label": MAE_COUNTRY_LABELS.get(country_key, country_key),
        "level": "Information indisponible",
        "level_code": "unknown", "color": "gray", "is_partial": False,
        "summary": error_msg, "url": url,
        "last_update_mae": "",
        "last_scraped": datetime.now(timezone.utc).isoformat(),
    }


# ──────────────────────────────────────────────
# Synthèse automatique — "L'essentiel"
# ──────────────────────────────────────────────

def generate_synthesis(articles):
    """
    Génère une synthèse à partir des 5-7 derniers articles.
    Extrait titre + première phrase de la description.
    """
    synthesis_points = []

    # Prendre les 7 derniers articles (ils sont déjà triés par date desc)
    for article in articles[:7]:
        title = article.get("title", "")
        desc = article.get("description", "")

        # Extraire la première phrase de la description
        first_sentence = ""
        if desc:
            # Couper à la première phrase (point suivi d'une majuscule ou fin)
            match = re.match(r'^(.+?[.!?])\s', desc)
            if match:
                first_sentence = match.group(1)
            else:
                first_sentence = desc[:150] + "..." if len(desc) > 150 else desc

        synthesis_points.append({
            "title": title,
            "summary": first_sentence,
            "link": article.get("link", ""),
            "date": article.get("pub_date").isoformat() if article.get("pub_date") else "",
            "author": article.get("author", ""),
        })

    return {
        "points": synthesis_points,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(articles),
    }


# ──────────────────────────────────────────────
# Écriture Firestore
# ──────────────────────────────────────────────

def generate_article_id(link):
    return hashlib.md5(link.encode("utf-8")).hexdigest()[:16]


def sync_articles(db, articles, keywords_data):
    ref = db.collection("articles")
    new_count = 0
    for article in articles:
        if not article["link"]: continue
        doc_id = generate_article_id(article["link"])
        doc_ref = ref.document(doc_id)
        if doc_ref.get().exists: continue

        category, countries = categorize_article(article, keywords_data)
        doc_ref.set({
            "title": article["title"], "link": article["link"],
            "description": article["description"], "image_url": article["image_url"],
            "author": article["author"], "pub_date": article["pub_date"],
            "category": category, "countries": countries,
            "created_at": firestore.SERVER_TIMESTAMP,
        })
        new_count += 1
        print(f"  + [{category}] {article['title'][:60]}...")
    print(f"Articles : {new_count} nouveaux sur {len(articles)} dans le flux")
    return new_count


def sync_finance(db, data):
    ref = db.collection("market_data")
    for key, d in data.items(): ref.document(key).set(d)
    print(f"Finance : {len(data)} symboles mis à jour")


def sync_mae_alerts(db, data):
    ref = db.collection("mae_alerts")
    for key, d in data.items(): ref.document(key).set(d)
    print(f"MAE : {len(data)} pays mis à jour")


def sync_synthesis(db, synthesis):
    db.collection("config").document("synthesis").set(synthesis)
    print(f"Synthèse : {len(synthesis['points'])} points générés")


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

    # 5. Synthèse automatique
    if articles:
        print("\n--- Synthèse ---")
        synthesis = generate_synthesis(articles)
        sync_synthesis(db, synthesis)

    # 6. Update config
    update_config(db, new_count)

    print("\n" + "=" * 50)
    print("Sync terminée")
    print("=" * 50)


if __name__ == "__main__":
    main()
