#!/usr/bin/env python3
"""
Radar Crise Moyen-Orient — v4
  1. Parse flux RSS TourMaG (HTML fallback)
  2. Classification IA via Groq (Llama 3)
  3. Synthèse "L'essentiel" via Groq
  4. Données financières (Yahoo Finance)
  5. Alertes France Diplomatie (zones partielles)
  6. NOTAM — fermetures d'espace aérien
  7. Écriture Firestore
"""

import json, hashlib, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path

import feedparser, requests, yfinance as yf
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
RSS_URL = os.getenv("RSS_URL", "https://www.tourmag.com/xml/syndication.rss?t=crise+golfe")
CONFLICT_START_DATE = os.getenv("CONFLICT_START_DATE", "2025-10-01")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

FINANCE_SYMBOLS = {
    "brent":   {"symbol":"BZ=F","label":"Brent (baril)","currency":"$","sector":"commodity"},
    "eurusd":  {"symbol":"EURUSD=X","label":"EUR / USD","currency":"","sector":"forex"},
    "AF.PA":   {"symbol":"AF.PA","label":"Air France-KLM","currency":"€","sector":"aerien"},
    "TUI1.DE": {"symbol":"TUI1.DE","label":"TUI Group","currency":"€","sector":"to"},
    "AC.PA":   {"symbol":"AC.PA","label":"Accor","currency":"€","sector":"hotellerie"},
    "BKNG":    {"symbol":"BKNG","label":"Booking Holdings","currency":"$","sector":"ota"},
    "CCL":     {"symbol":"CCL","label":"Carnival Corp","currency":"$","sector":"croisiere"},
    "AMS.MC":  {"symbol":"AMS.MC","label":"Amadeus IT","currency":"€","sector":"tech"},
    "AIR.PA":  {"symbol":"AIR.PA","label":"Airbus","currency":"€","sector":"aerien"},
    "RYA.IR":  {"symbol":"RYA.IR","label":"Ryanair","currency":"€","sector":"aerien"},
}

MAE_SLUGS = {
    "israel":"israel-palestine","liban":"liban","iran":"iran","irak":"irak",
    "syrie":"syrie","jordanie":"jordanie","egypte":"egypte","turquie":"turquie",
    "arabie_saoudite":"arabie-saoudite","emirats":"emirats-arabes-unis","qatar":"qatar",
    "oman":"oman","bahrein":"bahrein","koweit":"koweit","yemen":"yemen",
    "chypre":"chypre","grece":"grece",
}
MAE_LABELS = {
    "israel":"Israël / Palestine","liban":"Liban","iran":"Iran","irak":"Irak",
    "syrie":"Syrie","jordanie":"Jordanie","egypte":"Égypte","turquie":"Turquie",
    "arabie_saoudite":"Arabie Saoudite","emirats":"Émirats Arabes Unis","qatar":"Qatar",
    "oman":"Oman","bahrein":"Bahreïn","koweit":"Koweït","yemen":"Yémen",
    "chypre":"Chypre","grece":"Grèce",
}
MAE_BASE = "https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/conseils-par-pays-destination/"

# ICAO FIR codes pour NOTAM (espaces aériens Moyen-Orient)
NOTAM_FIRS = {
    "LLLL": "Israël", "OLBB": "Liban", "OIIX": "Iran", "ORBB": "Irak",
    "OSTT": "Syrie", "OJAC": "Jordanie", "HECC": "Égypte",
    "LTAA": "Turquie", "OEJD": "Arabie Saoudite", "OMAE": "Émirats",
    "OTHH": "Qatar", "OOMM": "Oman", "OBBB": "Bahreïn", "OKAC": "Koweït",
    "OYSC": "Yémen",
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

KEYWORDS_PATH = Path(__file__).parent / "keywords.json"


# ──────────────────────────────────────────────
# Firebase
# ──────────────────────────────────────────────
def init_firebase():
    sa = os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa: print("ERREUR: FIREBASE_SERVICE_ACCOUNT manquant"); sys.exit(1)
    firebase_admin.initialize_app(credentials.Certificate(json.loads(sa)))
    return firestore.client()

def load_keywords():
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f: return json.load(f)


# ──────────────────────────────────────────────
# RSS / HTML parsing (inchangé)
# ──────────────────────────────────────────────
def clean_xml(t):
    t = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', t)
    return re.sub(r'&(?!(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);)', '&amp;', t)

def parse_html_fallback(html_bytes):
    soup = BeautifulSoup(html_bytes, "html.parser")
    articles = []
    for div in soup.find_all("div", class_="result"):
        h3 = div.find("h3", class_="titre")
        if not h3: continue
        a = h3.find("a", href=True)
        if not a: continue
        title = a.get_text(strip=True)
        link = a["href"]
        if link.startswith("/"): link = "https://www.tourmag.com" + link
        author, pub_date = "", None
        rub = div.find("div", class_="rubrique")
        if rub:
            at = rub.find("a", rel="author")
            if at: author = at.get_text(strip=True)
            dm = re.search(r"(\d{2}/\d{2}/\d{4})", rub.get_text())
            if dm:
                try: pub_date = datetime.strptime(dm.group(1), "%d/%m/%Y").replace(tzinfo=timezone.utc)
                except: pass
        desc = ""
        td = div.find("div", class_="texte")
        if td:
            da = td.find("a")
            if da: desc = da.get_text(strip=True)
        img = ""
        im = div.find("img")
        if im:
            img = im.get("src", im.get("data-src", ""))
            # Filtrer les fausses images (pixels de tracking, gifs 1px, etc.)
            if img and (len(img) < 20 or "1.gif" in img or "pixel" in img or "blank" in img):
                img = ""
            if img and img.startswith("/"):
                img = "https://www.tourmag.com" + img
        articles.append({"title":title,"link":link,"description":desc,"pub_date":pub_date,"image_url":img,"author":author})
    print(f"HTML fallback : {len(articles)} articles")
    return articles

def parse_rss():
    try:
        r = requests.get(RSS_URL, timeout=30, headers=BROWSER_HEADERS)
        r.raise_for_status()
        raw = r.content
        print(f"RSS : {len(raw)} octets (Content-Type: {r.headers.get('Content-Type','')})")
        if b"<!DOCTYPE" in raw[:500] or b"<html" in raw[:500].lower():
            print("RSS : HTML reçu — fallback scraping")
            return parse_html_fallback(raw)
        if not raw.lstrip()[:5] in (b"<?xml", b"<rss ", b"<feed"):
            return []
        feed = feedparser.parse(raw)
        if not feed.entries and feed.bozo:
            feed = feedparser.parse(clean_xml(raw.decode("utf-8", errors="replace")))
        if not feed.entries: return []
        articles = []
        for e in feed.entries:
            pd = None
            if hasattr(e,"published_parsed") and e.published_parsed: pd = datetime(*e.published_parsed[:6],tzinfo=timezone.utc)
            elif hasattr(e,"updated_parsed") and e.updated_parsed: pd = datetime(*e.updated_parsed[:6],tzinfo=timezone.utc)
            img = ""
            for attr in ["enclosures","media_content","media_thumbnail"]:
                if hasattr(e,attr):
                    for it in (getattr(e,attr) if isinstance(getattr(e,attr),list) else [getattr(e,attr)]):
                        u = it.get("href",it.get("url",""))
                        if u: img=u; break
                if img: break
            articles.append({"title":e.get("title",""),"link":e.get("link",""),"description":e.get("summary",e.get("description","")),"pub_date":pd,"image_url":img,"author":e.get("author","")})
        return articles
    except Exception as ex:
        print(f"ERREUR RSS : {ex}"); return []


# ──────────────────────────────────────────────
# Groq — classification + synthèse
# ──────────────────────────────────────────────
def groq_call(messages, max_tokens=2000):
    if not GROQ_API_KEY:
        print("  Groq : clé API manquante")
        return None
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile", "messages": messages, "max_tokens": max_tokens, "temperature": 0.3},
            timeout=30)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  Groq ERREUR : {e}")
        return None

def classify_articles_groq(articles):
    """Classifie les articles via Groq en une seule requête."""
    cats = "institutionnel, aerien, juridique, solutions, temoignages, contexte"
    items = []
    for i, a in enumerate(articles):
        items.append(f"{i}. {a['title']} — {a.get('description','')[:100]}")

    prompt = f"""Tu es un expert du tourisme français. Classifie chaque article dans UNE seule catégorie parmi : {cats}, general.

Catégories :
- institutionnel : MAE, diplomatie, rapatriements, conseils aux voyageurs, cellule de crise, ressortissants
- aerien : compagnies aériennes, vols, suspensions, reprises, surcharges carburant, aéroports, hubs
- juridique : droits des clients, annulations, remboursements, assurance, force majeure, formalités
- solutions : initiatives des TO, reprogrammations, destinations alternatives, recommandations professionnelles (EDV, SETO)
- temoignages : récits d'agents de voyages, réceptifs sur place, expériences terrain, salons professionnels, vie quotidienne des pros
- contexte : analyses géopolitiques, données économiques, études de marché, tendances, pétrole, devises, intentions de voyage
- general : si aucune catégorie ne correspond

Articles :
{chr(10).join(items)}

Réponds UNIQUEMENT avec un JSON array de cette forme exacte, sans autre texte :
[{{"id":0,"cat":"aerien"}},{{"id":1,"cat":"institutionnel"}}]"""

    result = groq_call([{"role": "user", "content": prompt}])
    if not result:
        return None

    try:
        # Nettoyer la réponse
        clean = result.strip()
        if clean.startswith("```"): clean = re.sub(r'^```\w*\n?', '', clean).rstrip('`').strip()
        classifications = json.loads(clean)
        cat_map = {}
        for c in classifications:
            cat_map[c["id"]] = c["cat"]
        print(f"  Groq classification : {len(cat_map)} articles classifiés")
        return cat_map
    except Exception as e:
        print(f"  Groq parse error : {e}")
        print(f"  Réponse brute : {result[:300]}")
        return None

def generate_synthesis_groq(articles):
    """Génère une synthèse en bullet points via Groq."""
    items = []
    for a in articles[:10]:
        items.append(f"- {a['title']}: {a.get('description','')[:150]}")

    prompt = f"""Tu es un journaliste spécialisé dans le tourisme. À partir de ces articles récents sur la crise au Moyen-Orient, rédige une synthèse en 5 à 6 bullet points pour des agents de voyage français.

Chaque point doit :
- Être concis (1-2 phrases max)
- Donner une information actionnable
- Couvrir un aspect différent (vols, destinations, droits clients, solutions, contexte)

Articles récents :
{chr(10).join(items)}

Réponds UNIQUEMENT avec un JSON array de strings, chaque string étant un bullet point. Pas d'autre texte.
Exemple : ["Point 1...", "Point 2...", "Point 3..."]"""

    result = groq_call([{"role": "user", "content": prompt}])
    if not result:
        return None

    try:
        clean = result.strip()
        if clean.startswith("```"): clean = re.sub(r'^```\w*\n?', '', clean).rstrip('`').strip()
        points = json.loads(clean)
        if isinstance(points, list) and all(isinstance(p, str) for p in points):
            print(f"  Groq synthèse : {len(points)} points")
            return points
    except Exception as e:
        print(f"  Groq synthèse parse error : {e}")
    return None


# ──────────────────────────────────────────────
# Classification fallback (mots-clés)
# ──────────────────────────────────────────────
def classify_keywords(article, keywords_data):
    text = (article["title"] + " " + article.get("description","")).lower()
    scores = {}
    for cat in [k for k in keywords_data if k != "countries_detect"]:
        s = sum(1 for kw in keywords_data[cat]["keywords"] if kw.lower() in text)
        if s > 0: scores[cat] = s
    return max(scores, key=scores.get) if scores else "general"

def detect_countries(article, keywords_data):
    text = (article["title"] + " " + article.get("description","")).lower()
    countries = []
    for ck, ckws in keywords_data.get("countries_detect", {}).items():
        if ck.startswith("_"): continue
        if any(kw.lower() in text for kw in ckws): countries.append(ck)
    # Élargir : si "moyen-orient" ou "golfe" mentionné, ajouter les pays principaux
    if any(kw in text for kw in ["moyen-orient", "moyen orient", "golfe", "conflit iran"]):
        for c in ["iran", "israel", "liban", "irak", "syrie", "emirats"]:
            if c not in countries: countries.append(c)
    return countries


# ──────────────────────────────────────────────
# Finance
# ──────────────────────────────────────────────
def fetch_finance():
    results = {}
    for key, cfg in FINANCE_SYMBOLS.items():
        try:
            h = yf.Ticker(cfg["symbol"]).history(start=CONFLICT_START_DATE)
            if h.empty: continue
            cur = float(h["Close"].iloc[-1]); start = float(h["Close"].iloc[0])
            chg = round(((cur-start)/start)*100, 2)
            hist = [{"date":d.strftime("%Y-%m-%d"),"close":round(float(r["Close"]),2)} for d,r in h.iterrows()]
            fx = cfg["sector"]=="forex"
            results[key] = {"symbol":cfg["symbol"],"label":cfg["label"],"currency":cfg["currency"],
                "sector":cfg["sector"],"current_price":round(cur,4 if fx else 2),
                "start_price":round(start,4 if fx else 2),"change_pct":chg,
                "history":hist,"last_update":datetime.now(timezone.utc).isoformat()}
            print(f"  Finance : {cfg['label']} = {cur:.2f} ({chg:+.2f}%)")
        except Exception as e: print(f"  Finance ERREUR {cfg['symbol']} : {e}")
    return results


# ──────────────────────────────────────────────
# MAE — zones partielles
# ──────────────────────────────────────────────
ALERT_LEVELS = [
    ("formellement déconseillé", "formellement_deconseille", "red"),
    ("déconseillé sauf raison impérative", "deconseille_sauf_ri", "orange"),
    ("déconseillé sauf raison", "deconseille_sauf_ri", "orange"),
    ("vigilance renforcée", "vigilance_renforcee", "yellow"),
    ("vigilance normale", "vigilance_normale", "green"),
]

def scrape_mae():
    results = {}
    for ck, slug in MAE_SLUGS.items():
        url = f"{MAE_BASE}{slug}/"
        try:
            r = requests.get(url, timeout=15, headers=BROWSER_HEADERS)
            if r.status_code != 200:
                results[ck] = _mae_fb(ck, url, f"HTTP {r.status_code}"); continue

            soup = BeautifulSoup(r.content, "html.parser")
            text_lower = soup.get_text().lower()
            text_raw = soup.get_text()

            # Trouver TOUS les niveaux présents
            found = [(lt,cd,co) for lt,cd,co in ALERT_LEVELS if lt in text_lower]

            if found:
                is_partial = len(found) > 1
                if is_partial:
                    # Niveau principal = le MOINS grave (majorité du territoire)
                    main = found[-1]
                    worst = found[0]
                    level_label = f"{main[0].capitalize()} (certaines zones : {worst[0]})"
                    level_code = main[1]
                    level_color = main[2]
                else:
                    main = found[0]
                    level_label = main[0].capitalize()
                    level_code = main[1]
                    level_color = main[2]
                    is_partial = False
            else:
                level_label = "Non déterminé"; level_code = "unknown"; level_color = "gray"; is_partial = False

            # Résumé enrichi
            parts = []
            for p in soup.find_all("p"):
                t = p.get_text(strip=True)
                if len(t) < 20: continue
                if any(k in t.lower() for k in ["déconseillé","vigilance","quitter","se rendre","invités à","recommandé","risque","frappes","prudence"]):
                    parts.append(t)
                    if len(parts) >= 3: break
            summary = " ".join(parts)[:800]
            if not summary:
                meta = soup.find("meta", attrs={"name":"description"})
                if meta:
                    s = meta.get("content","").strip()
                    if s and "ministère" not in s.lower(): summary = s[:500]

            upd = ""
            um = re.search(r'Dernière mise à jour[^\d]*(\d{1,2}\s+\w+\s+\d{4})', text_raw.replace('\n',' '))
            if um: upd = um.group(1).strip()

            results[ck] = {"country":ck,"label":MAE_LABELS.get(ck,ck),"level":level_label,
                "level_code":level_code,"color":level_color,"is_partial":is_partial,
                "summary":summary,"url":url,"last_update_mae":upd,
                "last_scraped":datetime.now(timezone.utc).isoformat()}
            print(f"  MAE {ck} : {level_label}")
        except Exception as e:
            print(f"  MAE ERREUR {ck} : {e}")
            results[ck] = _mae_fb(ck, url, str(e)[:200])
    return results

def _mae_fb(ck, url, msg):
    return {"country":ck,"label":MAE_LABELS.get(ck,ck),"level":"Indisponible",
        "level_code":"unknown","color":"gray","is_partial":False,
        "summary":msg,"url":url,"last_update_mae":"","last_scraped":datetime.now(timezone.utc).isoformat()}


# ──────────────────────────────────────────────
# NOTAM — fermetures d'espace aérien
# ──────────────────────────────────────────────
def fetch_notams():
    """
    Récupère les NOTAM actifs via l'API FAA NOTAM.
    Filtre les fermetures d'espace aérien (type AIRSPACE) pour le Moyen-Orient.
    """
    results = []
    notam_url = "https://external-api.faa.gov/notamapi/v1/notams"

    for fir_code, country_name in NOTAM_FIRS.items():
        try:
            params = {
                "domesticLocation": fir_code,
                "notamType": "N",
                "sortBy": "effectiveStartDate",
                "sortOrder": "DESC",
                "pageSize": 5,
            }
            r = requests.get(notam_url, params=params, timeout=10, headers={
                "Accept": "application/json",
                "User-Agent": "TourMaG-Radar/1.0"
            })

            if r.status_code == 200:
                data = r.json()
                items = data.get("items", [])
                for item in items:
                    props = item.get("properties", {})
                    core = props.get("coreNOTAMData", {}).get("notam", {})
                    text = core.get("text", "")
                    # Filtrer : garder seulement les NOTAM liés aux fermetures/restrictions
                    if any(kw in text.upper() for kw in ["CLSD", "CLOSED", "PROHIB", "RESTRICTED", "DANGER AREA", "NO FLY"]):
                        results.append({
                            "fir": fir_code,
                            "country": country_name,
                            "id": core.get("id", ""),
                            "text": text[:500],
                            "effective": core.get("effectiveStart", ""),
                            "expire": core.get("effectiveEnd", ""),
                            "classification": core.get("classification", ""),
                        })
                if items:
                    filtered = len([x for x in results if x["fir"] == fir_code])
                    print(f"  NOTAM {fir_code} ({country_name}) : {len(items)} trouvés, {filtered} pertinents")
            else:
                print(f"  NOTAM {fir_code} : HTTP {r.status_code}")

        except Exception as e:
            print(f"  NOTAM ERREUR {fir_code} : {e}")

        time.sleep(0.5)  # Rate limiting

    print(f"NOTAM total : {len(results)} restrictions actives")
    return results


# ──────────────────────────────────────────────
# Firestore writes
# ──────────────────────────────────────────────
def gen_id(link): return hashlib.md5(link.encode("utf-8")).hexdigest()[:16]

def sync_articles(db, articles, keywords_data, groq_cats):
    ref = db.collection("articles"); new_count = 0
    for i, a in enumerate(articles):
        if not a["link"]: continue
        did = gen_id(a["link"])
        if ref.document(did).get().exists: continue

        # Classification : Groq si dispo, sinon mots-clés
        if groq_cats and i in groq_cats:
            cat = groq_cats[i]
        else:
            cat = classify_keywords(a, keywords_data)

        countries = detect_countries(a, keywords_data)

        ref.document(did).set({
            "title":a["title"],"link":a["link"],"description":a.get("description",""),
            "image_url":a.get("image_url",""),"author":a.get("author",""),
            "pub_date":a["pub_date"],"category":cat,"countries":countries,
            "created_at":firestore.SERVER_TIMESTAMP})
        new_count += 1
        print(f"  + [{cat}] {a['title'][:60]}...")
    print(f"Articles : {new_count} nouveaux sur {len(articles)}")
    return new_count

def sync_finance(db, data):
    ref = db.collection("market_data")
    for k,d in data.items(): ref.document(k).set(d)
    print(f"Finance : {len(data)} symboles")

def sync_mae(db, data):
    ref = db.collection("mae_alerts")
    for k,d in data.items(): ref.document(k).set(d)
    print(f"MAE : {len(data)} pays")

def sync_notams(db, data):
    db.collection("config").document("notams").set({
        "items": data,
        "count": len(data),
        "last_scraped": datetime.now(timezone.utc).isoformat(),
    })
    print(f"NOTAM : {len(data)} restrictions")

def sync_synthesis(db, points):
    db.collection("config").document("synthesis").set({
        "points": points,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    })
    print(f"Synthèse : {len(points)} points")

def update_config(db, n):
    db.collection("config").document("radar").set({
        "last_sync":datetime.now(timezone.utc).isoformat(),
        "conflict_start_date":CONFLICT_START_DATE,
        "rss_url":RSS_URL,"last_new_articles":n}, merge=True)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    print("="*50)
    print(f"Radar Crise v4 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*50)

    db = init_firebase()
    kw = load_keywords()

    # 1. RSS
    print("\n--- RSS ---")
    articles = parse_rss()

    # 2. Classification Groq
    groq_cats = None
    if articles and GROQ_API_KEY:
        print("\n--- Classification Groq ---")
        groq_cats = classify_articles_groq(articles)

    # 3. Sync articles
    if articles:
        print("\n--- Articles → Firestore ---")
        new_count = sync_articles(db, articles, kw, groq_cats)
    else:
        new_count = 0; print("Aucun article")

    # 4. Synthèse Groq
    if articles and GROQ_API_KEY:
        print("\n--- Synthèse Groq ---")
        points = generate_synthesis_groq(articles)
        if points:
            sync_synthesis(db, points)
        else:
            # Fallback : titres simples
            sync_synthesis(db, [a["title"] for a in articles[:5]])

    # 5. Finance
    print("\n--- Finance ---")
    fd = fetch_finance()
    if fd:
        print("\n--- Finance → Firestore ---")
        sync_finance(db, fd)

    # 6. MAE
    print("\n--- France Diplomatie ---")
    mae = scrape_mae()
    if mae:
        print("\n--- MAE → Firestore ---")
        sync_mae(db, mae)

    # 7. NOTAM
    print("\n--- NOTAM ---")
    notams = fetch_notams()
    sync_notams(db, notams)

    # 8. Config
    update_config(db, new_count)

    print("\n" + "="*50)
    print("Sync terminée")
    print("="*50)

if __name__ == "__main__":
    main()
