#!/usr/bin/env python3
"""
Radar Crise Moyen-Orient — v5.1
Fixes: MAE level detection, Groq country key mapping, enriched MAE content
"""

import json, hashlib, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path

import feedparser, requests, yfinance as yf
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore

RSS_URL = os.getenv("RSS_URL", "https://www.tourmag.com/xml/syndication.rss?t=crise+golfe")
CONFLICT_START_DATE = os.getenv("CONFLICT_START_DATE", "2025-10-01")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

FINANCE_SYMBOLS = {
    "brent":{"symbol":"BZ=F","label":"Brent (baril)","currency":"$","sector":"commodity"},
    "eurusd":{"symbol":"EURUSD=X","label":"EUR / USD","currency":"","sector":"forex"},
    "AF.PA":{"symbol":"AF.PA","label":"Air France-KLM","currency":"€","sector":"aerien"},
    "TUI1.DE":{"symbol":"TUI1.DE","label":"TUI Group","currency":"€","sector":"to"},
    "AC.PA":{"symbol":"AC.PA","label":"Accor","currency":"€","sector":"hotellerie"},
    "BKNG":{"symbol":"BKNG","label":"Booking Holdings","currency":"$","sector":"ota"},
    "CCL":{"symbol":"CCL","label":"Carnival Corp","currency":"$","sector":"croisiere"},
    "AMS.MC":{"symbol":"AMS.MC","label":"Amadeus IT","currency":"€","sector":"tech"},
    "AIR.PA":{"symbol":"AIR.PA","label":"Airbus","currency":"€","sector":"aerien"},
    "RYA.IR":{"symbol":"RYA.IR","label":"Ryanair","currency":"€","sector":"aerien"},
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

ALERT_LEVELS = [
    ("formellement déconseillé", "formellement_deconseille", "red"),
    ("déconseillé sauf raison impérative", "deconseille_sauf_ri", "orange"),
    ("déconseillé sauf raison", "deconseille_sauf_ri", "orange"),
    ("vigilance renforcée", "vigilance_renforcee", "yellow"),
    ("vigilance normale", "vigilance_normale", "green"),
]

# Textes génériques MAE à ignorer lors de la détection de niveau
MAE_GENERIC_TEXTS = [
    "urgence attentat",
    "vigilance renforcée pour les ressortissants français à l'étranger",
    "appel à la vigilance maximale",
]

BROWSER_HEADERS = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language":"fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding":"gzip, deflate, br",
}
KEYWORDS_PATH = Path(__file__).parent / "keywords.json"

# ── Firebase ──
def init_firebase():
    sa = os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa: print("ERREUR: FIREBASE_SERVICE_ACCOUNT manquant"); sys.exit(1)
    firebase_admin.initialize_app(credentials.Certificate(json.loads(sa)))
    return firestore.client()

def load_keywords():
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f: return json.load(f)

# ── RSS/HTML ──
def clean_xml(t):
    t = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', t)
    return re.sub(r'&(?!(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);)', '&amp;', t)

def valid_image(url):
    if not url or len(url) < 20: return ""
    if any(x in url for x in ["1.gif","pixel","blank","spacer"]): return ""
    if url.startswith("/"): return "https://www.tourmag.com" + url
    return url

def parse_html_fallback(html_bytes):
    soup = BeautifulSoup(html_bytes, "html.parser")
    articles = []
    for div in soup.find_all("div", class_="result"):
        h3 = div.find("h3", class_="titre")
        if not h3: continue
        a = h3.find("a", href=True)
        if not a: continue
        title, link = a.get_text(strip=True), a["href"]
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
        if td and td.find("a"): desc = td.find("a").get_text(strip=True)
        img = valid_image(div.find("img").get("src","") if div.find("img") else "")
        articles.append({"title":title,"link":link,"description":desc,"pub_date":pub_date,"image_url":img,"author":author})
    print(f"HTML fallback : {len(articles)} articles")
    return articles

def parse_rss():
    try:
        r = requests.get(RSS_URL, timeout=30, headers=BROWSER_HEADERS); r.raise_for_status()
        raw = r.content
        if b"<!DOCTYPE" in raw[:500] or b"<html" in raw[:500].lower():
            return parse_html_fallback(raw)
        if not raw.lstrip()[:5] in (b"<?xml",b"<rss ",b"<feed"): return []
        feed = feedparser.parse(raw)
        if not feed.entries and feed.bozo:
            feed = feedparser.parse(clean_xml(raw.decode("utf-8",errors="replace")))
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
                        if u: img=valid_image(u); break
                if img: break
            articles.append({"title":e.get("title",""),"link":e.get("link",""),"description":e.get("summary",e.get("description","")),"pub_date":pd,"image_url":img,"author":e.get("author","")})
        return articles
    except Exception as ex:
        print(f"ERREUR RSS : {ex}"); return []

# ── Groq ──
def groq_call(messages, max_tokens=2000):
    if not GROQ_API_KEY: return None
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
            json={"model":"llama-3.3-70b-versatile","messages":messages,"max_tokens":max_tokens,"temperature":0.3},
            timeout=60)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  Groq ERREUR : {e}"); return None

def parse_json_response(text):
    if not text: return None
    clean = text.strip()
    if clean.startswith("```"): clean = re.sub(r'^```\w*\n?','',clean).rstrip('`').strip()
    try: return json.loads(clean)
    except: return None

# ── Groq classification ──
def classify_articles_groq(articles):
    cats = "institutionnel, aerien, croisiere, juridique, solutions, temoignages, contexte, edito"
    items = [f"{i}. {a['title']} — {a.get('description','')[:100]}" for i,a in enumerate(articles)]
    prompt = f"""Classifie chaque article dans UNE catégorie parmi : {cats}, general.

- institutionnel : MAE, diplomatie, rapatriements, conseils voyageurs, cellule de crise
- aerien : compagnies, vols, suspensions, reprises, surcharges carburant, aéroports, hubs, équipages
- croisiere : croisiéristes, paquebots, ports, mer Rouge, canal de Suez, itinéraires maritimes
- juridique : droits clients, annulations, remboursements, assurance, force majeure, formalités, règles
- solutions : initiatives TO, reprogrammations, destinations alternatives, recommandations EDV/SETO
- temoignages : récits agents de voyage, réceptifs sur place, expériences terrain, salons pros, vie quotidienne des pros
- contexte : analyses géopolitiques, données économiques, études marché, pétrole, devises, intentions voyage
- edito : éditorial, billet d'humeur, chronique, opinion de la rédaction
- general : si aucune ne correspond

Articles :
{chr(10).join(items)}

Réponds UNIQUEMENT JSON : [{{"id":0,"cat":"aerien"}},{{"id":1,"cat":"institutionnel"}}]"""
    result = parse_json_response(groq_call([{"role":"user","content":prompt}]))
    if result and isinstance(result,list):
        m = {c["id"]:c["cat"] for c in result if "id" in c and "cat" in c}
        print(f"  Groq classification : {len(m)} articles"); return m
    return None

# ── Groq synthèse ──
def generate_synthesis_groq(articles):
    items = [f"- {a['title']}: {a.get('description','')[:150]}" for a in articles[:10]]
    prompt = f"""Tu es journaliste tourisme. Rédige 5-6 bullet points synthétiques sur la crise au Moyen-Orient pour des agents de voyage français. Chaque point : 1-2 phrases, info actionnable, aspect différent.

Articles récents :
{chr(10).join(items)}

Réponds UNIQUEMENT JSON array de strings : ["Point 1...", "Point 2..."]"""
    result = parse_json_response(groq_call([{"role":"user","content":prompt}]))
    if result and isinstance(result,list):
        print(f"  Groq synthèse : {len(result)} points"); return result
    return None

# ── Groq citations témoignages ──
def extract_citations_groq(articles):
    items = [f'{i}. "{a["title"]}" — {a.get("description","")[:200]}' for i,a in enumerate(articles)]
    if not items: return None
    prompt = f"""Pour chaque article ci-dessous, invente une citation percutante et réaliste qu'un professionnel du tourisme aurait pu dire, en cohérence avec le sujet. 1-2 phrases, ton de témoignage direct.

Articles :
{chr(10).join(items)}

Réponds UNIQUEMENT JSON : [{{"id":0,"citation":"La citation...","auteur_role":"Directrice d'agence, Paris"}}]"""
    result = parse_json_response(groq_call([{"role":"user","content":prompt}]))
    if result and isinstance(result,list):
        m = {c["id"]:{"citation":c.get("citation",""),"auteur_role":c.get("auteur_role","")} for c in result if "id" in c}
        print(f"  Groq citations : {len(m)} extraites"); return m
    return None

# ── Groq MAE tourisme — FIXED: uses country keys, not labels ──
def reformulate_mae_groq(mae_data):
    items = []
    for k, v in mae_data.items():
        content = v.get('full_content', v.get('summary',''))[:400]
        # IMPORTANT: on passe la clé k (ex: "liban") ET le label pour que Groq retourne la clé
        items.append(f"- country_key={k} | {v['label']}: niveau={v['level']}. Contenu: {content}")

    prompt = f"""Tu es expert tourisme. Pour chaque pays, rédige une fiche conseil de 2-3 phrases pour un agent de voyage.

Chaque fiche : 1) destination vendable ou à suspendre 2) zones sûres/à éviter 3) conseil pratique (visa, précautions, alternatives)

IMPORTANT: dans ta réponse, utilise EXACTEMENT la valeur "country_key" fournie comme clé "country".

Pays :
{chr(10).join(items)}

Réponds UNIQUEMENT JSON : [{{"country":"liban","conseil_tourisme":"Le Liban est à suspendre..."}}]"""

    result = parse_json_response(groq_call([{"role":"user","content":prompt}], max_tokens=3000))
    if result and isinstance(result,list):
        m = {c["country"]:c.get("conseil_tourisme","") for c in result if "country" in c}
        print(f"  Groq MAE tourisme : {len(m)} pays")
        # Debug: afficher les clés retournées vs attendues
        mae_keys = set(mae_data.keys())
        groq_keys = set(m.keys())
        matched = mae_keys & groq_keys
        unmatched = groq_keys - mae_keys
        print(f"  Groq MAE matched: {len(matched)}, unmatched: {unmatched}")
        return m
    return None

# ── Country detection ──
def detect_countries(article, keywords_data):
    text = (article["title"] + " " + article.get("description","")).lower()
    countries = []
    for ck, ckws in keywords_data.get("countries_detect",{}).items():
        if ck.startswith("_"): continue
        if any(kw.lower() in text for kw in ckws): countries.append(ck)
    return countries

def classify_keywords(article, keywords_data):
    text = (article["title"] + " " + article.get("description","")).lower()
    scores = {}
    for cat in [k for k in keywords_data if k != "countries_detect"]:
        s = sum(1 for kw in keywords_data[cat]["keywords"] if kw.lower() in text)
        if s > 0: scores[cat] = s
    return max(scores, key=scores.get) if scores else "general"

# ── Finance ──
def fetch_finance():
    results = {}
    for key, cfg in FINANCE_SYMBOLS.items():
        try:
            h = yf.Ticker(cfg["symbol"]).history(start=CONFLICT_START_DATE)
            if h.empty: continue
            cur,start = float(h["Close"].iloc[-1]),float(h["Close"].iloc[0])
            chg = round(((cur-start)/start)*100,2)
            hist = [{"date":d.strftime("%Y-%m-%d"),"close":round(float(r["Close"]),2)} for d,r in h.iterrows()]
            fx = cfg["sector"]=="forex"
            results[key] = {"symbol":cfg["symbol"],"label":cfg["label"],"currency":cfg["currency"],
                "sector":cfg["sector"],"current_price":round(cur,4 if fx else 2),
                "start_price":round(start,4 if fx else 2),"change_pct":chg,
                "history":hist,"last_update":datetime.now(timezone.utc).isoformat()}
            print(f"  Finance : {cfg['label']} = {cur:.2f} ({chg:+.2f}%)")
        except Exception as e: print(f"  Finance ERREUR {cfg['symbol']} : {e}")
    return results

# ── MAE scraping — FIXED: level detection on first paragraphs only ──
def scrape_mae():
    results = {}
    for ck, slug in MAE_SLUGS.items():
        url = f"{MAE_BASE}{slug}/"
        try:
            r = requests.get(url, timeout=15, headers=BROWSER_HEADERS)
            if r.status_code != 200:
                results[ck] = _mae_fb(ck,url,f"HTTP {r.status_code}"); continue
            soup = BeautifulSoup(r.content, "html.parser")

            # Extraire les paragraphes pertinents
            all_paras = []
            for p in soup.find_all("p"):
                t = p.get_text(strip=True)
                if len(t) >= 15: all_paras.append(t)

            # NIVEAU D'ALERTE: chercher UNIQUEMENT dans les 15 premiers paragraphes
            # et EXCLURE les textes génériques (bandeau "Urgence Attentat")
            early_text = " ".join(all_paras[:15]).lower()
            # Retirer les phrases génériques
            for generic in MAE_GENERIC_TEXTS:
                early_text = early_text.replace(generic, "")

            found = []
            for lt, cd, co in ALERT_LEVELS:
                if lt in early_text:
                    found.append((lt, cd, co))

            if found:
                is_partial = len(found) > 1
                if is_partial:
                    # Le PREMIER trouvé dans les premiers paragraphes = le niveau principal
                    main = found[0]
                    other = found[-1]
                    level_label = f"{main[0].capitalize()} (certaines zones : {other[0]})"
                    level_code, level_color = main[1], main[2]
                else:
                    main = found[0]; is_partial = False
                    level_label, level_code, level_color = main[0].capitalize(), main[1], main[2]
            else:
                level_label, level_code, level_color, is_partial = "Non déterminé", "unknown", "gray", False

            # Contenu enrichi pour Groq
            relevant = []
            for t in all_paras:
                tl = t.lower()
                if any(k in tl for k in [
                    "déconseillé","vigilance","quitter","se rendre","invités à",
                    "recommandé","risque","frappes","prudence","sécurité",
                    "visa","passeport","entrée","séjour","ambassade","consulat",
                    "zone","éviter","déplacement","transport","frontière",
                    "assurance","santé","aéroport"
                ]):
                    # Exclure les textes génériques
                    if not any(g in tl for g in MAE_GENERIC_TEXTS):
                        relevant.append(t)

            full_content = " ".join(relevant)[:1500]
            short_summary = " ".join(relevant[:3])[:500]
            if not short_summary:
                meta = soup.find("meta",attrs={"name":"description"})
                if meta:
                    s = meta.get("content","").strip()
                    if s and "ministère" not in s.lower(): short_summary = s[:500]

            text_raw = soup.get_text()
            upd = ""
            um = re.search(r'Dernière mise à jour[^\d]*(\d{1,2}\s+\w+\s+\d{4})',text_raw.replace('\n',' '))
            if um: upd = um.group(1).strip()

            results[ck] = {"country":ck,"label":MAE_LABELS.get(ck,ck),"level":level_label,
                "level_code":level_code,"color":level_color,"is_partial":is_partial,
                "summary":short_summary,"full_content":full_content,
                "url":url,"last_update_mae":upd,"conseil_tourisme":"",
                "last_scraped":datetime.now(timezone.utc).isoformat()}
            print(f"  MAE {ck} : {level_label}")
        except Exception as e:
            print(f"  MAE ERREUR {ck} : {e}")
            results[ck] = _mae_fb(ck,url,str(e)[:200])
    return results

def _mae_fb(ck,url,msg):
    return {"country":ck,"label":MAE_LABELS.get(ck,ck),"level":"Indisponible",
        "level_code":"unknown","color":"gray","is_partial":False,
        "summary":msg,"full_content":"","url":url,"last_update_mae":"","conseil_tourisme":"",
        "last_scraped":datetime.now(timezone.utc).isoformat()}

# ── Firestore ──
def gen_id(link): return hashlib.md5(link.encode("utf-8")).hexdigest()[:16]

def sync_articles(db, articles, keywords_data, groq_cats, citations_map):
    ref = db.collection("articles"); new_count = 0
    for i, a in enumerate(articles):
        if not a["link"]: continue
        did = gen_id(a["link"])
        if ref.document(did).get().exists: continue
        cat = groq_cats[i] if groq_cats and i in groq_cats else classify_keywords(a, keywords_data)
        countries = detect_countries(a, keywords_data)
        doc = {"title":a["title"],"link":a["link"],"description":a.get("description",""),
            "image_url":a.get("image_url",""),"author":a.get("author",""),
            "pub_date":a["pub_date"],"category":cat,"countries":countries,
            "created_at":firestore.SERVER_TIMESTAMP}
        if citations_map and i in citations_map:
            doc["citation"] = citations_map[i].get("citation","")
            doc["citation_role"] = citations_map[i].get("auteur_role","")
        ref.document(did).set(doc)
        new_count += 1
        print(f"  + [{cat}] {a['title'][:60]}...")
    print(f"Articles : {new_count} nouveaux sur {len(articles)}")
    return new_count

def sync_finance(db, data):
    for k,d in data.items(): db.collection("market_data").document(k).set(d)
def sync_mae(db, data):
    for k,d in data.items(): db.collection("mae_alerts").document(k).set(d)
def sync_synthesis(db, pts):
    db.collection("config").document("synthesis").set({"points":pts,"generated_at":datetime.now(timezone.utc).isoformat()})
def update_config(db, n):
    db.collection("config").document("radar").set({"last_sync":datetime.now(timezone.utc).isoformat(),
        "conflict_start_date":CONFLICT_START_DATE,"rss_url":RSS_URL,"last_new_articles":n},merge=True)

# ── Main ──
def main():
    print("="*50)
    print(f"Radar Crise v5.1 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*50)
    db = init_firebase(); kw = load_keywords()

    print("\n--- RSS ---")
    articles = parse_rss()

    groq_cats = None
    if articles and GROQ_API_KEY:
        print("\n--- Classification Groq ---")
        groq_cats = classify_articles_groq(articles)

    citations_map = None
    if articles and GROQ_API_KEY and groq_cats:
        temo = [(i,a) for i,a in enumerate(articles) if groq_cats.get(i)=="temoignages"]
        if temo:
            print("\n--- Citations Groq ---")
            raw = extract_citations_groq([a for _,a in temo])
            if raw:
                citations_map = {}
                for li,gi in enumerate([i for i,_ in temo]):
                    if li in raw: citations_map[gi] = raw[li]

    if articles:
        print("\n--- Articles → Firestore ---")
        new_count = sync_articles(db, articles, kw, groq_cats, citations_map)
    else: new_count = 0

    if articles and GROQ_API_KEY:
        print("\n--- Synthèse Groq ---")
        pts = generate_synthesis_groq(articles)
        if pts: sync_synthesis(db, pts)
        else: sync_synthesis(db, [a["title"] for a in articles[:5]])

    print("\n--- Finance ---")
    fd = fetch_finance()
    if fd: sync_finance(db, fd)

    print("\n--- France Diplomatie ---")
    mae = scrape_mae()

    if mae and GROQ_API_KEY:
        print("\n--- Reformulation MAE Groq ---")
        conseils = reformulate_mae_groq(mae)
        if conseils:
            for ck, conseil in conseils.items():
                if ck in mae:
                    mae[ck]["conseil_tourisme"] = conseil
                    print(f"  ✓ {ck} : {conseil[:60]}...")

    if mae: sync_mae(db, mae)
    update_config(db, new_count)
    print("\n" + "="*50 + "\nSync terminée\n" + "="*50)

if __name__ == "__main__": main()
