#!/usr/bin/env python3
"""Radar Crise Moyen-Orient — v5.2"""

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
MAE_SLUGS = {"israel":"israel-palestine","liban":"liban","iran":"iran","irak":"irak","syrie":"syrie","jordanie":"jordanie","egypte":"egypte","turquie":"turquie","arabie_saoudite":"arabie-saoudite","emirats":"emirats-arabes-unis","qatar":"qatar","oman":"oman","bahrein":"bahrein","koweit":"koweit","yemen":"yemen","chypre":"chypre","grece":"grece"}
MAE_LABELS = {"israel":"Israël / Palestine","liban":"Liban","iran":"Iran","irak":"Irak","syrie":"Syrie","jordanie":"Jordanie","egypte":"Égypte","turquie":"Turquie","arabie_saoudite":"Arabie Saoudite","emirats":"Émirats Arabes Unis","qatar":"Qatar","oman":"Oman","bahrein":"Bahreïn","koweit":"Koweït","yemen":"Yémen","chypre":"Chypre","grece":"Grèce"}
MAE_BASE = "https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/conseils-par-pays-destination/"
ALERT_LEVELS = [("formellement déconseillé","formellement_deconseille","red"),("déconseillé sauf raison impérative","deconseille_sauf_ri","orange"),("déconseillé sauf raison","deconseille_sauf_ri","orange"),("vigilance renforcée","vigilance_renforcee","yellow"),("vigilance normale","vigilance_normale","green")]
MAE_GENERIC = ["urgence attentat","vigilance renforcée pour les ressortissants français à l'étranger","appel à la vigilance maximale"]
BROWSER_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept":"text/html,*/*","Accept-Language":"fr-FR,fr;q=0.9","Accept-Encoding":"gzip, deflate, br"}
KEYWORDS_PATH = Path(__file__).parent / "keywords.json"

def init_firebase():
    sa = os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa: sys.exit("ERREUR: FIREBASE_SERVICE_ACCOUNT manquant")
    firebase_admin.initialize_app(credentials.Certificate(json.loads(sa)))
    return firestore.client()
def load_keywords():
    with open(KEYWORDS_PATH,"r",encoding="utf-8") as f: return json.load(f)
def clean_xml(t):
    t = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]','',t)
    return re.sub(r'&(?!(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);)','&amp;',t)
def valid_image(url):
    if not url or len(url)<20: return ""
    if any(x in url for x in ["1.gif","pixel","blank","spacer"]): return ""
    if url.startswith("/"): return "https://www.tourmag.com"+url
    return url

# ── RSS/HTML ──
def parse_html_fallback(html_bytes):
    soup=BeautifulSoup(html_bytes,"html.parser"); articles=[]
    for div in soup.find_all("div",class_="result"):
        h3=div.find("h3",class_="titre")
        if not h3: continue
        a=h3.find("a",href=True)
        if not a: continue
        title,link=a.get_text(strip=True),a["href"]
        if link.startswith("/"): link="https://www.tourmag.com"+link
        author,pub_date="",None
        rub=div.find("div",class_="rubrique")
        if rub:
            at=rub.find("a",rel="author")
            if at: author=at.get_text(strip=True)
            dm=re.search(r"(\d{2}/\d{2}/\d{4})",rub.get_text())
            if dm:
                try: pub_date=datetime.strptime(dm.group(1),"%d/%m/%Y").replace(tzinfo=timezone.utc)
                except: pass
        desc=""
        td=div.find("div",class_="texte")
        if td and td.find("a"): desc=td.find("a").get_text(strip=True)
        img=valid_image(div.find("img").get("src","") if div.find("img") else "")
        articles.append({"title":title,"link":link,"description":desc,"pub_date":pub_date,"image_url":img,"author":author})
    print(f"HTML fallback : {len(articles)} articles"); return articles

def parse_rss():
    try:
        r=requests.get(RSS_URL,timeout=30,headers=BROWSER_HEADERS); r.raise_for_status(); raw=r.content
        if b"<!DOCTYPE" in raw[:500] or b"<html" in raw[:500].lower(): return parse_html_fallback(raw)
        if not raw.lstrip()[:5] in (b"<?xml",b"<rss ",b"<feed"): return []
        feed=feedparser.parse(raw)
        if not feed.entries and feed.bozo: feed=feedparser.parse(clean_xml(raw.decode("utf-8",errors="replace")))
        if not feed.entries: return []
        articles=[]
        for e in feed.entries:
            pd=None
            if hasattr(e,"published_parsed") and e.published_parsed: pd=datetime(*e.published_parsed[:6],tzinfo=timezone.utc)
            elif hasattr(e,"updated_parsed") and e.updated_parsed: pd=datetime(*e.updated_parsed[:6],tzinfo=timezone.utc)
            img=""
            for attr in ["enclosures","media_content","media_thumbnail"]:
                if hasattr(e,attr):
                    for it in (getattr(e,attr) if isinstance(getattr(e,attr),list) else [getattr(e,attr)]):
                        u=it.get("href",it.get("url",""))
                        if u: img=valid_image(u); break
                if img: break
            articles.append({"title":e.get("title",""),"link":e.get("link",""),"description":e.get("summary",e.get("description","")),"pub_date":pd,"image_url":img,"author":e.get("author","")})
        return articles
    except Exception as ex: print(f"ERREUR RSS : {ex}"); return []

# ── Groq ──
def groq_call(msgs,max_tokens=2000):
    if not GROQ_API_KEY: return None
    try:
        r=requests.post("https://api.groq.com/openai/v1/chat/completions",headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},json={"model":"llama-3.3-70b-versatile","messages":msgs,"max_tokens":max_tokens,"temperature":0.3},timeout=60)
        r.raise_for_status(); return r.json()["choices"][0]["message"]["content"]
    except Exception as e: print(f"  Groq ERREUR : {e}"); return None
def pjson(text):
    if not text: return None
    c=text.strip()
    if c.startswith("```"): c=re.sub(r'^```\w*\n?','',c).rstrip('`').strip()
    try: return json.loads(c)
    except: return None

def classify_articles_groq(articles):
    cats="institutionnel, aerien, croisiere, juridique, solutions, temoignages, contexte, edito"
    items=[f"{i}. {a['title']} — {a.get('description','')[:100]}" for i,a in enumerate(articles)]
    prompt=f"""Classifie chaque article dans UNE catégorie parmi : {cats}, general.

- institutionnel : MAE, diplomatie, rapatriements, conseils voyageurs, cellule de crise
- aerien : compagnies, vols, suspensions, reprises, surcharges carburant, aéroports, hubs, équipages
- croisiere : paquebots, ports, mer Rouge, canal de Suez, itinéraires maritimes
- juridique : droits clients, annulations, remboursements, assurance, force majeure, formalités
- solutions : initiatives TO, reprogrammations, destinations alternatives, recommandations EDV/SETO
- temoignages : récits agents de voyage, réceptifs sur place, salons pros, vie quotidienne des pros, expériences terrain
- contexte : analyses géopolitiques, données économiques, études marché, pétrole, devises, intentions voyage
- edito : éditorial, billet d'humeur, chronique, opinion personnelle, inventaire des peurs/craintes, "même pas peur", "les galères"
- general : si aucune ne correspond

Articles :
{chr(10).join(items)}

Réponds UNIQUEMENT JSON : [{{"id":0,"cat":"aerien"}}]"""
    r=pjson(groq_call([{"role":"user","content":prompt}]))
    if r and isinstance(r,list):
        m={c["id"]:c["cat"] for c in r if "id" in c and "cat" in c}
        print(f"  Groq classification : {len(m)} articles"); return m
    return None

def generate_synthesis_groq(articles):
    items=[f"- {a['title']}: {a.get('description','')[:150]}" for a in articles[:10]]
    prompt=f"""Tu es journaliste tourisme. Rédige 5-6 bullet points synthétiques sur la crise au Moyen-Orient pour des agents de voyage français. Chaque point : 1-2 phrases, info actionnable, aspect différent.
Articles récents :
{chr(10).join(items)}
Réponds UNIQUEMENT JSON array de strings : ["Point 1...","Point 2..."]"""
    r=pjson(groq_call([{"role":"user","content":prompt}]))
    if r and isinstance(r,list): print(f"  Groq synthèse : {len(r)} points"); return r
    return None

def extract_citations_groq(articles):
    items=[f'{i}. "{a["title"]}" — {a.get("description","")[:200]}' for i,a in enumerate(articles)]
    if not items: return None
    prompt=f"""Pour chaque article, génère une citation percutante et réaliste qu'un professionnel du tourisme aurait pu dire. La citation doit être COMPLÈTE (2-3 phrases), pas tronquée. Ton de témoignage direct.
Articles :
{chr(10).join(items)}
Réponds UNIQUEMENT JSON : [{{"id":0,"citation":"La citation complète ici...","auteur_role":"Directrice d'agence, Paris"}}]"""
    r=pjson(groq_call([{"role":"user","content":prompt}]))
    if r and isinstance(r,list):
        m={c["id"]:{"citation":c.get("citation",""),"auteur_role":c.get("auteur_role","")} for c in r if "id" in c}
        print(f"  Groq citations : {len(m)}"); return m
    return None

def reformulate_mae_groq(mae_data):
    items=[]
    for k,v in mae_data.items():
        content=v.get('full_content',v.get('summary',''))[:400]
        items.append(f"- country_key={k} | {v['label']}: niveau={v['level']}. Contenu: {content}")
    prompt=f"""Expert tourisme. Pour chaque pays, rédige une fiche conseil de 2-3 phrases pour un agent de voyage.
Chaque fiche : 1) destination vendable ou à suspendre 2) zones sûres/à éviter si applicable 3) conseil pratique
IMPORTANT: utilise EXACTEMENT la valeur "country_key" comme clé "country" dans ta réponse.
Pays :
{chr(10).join(items)}
Réponds UNIQUEMENT JSON : [{{"country":"liban","conseil_tourisme":"..."}}]"""
    r=pjson(groq_call([{"role":"user","content":prompt}],max_tokens=3000))
    if r and isinstance(r,list):
        m={c["country"]:c.get("conseil_tourisme","") for c in r if "country" in c}
        print(f"  Groq MAE : {len(m)} pays, matched={len(set(m.keys())&set(mae_data.keys()))}"); return m
    return None

# ── Classification/détection ──
def detect_countries(article,kw):
    text=(article["title"]+" "+article.get("description","")).lower()
    countries=[]
    for ck,ckws in kw.get("countries_detect",{}).items():
        if ck.startswith("_"): continue
        if any(k.lower() in text for k in ckws): countries.append(ck)
    return countries
def classify_keywords(article,kw):
    text=(article["title"]+" "+article.get("description","")).lower()
    scores={}
    for cat in [k for k in kw if k!="countries_detect"]:
        s=sum(1 for k2 in kw[cat]["keywords"] if k2.lower() in text)
        if s>0: scores[cat]=s
    return max(scores,key=scores.get) if scores else "general"

# ── Finance ──
def fetch_finance():
    results={}
    for key,cfg in FINANCE_SYMBOLS.items():
        try:
            h=yf.Ticker(cfg["symbol"]).history(start=CONFLICT_START_DATE)
            if h.empty: continue
            cur,start=float(h["Close"].iloc[-1]),float(h["Close"].iloc[0])
            chg=round(((cur-start)/start)*100,2)
            hist=[{"date":d.strftime("%Y-%m-%d"),"close":round(float(r["Close"]),2)} for d,r in h.iterrows()]
            fx=cfg["sector"]=="forex"
            results[key]={"symbol":cfg["symbol"],"label":cfg["label"],"currency":cfg["currency"],"sector":cfg["sector"],"current_price":round(cur,4 if fx else 2),"start_price":round(start,4 if fx else 2),"change_pct":chg,"history":hist,"last_update":datetime.now(timezone.utc).isoformat()}
            print(f"  Finance : {cfg['label']} = {cur:.2f} ({chg:+.2f}%)")
        except Exception as e: print(f"  Finance ERREUR {cfg['symbol']} : {e}")
    return results

# ── MAE — FIXED: partial zones show least severe as main ──
def scrape_mae():
    results={}
    for ck,slug in MAE_SLUGS.items():
        url=f"{MAE_BASE}{slug}/"
        try:
            r=requests.get(url,timeout=15,headers=BROWSER_HEADERS)
            if r.status_code!=200: results[ck]=_mae_fb(ck,url,f"HTTP {r.status_code}"); continue
            soup=BeautifulSoup(r.content,"html.parser")
            all_paras=[p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True))>=15]
            relevant=[]
            for t in all_paras:
                tl=t.lower()
                if any(k in tl for k in ["déconseillé","vigilance","quitter","se rendre","invités à","recommandé","risque","frappes","prudence","sécurité","visa","passeport","entrée","séjour","ambassade","consulat","zone","éviter","déplacement","transport","frontière","assurance","santé","aéroport"]):
                    if not any(g in tl for g in MAE_GENERIC): relevant.append(t)
            relevant_text=" ".join(relevant).lower()
            found=[(lt,cd,co) for lt,cd,co in ALERT_LEVELS if lt in relevant_text]

            if found:
                is_partial=len(found)>1
                if is_partial:
                    # LEAST severe = main level (applies to most of territory)
                    # MOST severe = restricted zones
                    least=found[-1]; worst=found[0]
                    level_label=f"{least[0].capitalize()} (certaines zones : {worst[0]})"
                    level_code,level_color=least[1],least[2]
                else:
                    main=found[0]; is_partial=False
                    level_label,level_code,level_color=main[0].capitalize(),main[1],main[2]
            else:
                level_label,level_code,level_color,is_partial="Non déterminé","unknown","gray",False

            full_content=" ".join(relevant)[:1500]
            short_summary=" ".join(relevant[:3])[:500]
            if not short_summary:
                meta=soup.find("meta",attrs={"name":"description"})
                if meta:
                    s=meta.get("content","").strip()
                    if s and "ministère" not in s.lower(): short_summary=s[:500]
            text_raw=soup.get_text()
            upd=""
            um=re.search(r'Dernière mise à jour[^\d]*(\d{1,2}\s+\w+\s+\d{4})',text_raw.replace('\n',' '))
            if um: upd=um.group(1).strip()
            results[ck]={"country":ck,"label":MAE_LABELS.get(ck,ck),"level":level_label,"level_code":level_code,"color":level_color,"is_partial":is_partial,"summary":short_summary,"full_content":full_content,"url":url,"last_update_mae":upd,"conseil_tourisme":"","last_scraped":datetime.now(timezone.utc).isoformat()}
            print(f"  MAE {ck} : {level_label}")
        except Exception as e: print(f"  MAE ERREUR {ck} : {e}"); results[ck]=_mae_fb(ck,url,str(e)[:200])
    return results
def _mae_fb(ck,url,msg):
    return {"country":ck,"label":MAE_LABELS.get(ck,ck),"level":"Indisponible","level_code":"unknown","color":"gray","is_partial":False,"summary":msg,"full_content":"","url":url,"last_update_mae":"","conseil_tourisme":"","last_scraped":datetime.now(timezone.utc).isoformat()}

# ── Firestore ──
def gen_id(link): return hashlib.md5(link.encode("utf-8")).hexdigest()[:16]
def sync_articles(db,articles,kw,groq_cats,cit_map):
    ref=db.collection("articles"); n=0
    for i,a in enumerate(articles):
        if not a["link"]: continue
        did=gen_id(a["link"])
        if ref.document(did).get().exists: continue
        cat=groq_cats[i] if groq_cats and i in groq_cats else classify_keywords(a,kw)
        countries=detect_countries(a,kw)
        doc={"title":a["title"],"link":a["link"],"description":a.get("description",""),"image_url":a.get("image_url",""),"author":a.get("author",""),"pub_date":a["pub_date"],"category":cat,"countries":countries,"created_at":firestore.SERVER_TIMESTAMP}
        if cit_map and i in cit_map:
            doc["citation"]=cit_map[i].get("citation","")
            doc["citation_role"]=cit_map[i].get("auteur_role","")
        ref.document(did).set(doc); n+=1
        print(f"  + [{cat}] {a['title'][:60]}...")
    print(f"Articles : {n} nouveaux sur {len(articles)}"); return n

def sync_finance(db,data):
    for k,d in data.items(): db.collection("market_data").document(k).set(d)
def sync_mae(db,data,existing_mae):
    """Write MAE data, but PROTECT existing conseil_tourisme if new one is empty."""
    for k,d in data.items():
        if not d.get("conseil_tourisme") and existing_mae.get(k,{}).get("conseil_tourisme"):
            d["conseil_tourisme"]=existing_mae[k]["conseil_tourisme"]
            print(f"  MAE {k} : conseil_tourisme préservé (nouveau vide)")
        db.collection("mae_alerts").document(k).set(d)
def sync_synthesis(db,pts):
    db.collection("config").document("synthesis").set({"points":pts,"generated_at":datetime.now(timezone.utc).isoformat()})
def update_config(db,n):
    db.collection("config").document("radar").set({"last_sync":datetime.now(timezone.utc).isoformat(),"conflict_start_date":CONFLICT_START_DATE,"rss_url":RSS_URL,"last_new_articles":n},merge=True)

# ── Main ──
def main():
    print("="*50+f"\nRadar Crise v5.2 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"+"="*50)
    db=init_firebase(); kw=load_keywords()

    # Load existing MAE for conseil protection
    print("\n--- Loading existing MAE ---")
    existing_mae={}
    try:
        for doc in db.collection("mae_alerts").stream():
            existing_mae[doc.id]=doc.to_dict()
        print(f"  {len(existing_mae)} pays existants chargés")
    except: pass

    print("\n--- RSS ---")
    articles=parse_rss()
    groq_cats=None
    if articles and GROQ_API_KEY:
        print("\n--- Classification Groq ---"); groq_cats=classify_articles_groq(articles)
    cit_map=None
    if articles and GROQ_API_KEY and groq_cats:
        temo=[(i,a) for i,a in enumerate(articles) if groq_cats.get(i)=="temoignages"]
        if temo:
            print("\n--- Citations Groq ---")
            raw=extract_citations_groq([a for _,a in temo])
            if raw:
                cit_map={}
                for li,gi in enumerate([i for i,_ in temo]):
                    if li in raw: cit_map[gi]=raw[li]
    if articles:
        print("\n--- Articles → Firestore ---"); n=sync_articles(db,articles,kw,groq_cats,cit_map)
    else: n=0
    if articles and GROQ_API_KEY:
        print("\n--- Synthèse Groq ---")
        pts=generate_synthesis_groq(articles)
        sync_synthesis(db,pts if pts else [a["title"] for a in articles[:5]])
    print("\n--- Finance ---")
    fd=fetch_finance()
    if fd: sync_finance(db,fd)
    print("\n--- France Diplomatie ---")
    mae=scrape_mae()
    if mae and GROQ_API_KEY:
        print("\n--- Reformulation MAE Groq ---")
        conseils=reformulate_mae_groq(mae)
        if conseils:
            for ck,conseil in conseils.items():
                if ck in mae and conseil:
                    mae[ck]["conseil_tourisme"]=conseil
                    print(f"  ✓ {ck} : {conseil[:60]}...")
    if mae: sync_mae(db,mae,existing_mae)
    update_config(db,n)
    print("\n"+"="*50+"\nSync terminée\n"+"="*50)

if __name__=="__main__": main()
