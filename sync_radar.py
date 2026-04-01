import sys; print("SCRIPT DÉMARRÉ", flush=True); sys.stdout.flush()

#!/usr/bin/env python3
"""Radar Crise Moyen-Orient — v6.1"""
print("1-json", flush=True)
import json,hashlib,os,re,sys,time
print("2-datetime", flush=True)
from datetime import datetime,timezone
from pathlib import Path
print("3-feedparser", flush=True)
import feedparser
print("4-requests", flush=True)
import requests
print("5-yfinance", flush=True)
import yfinance as yf
print("6-bs4", flush=True)
from bs4 import BeautifulSoup
print("7-firebase", flush=True)
import firebase_admin
from firebase_admin import credentials,firestore
print("IMPORTS OK", flush=True)

#!/usr/bin/env python3
"""Radar Crise Moyen-Orient — v6.1
Basé sur sync_radar (24).py + fix rate limit Groq (pauses + retry 429)
"""
import json,hashlib,os,re,sys,time
from datetime import datetime,timezone
from pathlib import Path
import feedparser,requests,yfinance as yf
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials,firestore

RSS_URL=os.getenv("RSS_URL","https://www.tourmag.com/xml/syndication.rss?t=crise+golfe")
CONFLICT_START_DATE=os.getenv("CONFLICT_START_DATE","2025-10-01")
GROQ_API_KEY=os.getenv("GROQ_API_KEY","")
AVIATIONSTACK_API_KEY=os.getenv("AVIATIONSTACK_API_KEY","")
# Aéroports Moyen-Orient à surveiller dans les vols au départ de CDG
ME_AIRPORTS={"BEY":"Beyrouth","TLV":"Tel-Aviv","THR":"Téhéran","IKA":"Téhéran (Imam Khomeini)","AMM":"Amman","CAI":"Le Caire","IST":"Istanbul","DXB":"Dubaï","DOH":"Doha","RUH":"Riyad","JED":"Djeddah","MCT":"Mascate","BAH":"Bahreïn","KWI":"Koweït","AUH":"Abu Dhabi","SSH":"Charm el-Cheikh","HRG":"Hurghada","LCA":"Larnaca","AYT":"Antalya","BGW":"Bagdad","DAM":"Damas","SAH":"Sanaa"}
FINANCE_SYMBOLS={"brent":{"symbol":"BZ=F","label":"Brent (baril)","currency":"$","sector":"commodity"},"eurusd":{"symbol":"EURUSD=X","label":"EUR / USD","currency":"","sector":"forex"},"AF.PA":{"symbol":"AF.PA","label":"Air France-KLM","currency":"€","sector":"aerien"},"TUI1.DE":{"symbol":"TUI1.DE","label":"TUI Group","currency":"€","sector":"to"},"AC.PA":{"symbol":"AC.PA","label":"Accor","currency":"€","sector":"hotellerie"},"BKNG":{"symbol":"BKNG","label":"Booking Holdings","currency":"$","sector":"ota"},"CCL":{"symbol":"CCL","label":"Carnival Corp","currency":"$","sector":"croisiere"},"AMS.MC":{"symbol":"AMS.MC","label":"Amadeus IT","currency":"€","sector":"tech"},"AIR.PA":{"symbol":"AIR.PA","label":"Airbus","currency":"€","sector":"aerien"},"RYA.IR":{"symbol":"RYA.IR","label":"Ryanair","currency":"€","sector":"aerien"},"IAG.L":{"symbol":"IAG.L","label":"IAG (British Airways)","currency":"£","sector":"aerien"},"LHA.DE":{"symbol":"LHA.DE","label":"Lufthansa","currency":"€","sector":"aerien"},"EXPE":{"symbol":"EXPE","label":"Expedia","currency":"$","sector":"ota"},"MAR":{"symbol":"MAR","label":"Marriott","currency":"$","sector":"hotellerie"},"RCL":{"symbol":"RCL","label":"Royal Caribbean","currency":"$","sector":"croisiere"},"HLT":{"symbol":"HLT","label":"Hilton","currency":"$","sector":"hotellerie"},"GC=F":{"symbol":"GC=F","label":"Or (once)","currency":"$","sector":"commodity"}}
MAE_SLUGS={"israel":"israel-palestine","liban":"liban","iran":"iran","irak":"irak","syrie":"syrie","jordanie":"jordanie","egypte":"egypte","turquie":"turquie","arabie_saoudite":"arabie-saoudite","emirats":"emirats-arabes-unis","qatar":"qatar","oman":"oman","bahrein":"bahrein","koweit":"koweit","yemen":"yemen","chypre":"chypre","grece":"grece"}
MAE_LABELS={"israel":"Israël / Palestine","liban":"Liban","iran":"Iran","irak":"Irak","syrie":"Syrie","jordanie":"Jordanie","egypte":"Égypte","turquie":"Turquie","arabie_saoudite":"Arabie Saoudite","emirats":"Émirats Arabes Unis","qatar":"Qatar","oman":"Oman","bahrein":"Bahreïn","koweit":"Koweït","yemen":"Yémen","chypre":"Chypre","grece":"Grèce"}
MAE_BASE="https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/conseils-par-pays-destination/"
ALERT_LEVELS=[("formellement déconseillé","formellement_deconseille","red"),("déconseillé sauf raison impérative","deconseille_sauf_ri","orange"),("déconseillé sauf raison","deconseille_sauf_ri","orange"),("vigilance renforcée","vigilance_renforcee","yellow"),("vigilance normale","vigilance_normale","green")]
MAE_GENERIC=["urgence attentat","vigilance renforcée pour les ressortissants français à l'étranger","appel à la vigilance maximale"]
HDR={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept":"text/html,*/*","Accept-Language":"fr-FR,fr;q=0.9"}
KEYWORDS_PATH=Path(__file__).parent/"keywords.json"

# ── Pauses Groq (secondes) ──
GROQ_PAUSE_BETWEEN_BLOCKS = 30     # Pause entre chaque gros bloc Groq
GROQ_PAUSE_RETRY = 45              # Pause avant retry après erreur 429

EDITO_TAGS=["expert","spokojny","guena","remi duchange","futuroscopie","eric didier","mazzola"]

def init_fb():
    sa=os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa: sys.exit("ERREUR: FIREBASE_SERVICE_ACCOUNT")
    firebase_admin.initialize_app(credentials.Certificate(json.loads(sa))); return firestore.client()
def load_kw():
    with open(KEYWORDS_PATH,"r",encoding="utf-8") as f: return json.load(f)

# ── Scraping tags TourMaG ──
def scrape_tags(url):
    try:
        r=requests.get(url,timeout=15,headers=HDR)
        if r.status_code!=200: return []
        soup=BeautifulSoup(r.content,"html.parser")
        tags=[]
        tags_section=None
        for el in soup.find_all(string=re.compile(r'Tags?\s*:', re.IGNORECASE)):
            parent=el.find_parent()
            if parent: tags_section=parent; break
        if tags_section:
            for a in tags_section.find_all("a"):
                tag_text=a.get_text(strip=True).lower()
                if tag_text and len(tag_text)>1: tags.append(tag_text)
            if not tags:
                raw=tags_section.get_text()
                m=re.search(r'Tags?\s*:\s*(.+)',raw,re.IGNORECASE)
                if m: tags=[t.strip().lower() for t in m.group(1).split(",") if t.strip()]
        if not tags:
            meta_kw=soup.find("meta",attrs={"name":"keywords"})
            if meta_kw and meta_kw.get("content"):
                tags=[t.strip().lower() for t in meta_kw["content"].split(",") if t.strip()]
        if not tags:
            for el in soup.find_all(class_=re.compile(r'tag',re.IGNORECASE)):
                for a in el.find_all("a"):
                    tag_text=a.get_text(strip=True).lower()
                    if tag_text and len(tag_text)>1: tags.append(tag_text)
        return [t for t in tags if len(t)>1 and len(t)<80]
    except Exception as e: print(f"  Tags scrape ERR {url[:50]}: {e}"); return []

def has_edito_tag(tags):
    if not tags: return False
    tags_lower=[t.lower().strip() for t in tags]
    for edito_tag in EDITO_TAGS:
        for t in tags_lower:
            if edito_tag in t: return True
    return False

def enrich_tags_existing(db, max_per_run=30):
    """Scrape les tags des articles existants qui n'en ont pas (max 30 par run), reclasse en edito si nécessaire."""
    ref=db.collection("articles")
    all_docs=list(ref.stream())
    count=0; reclassed=0; processed=0
    for doc in all_docs:
        if processed>=max_per_run: break
        d=doc.to_dict()
        if d.get("tags"): continue
        link=d.get("link","")
        if not link: continue
        processed+=1
        tags=scrape_tags(link)
        time.sleep(0.3)
        if tags:
            updates={"tags":tags}
            if has_edito_tag(tags) and d.get("category")!="edito":
                updates["category"]="edito"
                matched=[t for t in tags if any(et in t for et in EDITO_TAGS)]
                print(f"  ✎ reclassé edito (tag: {matched}) : {d.get('title','')[:50]}...")
                reclassed+=1
            # Aussi vérifier le tag "léa" + "crise golfe"
            tags_lower=[t.lower() for t in tags]
            has_lea=any('léa' in t or 'lea' in t for t in tags_lower)
            has_crise=any('crise golfe' in t or 'crise+golfe' in t for t in tags_lower)
            if has_lea and has_crise and d.get("category")!="edito":
                updates["category"]="edito"
                print(f"  ✎ reclassé edito (léa+crise golfe) : {d.get('title','')[:50]}...")
                reclassed+=1
            ref.document(doc.id).update(updates)
            count+=1
        else:
            ref.document(doc.id).update({"tags":[]})
    print(f"  Tags enrichis : {count}/{processed} articles traités (max {max_per_run}), {reclassed} reclassés edito")
def clean_xml(t):
    return re.sub(r'&(?!(?:#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);)','&amp;',re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]','',t))
def vimg(u):
    if not u or len(u)<20: return ""
    if any(x in u for x in ["1.gif","pixel","blank","spacer","logo"]): return ""
    return ("https://www.tourmag.com"+u) if u.startswith("/") else u

def check_image_url(url):
    if not url: return False
    try:
        r=requests.head(url,timeout=5,headers=HDR,allow_redirects=True)
        ct=r.headers.get("Content-Type","")
        cl=int(r.headers.get("Content-Length","0") or 0)
        return r.status_code==200 and "image" in ct and cl>2000
    except:
        return False

# ── RSS/HTML ──
def parse_html_fb(hb):
    soup=BeautifulSoup(hb,"html.parser"); arts=[]
    for d in soup.find_all("div",class_="result"):
        h3=d.find("h3",class_="titre")
        if not h3: continue
        a=h3.find("a",href=True)
        if not a: continue
        t,l=a.get_text(strip=True),a["href"]
        if l.startswith("/"): l="https://www.tourmag.com"+l
        au,pd="",None
        rub=d.find("div",class_="rubrique")
        if rub:
            at2=rub.find("a",rel="author")
            if at2: au=at2.get_text(strip=True)
            dm=re.search(r"(\d{2}/\d{2}/\d{4})",rub.get_text())
            if dm:
                try: pd=datetime.strptime(dm.group(1),"%d/%m/%Y").replace(tzinfo=timezone.utc)
                except: pass
        desc=""
        td=d.find("div",class_="texte")
        if td and td.find("a"): desc=td.find("a").get_text(strip=True)
        img=vimg(d.find("img").get("src","") if d.find("img") else "")
        arts.append({"title":t,"link":l,"description":desc,"pub_date":pd,"image_url":img,"author":au})
    print(f"HTML fallback : {len(arts)} articles"); return arts

def parse_rss():
    try:
        r=requests.get(RSS_URL,timeout=30,headers=HDR); r.raise_for_status(); raw=r.content
        if b"<!DOCTYPE" in raw[:500] or b"<html" in raw[:500].lower(): return parse_html_fb(raw)
        if not raw.lstrip()[:5] in (b"<?xml",b"<rss ",b"<feed"): return []
        feed=feedparser.parse(raw)
        if not feed.entries and feed.bozo: feed=feedparser.parse(clean_xml(raw.decode("utf-8",errors="replace")))
        if not feed.entries: return []
        arts=[]
        for e in feed.entries:
            pd=None
            if hasattr(e,"published_parsed") and e.published_parsed: pd=datetime(*e.published_parsed[:6],tzinfo=timezone.utc)
            elif hasattr(e,"updated_parsed") and e.updated_parsed: pd=datetime(*e.updated_parsed[:6],tzinfo=timezone.utc)
            img=""
            for at in ["enclosures","media_content","media_thumbnail"]:
                if hasattr(e,at):
                    for it in (getattr(e,at) if isinstance(getattr(e,at),list) else [getattr(e,at)]):
                        u=it.get("href",it.get("url",""))
                        if u: img=vimg(u); break
                if img: break
            arts.append({"title":e.get("title",""),"link":e.get("link",""),"description":e.get("summary",e.get("description","")),"pub_date":pd,"image_url":img,"author":e.get("author","")})
        return arts
    except Exception as ex: print(f"ERREUR RSS : {ex}"); return []

# ── Scrape full article content ──
def scrape_og_image(url):
    try:
        r=requests.get(url,timeout=10,headers=HDR)
        if r.status_code!=200: return ""
        soup=BeautifulSoup(r.content,"html.parser")
        og=soup.find("meta",property="og:image")
        if og and og.get("content"):
            return vimg(og["content"])
        tw=soup.find("meta",attrs={"name":"twitter:image"})
        if tw and tw.get("content"):
            return vimg(tw["content"])
        body=soup.find("div",class_="contenu") or soup.find("article") or soup
        for img in body.find_all("img"):
            src=img.get("src","")
            w=img.get("width","")
            if src and vimg(src) and (not w or int(w or 0)>100):
                return vimg(src)
        return ""
    except Exception as e:
        print(f"  og:image ERREUR {url[:40]}: {e}"); return ""

def enrich_images(articles):
    enriched=0
    for a in articles:
        if not a.get("image_url"):
            print(f"  Enrichissement image : {a['title'][:50]}...")
            img=scrape_og_image(a["link"])
            if img:
                a["image_url"]=img
                enriched+=1
                print(f"    → {img[:60]}")
            else:
                print(f"    → Aucune image trouvée")
            time.sleep(0.3)
    print(f"  Images enrichies : {enriched}/{len([a for a in articles if not a.get('image_url')])+enriched}")
    return articles

def scrape_article_content(url):
    try:
        r=requests.get(url,timeout=15,headers=HDR)
        if r.status_code!=200: return ""
        soup=BeautifulSoup(r.content,"html.parser")
        body=soup.find("div",class_="contenu") or soup.find("article") or soup.find("div",class_="article")
        if not body: body=soup
        paras=[p.get_text(strip=True) for p in body.find_all("p") if len(p.get_text(strip=True))>20]
        full_text=" ".join(paras)
        extras=""
        citations_guillemets=re.findall(r'[«""\u201c](.{30,800}?)[»""\u201d]',full_text)
        if citations_guillemets:
            extras+="\n--- CITATIONS ENTRE GUILLEMETS ---\n"+"\n".join(f'• «{c}»' for c in citations_guillemets[:8])
        italics=[]
        for tag in body.find_all(["em","i"]):
            txt=tag.get_text(strip=True)
            if len(txt)>40 and not txt.startswith("©") and not txt.startswith("Photo"):
                italics.append(txt)
        if italics:
            extras+="\n--- PASSAGES EN ITALIQUE (souvent des citations) ---\n"+"\n".join(f'• {c}' for c in italics[:6])
        noms=re.findall(r'(?:selon|explique|confie|déclare|témoigne|affirme|raconte|précise|indique|souligne)\s+([A-ZÀ-Ü][a-zà-ü]+\s+[A-ZÀ-Ü][a-zà-ü]+(?:\s*,\s*[^.«»"]{5,60})?)',full_text)
        if noms:
            extras+="\n--- PERSONNES CITÉES ---\n"+"\n".join(f'• {n}' for n in noms[:6])
        return full_text[:4000]+extras
    except Exception as e:
        print(f"  Scrape article ERREUR : {e}"); return ""

# ── Groq avec gestion rate limit ──
def gcall(msgs,mt=2000,retries=3):
    if not GROQ_API_KEY: return None
    for attempt in range(retries):
        try:
            r=requests.post("https://api.groq.com/openai/v1/chat/completions",headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},json={"model":"llama-3.3-70b-versatile","messages":msgs,"max_tokens":mt,"temperature":0.3},timeout=60)
            if r.status_code==429:
                # Lire le header Retry-After si disponible
                retry_after=int(r.headers.get("Retry-After",GROQ_PAUSE_RETRY))
                wait=max(retry_after,GROQ_PAUSE_RETRY)
                print(f"  Groq 429 — attente {wait}s (tentative {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        except requests.exceptions.HTTPError as e:
            if '429' in str(e):
                wait=GROQ_PAUSE_RETRY
                print(f"  Groq 429 — attente {wait}s (tentative {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            print(f"  Groq ERREUR (tentative {attempt+1}/{retries}) : {e}")
            if attempt<retries-1: time.sleep(10)
        except Exception as e:
            print(f"  Groq ERREUR (tentative {attempt+1}/{retries}) : {e}")
            if attempt<retries-1: time.sleep(10)
    return None

def pj(t):
    if not t: print("  pj: réponse Groq vide"); return None
    c=t.strip()
    if c.startswith("```"): c=re.sub(r'^```\w*\n?','',c).rstrip('`').strip()
    try: return json.loads(c)
    except Exception as e:
        print(f"  pj: JSON invalide — {e}")
        print(f"  pj: début réponse = {c[:200]}")
        m=re.search(r'\[.*\]',c,re.DOTALL)
        if m:
            try: return json.loads(m.group())
            except: pass
        return None

def classify_groq(articles):
    cats="institutionnel, aerien, croisiere, juridique, solutions, temoignages, geopolitique, economie, destinations, edito"
    items=[f"{i}. {a['title']} — {a.get('description','')[:120]} — auteur: {a.get('author','')}" for i,a in enumerate(articles)]
    prompt=f"""Classifie chaque article dans UNE catégorie : {cats}, general.
- institutionnel : MAE, diplomatie, rapatriements, conseils voyageurs
- aerien : compagnies, vols, suspensions, reprises, surcharges, aéroports, équipages
- croisiere : paquebots, ports, mer Rouge, canal de Suez
- juridique : droits clients, annulations, remboursements, assurance, force majeure
- solutions : initiatives TO, reprogrammations, destinations alternatives, EDV/SETO
- temoignages : récits agents de voyage, réceptifs, salons pros, vie quotidienne pros, interviews de professionnels
- geopolitique : analyses de la situation géopolitique, conflits, tensions, diplomatie internationale, frappes, cessez-le-feu
- economie : données économiques, études, sondages, intentions voyage, devises, impact économique chiffré
- destinations : impact sur des destinations spécifiques, pays qui restent accessibles, état du tourisme dans un pays
- edito : éditorial, billet d'humeur, chronique, opinion, tribune, "même pas peur", inventaire peurs/craintes
- general : si aucune
RÈGLE SPÉCIALE EDITO : si l'auteur est "Josette Sicsic" ou si le titre/description contient le mot "édito" ou "éditorial" ou "billet", classifie en edito.
Articles :
{chr(10).join(items)}
JSON uniquement : [{{"id":0,"cat":"aerien"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}]))
    if r and isinstance(r,list):
        m={c["id"]:c["cat"] for c in r if "id" in c}; print(f"  Groq classif : {len(m)}"); return m
    return None

def synthesis_groq(articles):
    items=[f"- {a['title']}: {a.get('description','')[:200]}" for a in articles[:15]]
    prompt=f"""Tu es journaliste spécialisé tourisme. À partir des articles ci-dessous, rédige EXACTEMENT 6 points de synthèse sur la crise au Moyen-Orient destinés aux agents de voyage français.

RÈGLES IMPÉRATIVES :
1. Chaque point est un objet JSON avec "tag" et "text"
2. Les 6 tags dans cet ordre : AÉRIEN, GÉOPOLITIQUE, DESTINATIONS, JURIDIQUE, TOUR-OPÉRATEURS, CONSEIL
3. Le texte fait entre 35 et 50 mots, c'est une VRAIE ANALYSE avec des faits concrets et des noms propres
4. Mets en **gras** UNIQUEMENT 1 à 2 mots-clés par point (un nom propre ET un fait important). Pas plus. Trop de gras = REJETÉ.

EXEMPLE :
{{"tag":"AÉRIEN","text":"**Air France** maintient la suspension de ses liaisons vers Beyrouth et Téhéran au moins jusqu'à fin mai. Emirates et Qatar Airways réduisent leurs fréquences sur les routes régionales, compliquant les **correspondances vers le Golfe** pour les voyageurs français."}}

Articles récents :
{chr(10).join(items)}

Réponds UNIQUEMENT avec un JSON array de 6 objets. Rien d'autre."""
    r=pj(gcall([{"role":"user","content":prompt}],mt=2500))
    if r and isinstance(r,list) and len(r)>=3:
        titles_lower={a['title'].lower().strip() for a in articles}
        processed=[]
        for p in r:
            if isinstance(p,dict) and p.get("text"):
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p["text"])
                tag=p.get("tag","INFO")
                if txt.lower().strip() not in titles_lower and len(txt)>20:
                    processed.append({"tag":tag,"text":txt})
            elif isinstance(p,str) and len(p)>30:
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p)
                if txt.lower().strip() not in titles_lower:
                    processed.append({"tag":"INFO","text":txt})
        if len(processed)>=3:
            print(f"  Synthèse : {len(processed)} pts"); return processed[:6]
        # Fallback
        fallback=[]
        for p in r:
            if isinstance(p,dict) and p.get("text"):
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p["text"])
                fallback.append({"tag":p.get("tag","INFO"),"text":txt})
            elif isinstance(p,str):
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p)
                fallback.append({"tag":"INFO","text":txt})
        if fallback: print(f"  Synthèse : {len(fallback)} pts (fallback)"); return fallback[:6]
    return None

def citations_groq(articles_with_content):
    items=[]
    for i,(a,content) in enumerate(articles_with_content):
        items.append(f'{i}. Titre: "{a["title"]}"\nAuteur article (JOURNALISTE — NE JAMAIS CITER): {a.get("author","")}\nContenu:\n{content[:2500]}')
    prompt=f"""Tu dois extraire des VRAIES CITATIONS VERBATIM (mot pour mot) depuis le contenu des articles ci-dessous.

INDICES POUR TROUVER LES CITATIONS :
- Les passages entre guillemets « » ou " " sont des citations directes
- Les passages EN ITALIQUE (section "PASSAGES EN ITALIQUE") sont souvent des citations sur TourMaG
- La section "PERSONNES CITÉES" te donne les noms des professionnels interviewés
- Les verbes "explique", "confie", "déclare", "témoigne", "selon" introduisent des citations

RÈGLES ABSOLUMENT NON NÉGOCIABLES :
1. Recopie la citation INTÉGRALEMENT, mot pour mot, entre 2 et 5 phrases. Ne tronque RIEN.
2. Le nom est celui de la PERSONNE QUI PARLE (un professionnel du tourisme), JAMAIS le journaliste/auteur
3. La fonction doit inclure le poste ET l'entreprise (ex: "Directeur, Voyages Leclerc Marseille")
4. Si l'article ne contient AUCUNE citation d'un professionnel du tourisme → citation="" nom="" fonction=""

Articles :
{chr(10).join(items)}

JSON uniquement : [{{"id":0,"citation":"La citation exacte mot pour mot...","nom":"Prénom Nom","fonction":"Poste, Entreprise"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=3000))
    if r and isinstance(r,list):
        m={c["id"]:{"citation":c.get("citation",""),"nom":c.get("nom",""),"fonction":c.get("fonction","")} for c in r if "id" in c and c.get("citation","")}
        print(f"  Citations : {len(m)} (avec contenu)"); return m
    return None

def timeline_groq(articles):
    items=[f"- [{a.get('pub_date','').isoformat()[:10] if a.get('pub_date') else '?'}] {a['title']} — {a.get('description','')[:120]}" for a in articles[:25]]
    prompt=f"""À partir de ces articles sur la crise au Moyen-Orient, extrais les 8-10 événements clés dans l'ordre chronologique.

RÈGLES IMPÉRATIVES :
- Chaque événement doit être rédigé comme une VRAIE PHRASE avec sujet, verbe, complément
- Utilise des NOMS PROPRES (compagnies aériennes, pays, organisations, personnes)
- La phrase fait entre 8 et 18 mots
- Utilise les vraies dates des articles, pas des dates inventées
- Privilégie les événements qui impactent directement le tourisme français

Articles :
{chr(10).join(items)}

JSON uniquement : [{{"date":"2025-10-01","event":"Air France suspend tous ses vols vers Beyrouth et Téhéran."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=1500))
    if r and isinstance(r,list): print(f"  Timeline : {len(r)} events"); return r
    return None

def airlines_groq(articles):
    aero=[a for a in articles if a.get("_cat")=="aerien"]
    if not aero: return None
    items=[f"- {a['title']}: {a.get('description','')[:120]}" for a in aero[:10]]
    prompt=f"""À partir de ces articles sur le trafic aérien au Moyen-Orient, extrais le statut des compagnies aériennes mentionnées. Pour chaque compagnie : nom, statut (suspendu/perturbé/opérationnel), détail court.

Articles :
{chr(10).join(items)}

JSON uniquement : [{{"compagnie":"Air France","statut":"suspendu","detail":"Vols suspendus vers le Liban et l'Iran"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}]))
    if r and isinstance(r,list): print(f"  Airlines : {len(r)} compagnies"); return r
    return None

def intro_groq(articles):
    items=[f"- {a['title']}" for a in articles[:15]]
    prompt=f"""Tu es rédacteur en chef d'un média spécialisé tourisme. Rédige un paragraphe d'introduction (3-4 phrases, environ 60-80 mots) pour un dashboard de veille sur la crise au Moyen-Orient destiné aux agents de voyage français.

Ce texte doit :
- Contextualiser brièvement la crise (depuis quand, quels pays principalement touchés)
- Mentionner l'impact concret sur le secteur du tourisme (vols, croisières, destinations)
- Donner le ton : informatif, professionnel, rassurant mais réaliste
- Être rédigé au présent

Articles récents pour contexte :
{chr(10).join(items)}

Réponds UNIQUEMENT avec le texte du paragraphe, sans guillemets, sans JSON."""
    r=gcall([{"role":"user","content":prompt}],mt=500)
    if r:
        t=r.strip().strip('"').strip("'")
        print(f"  Intro : {len(t)} car."); return t
    return None

def mae_groq(mae_data):
    items=[f"- country_key={k} | {v['label']}: niveau={v['level']}. Contenu: {v.get('full_content',v.get('summary',''))[:400]}" for k,v in mae_data.items()]
    prompt=f"""Expert tourisme. Pour chaque pays, fiche conseil 2-3 phrases pour agent de voyage : vendable ou à suspendre, zones sûres/à éviter, conseil pratique.
IMPORTANT: utilise EXACTEMENT la country_key comme "country".
Pays :
{chr(10).join(items)}
JSON : [{{"country":"liban","conseil_tourisme":"..."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=3000))
    if r and isinstance(r,list):
        m={c["country"]:c.get("conseil_tourisme","") for c in r if "country" in c}
        print(f"  MAE Groq : {len(m)} matched={len(set(m)&set(mae_data))}"); return m
    return None

# ── Classification/détection ──
def det_countries(a,kw):
    text=(a["title"]+" "+a.get("description","")).lower()
    return [ck for ck,ckws in kw.get("countries_detect",{}).items() if not ck.startswith("_") and any(k.lower() in text for k in ckws)]
def classif_kw(a,kw):
    text=(a["title"]+" "+a.get("description","")).lower()
    scores={cat:sum(1 for k2 in kw[cat]["keywords"] if k2.lower() in text) for cat in kw if cat!="countries_detect"}
    scores={k:v for k,v in scores.items() if v>0}
    return max(scores,key=scores.get) if scores else "general"

# ── AviationStack — Vols temps réel au départ de CDG ──
def fetch_aviationstack():
    """Récupère les vols au départ de CDG vers le Moyen-Orient via AviationStack."""
    if not AVIATIONSTACK_API_KEY:
        print("  AviationStack : pas de clé API, skip")
        return None
    try:
        print("  AviationStack : requête CDG départs...")
        url="http://api.aviationstack.com/v1/flights"
        params={"access_key":AVIATIONSTACK_API_KEY,"dep_iata":"CDG","flight_status":"scheduled","limit":100}
        r=requests.get(url,params=params,timeout=30)
        if r.status_code!=200:
            print(f"  AviationStack HTTP {r.status_code}")
            return None
        data=r.json()
        if "error" in data:
            print(f"  AviationStack erreur : {data['error'].get('message','')}")
            return None
        flights=data.get("data",[])
        print(f"  AviationStack : {len(flights)} vols CDG récupérés")
        # Filtrer les vols vers le Moyen-Orient
        destinations={}
        for f in flights:
            arr=f.get("arrival",{})
            arr_iata=arr.get("iata","")
            if arr_iata not in ME_AIRPORTS: continue
            city=ME_AIRPORTS[arr_iata]
            if city not in destinations:
                destinations[city]={"city":city,"iata":arr_iata,"flights":[]}
            airline_name=f.get("airline",{}).get("name","Inconnu")
            flight_num=f.get("flight",{}).get("iata","")
            status=f.get("flight_status","unknown")
            status_labels={"scheduled":"Programmé","active":"En vol","landed":"Atterri","cancelled":"Annulé","incident":"Incident","diverted":"Dérouté","delayed":"Retardé"}
            destinations[city]["flights"].append({
                "airline":airline_name,
                "flight":flight_num,
                "status":status,
                "status_label":status_labels.get(status,status)
            })
        result={"destinations":sorted(destinations.values(),key=lambda d:d["city"]),"last_check":datetime.now(timezone.utc).isoformat(),"source":"AviationStack"}
        me_count=sum(len(d["flights"]) for d in result["destinations"])
        print(f"  AviationStack : {me_count} vols Moyen-Orient vers {len(result['destinations'])} destinations")
        return result
    except Exception as e:
        print(f"  AviationStack ERREUR : {e}")
        return None

# ── Finance ──
def fetch_fin():
    res={}
    for key,cfg in FINANCE_SYMBOLS.items():
        try:
            h=yf.Ticker(cfg["symbol"]).history(start=CONFLICT_START_DATE)
            if h.empty: continue
            cur,st2=float(h["Close"].iloc[-1]),float(h["Close"].iloc[0])
            chg=round(((cur-st2)/st2)*100,2); fx=cfg["sector"]=="forex"
            res[key]={"symbol":cfg["symbol"],"label":cfg["label"],"currency":cfg["currency"],"sector":cfg["sector"],"current_price":round(cur,4 if fx else 2),"start_price":round(st2,4 if fx else 2),"change_pct":chg,"history":[{"date":d.strftime("%Y-%m-%d"),"close":round(float(r["Close"]),2)} for d,r in h.iterrows()],"last_update":datetime.now(timezone.utc).isoformat()}
            print(f"  Finance : {cfg['label']} ({chg:+.2f}%)")
        except Exception as e: print(f"  Finance ERR {cfg['symbol']}: {e}")
    return res

# ── MAE ──
def scrape_mae():
    res={}
    for ck,slug in MAE_SLUGS.items():
        url=f"{MAE_BASE}{slug}/"
        try:
            r=requests.get(url,timeout=15,headers=HDR)
            if r.status_code!=200: res[ck]=_mfb(ck,url,f"HTTP {r.status_code}"); continue
            soup=BeautifulSoup(r.content,"html.parser")
            ap=[p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True))>=15]
            rel=[t for t in ap if any(k in t.lower() for k in ["déconseillé","vigilance","quitter","se rendre","invités à","recommandé","risque","frappes","prudence","sécurité","visa","passeport","entrée","séjour","ambassade","zone","éviter","déplacement","frontière","aéroport"]) and not any(g in t.lower() for g in MAE_GENERIC)]
            rt=" ".join(rel).lower()
            found=[(lt,cd,co) for lt,cd,co in ALERT_LEVELS if lt in rt]
            if found:
                ip=len(found)>1
                if ip: least,worst=found[-1],found[0]; ll=f"{least[0].capitalize()} (certaines zones : {worst[0]})"; lc,lcl=least[1],least[2]
                else: ip=False; ll,lc,lcl=found[0][0].capitalize(),found[0][1],found[0][2]
            else: ll,lc,lcl,ip="Non déterminé","unknown","gray",False
            fc=" ".join(rel)[:1500]; ss=" ".join(rel[:3])[:500]
            if not ss:
                meta=soup.find("meta",attrs={"name":"description"})
                if meta:
                    s=meta.get("content","").strip()
                    if s and "ministère" not in s.lower(): ss=s[:500]
            upd=""; um=re.search(r'Dernière mise à jour[^\d]*(\d{1,2}\s+\w+\s+\d{4})',soup.get_text().replace('\n',' '))
            if um: upd=um.group(1).strip()
            res[ck]={"country":ck,"label":MAE_LABELS.get(ck,ck),"level":ll,"level_code":lc,"color":lcl,"is_partial":ip,"summary":ss,"full_content":fc,"url":url,"last_update_mae":upd,"conseil_tourisme":"","last_scraped":datetime.now(timezone.utc).isoformat()}
            print(f"  MAE {ck} : {ll}")
        except Exception as e: print(f"  MAE ERR {ck}: {e}"); res[ck]=_mfb(ck,url,str(e)[:200])
    return res
def _mfb(ck,url,msg):
    return {"country":ck,"label":MAE_LABELS.get(ck,ck),"level":"Indisponible","level_code":"unknown","color":"gray","is_partial":False,"summary":msg,"full_content":"","url":url,"last_update_mae":"","conseil_tourisme":"","last_scraped":datetime.now(timezone.utc).isoformat()}

# ── Firestore ──
def gid(l): return hashlib.md5(l.encode()).hexdigest()[:16]
def sync_arts(db,articles,kw,gc,cit):
    ref=db.collection("articles"); n=0; img_updated=0; tags_updated=0
    for i,a in enumerate(articles):
        if not a["link"]: continue
        did=gid(a["link"])
        tags=a.get("_tags",[])
        existing=ref.document(did).get()
        if existing.exists:
            ed=existing.to_dict()
            updates={}
            if not ed.get("image_url") and a.get("image_url"):
                updates["image_url"]=a["image_url"]; img_updated+=1
            if not ed.get("tags") and tags:
                updates["tags"]=tags; tags_updated+=1
            if has_edito_tag(tags) and ed.get("category")!="edito":
                updates["category"]="edito"
                matched=[t for t in tags if any(et in t for et in EDITO_TAGS)]
                print(f"  ✎ reclassé edito (tag: {matched}) : {a['title'][:50]}...")
            if updates: ref.document(did).update(updates)
            continue
        cat=gc[i] if gc and i in gc else classif_kw(a,kw)
        if has_edito_tag(tags) and cat!="edito":
            matched=[t for t in tags if any(et in t for et in EDITO_TAGS)]
            print(f"  ⚑ edito forcé par tag ({matched}) : {a['title'][:50]}...")
            cat="edito"
        doc={"title":a["title"],"link":a["link"],"description":a.get("description",""),"image_url":a.get("image_url",""),"author":a.get("author",""),"pub_date":a["pub_date"],"category":cat,"countries":det_countries(a,kw),"tags":tags,"created_at":firestore.SERVER_TIMESTAMP}
        if cit and i in cit:
            doc["citation"]=cit[i].get("citation","")
            doc["citation_nom"]=cit[i].get("nom","")
            doc["citation_fonction"]=cit[i].get("fonction","")
        ref.document(did).set(doc); n+=1; print(f"  + [{cat}] {a['title'][:60]}...")
    print(f"Articles : {n} nouveaux, {img_updated} images, {tags_updated} tags mis à jour"); return n

def sync_fin(db,d):
    for k,v in d.items(): db.collection("market_data").document(k).set(v)
def sync_mae(db,d,ex):
    for k,v in d.items():
        if not v.get("conseil_tourisme") and ex.get(k,{}).get("conseil_tourisme"):
            v["conseil_tourisme"]=ex[k]["conseil_tourisme"]
        db.collection("mae_alerts").document(k).set(v)
def sync_synth(db,p): db.collection("config").document("synthesis").set({"points":p,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_timeline(db,t): db.collection("config").document("timeline").set({"events":t,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_airlines(db,a,realtime=None):
    doc={"airlines":a,"generated_at":datetime.now(timezone.utc).isoformat()}
    if realtime: doc["realtime"]=realtime
    db.collection("config").document("airlines").set(doc)
def sync_intro(db,t): db.collection("config").document("intro").set({"text":t,"generated_at":datetime.now(timezone.utc).isoformat()})
def upd_cfg(db,n): db.collection("config").document("radar").set({"last_sync":datetime.now(timezone.utc).isoformat(),"conflict_start_date":CONFLICT_START_DATE,"rss_url":RSS_URL,"last_articles":n},merge=True)

# ── Main ──
def main():
	print("MAIN DÉMARRÉ", flush=True)
    print("="*50+f"\nRadar v6.1 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"+"="*50)
    db=init_fb(); kw=load_kw()

    ex_mae={}
    try:
        for doc in db.collection("mae_alerts").stream(): ex_mae[doc.id]=doc.to_dict()
    except: pass

    print("\n--- RSS ---")
    articles=parse_rss()

    if articles:
        missing=[a for a in articles if not a.get("image_url")]
        if missing:
            print(f"\n--- Enrichissement images ({len(missing)} sans image) ---")
            enrich_images(articles)

    # Classification Groq
    gc=None
    if articles and GROQ_API_KEY:
        print("\n--- Classification Groq ---")
        gc=classify_groq(articles)
        print(f"  ⏳ Pause {GROQ_PAUSE_BETWEEN_BLOCKS}s...")
        time.sleep(GROQ_PAUSE_BETWEEN_BLOCKS)

    if gc is None: gc={}

    # Scraping tags pour les nouveaux articles
    if articles:
        print(f"\n--- Scraping tags ({len(articles)} articles) ---")
        for a in articles:
            tags=scrape_tags(a["link"])
            a["_tags"]=tags
            if tags: print(f"  Tags : {a['title'][:40]}... → {tags}")
            time.sleep(0.3)

    for i,a in enumerate(articles):
        author=(a.get("author","") or "").lower().strip()
        title_desc=(a.get("title","")+" "+a.get("description","")).lower()
        if "josette sicsic" in author:
            gc[i]="edito"
            print(f"  Edito forcé (Josette Sicsic) : {a['title'][:50]}")
        elif any(kw_e in title_desc for kw_e in ["édito","editorial","éditorial","billet d'humeur","billet d'humeur","futuroscopie","expert"]):
            gc[i]="edito"
            print(f"  Edito forcé (mot-clé) : {a['title'][:50]}")
        # Détection edito par tags TourMaG
        tags=a.get("_tags",[])
        if has_edito_tag(tags) and gc.get(i)!="edito":
            gc[i]="edito"
            matched=[t for t in tags if any(et in t for et in EDITO_TAGS)]
            print(f"  Edito forcé (tag: {matched}) : {a['title'][:50]}")

    for i,a in enumerate(articles):
        a["_cat"]=gc[i] if i in gc else classif_kw(a,kw)

    # Citations Groq
    cit=None
    if articles and GROQ_API_KEY and gc:
        temo_idx=[(i,a) for i,a in enumerate(articles) if gc.get(i)=="temoignages"][:3]
        if temo_idx:
            print("\n--- Scraping articles témoignages ---")
            arts_with_content=[]
            for i,a in temo_idx:
                print(f"  Scraping {a['link'][:60]}...")
                content=scrape_article_content(a["link"])
                arts_with_content.append((a,content))
                time.sleep(0.5)
            print("\n--- Citations Groq ---")
            raw_cit=citations_groq(arts_with_content)
            if raw_cit:
                cit={}
                for li,gi in enumerate([i for i,_ in temo_idx]):
                    if li in raw_cit: cit[gi]=raw_cit[li]
            print(f"  ⏳ Pause {GROQ_PAUSE_BETWEEN_BLOCKS}s...")
            time.sleep(GROQ_PAUSE_BETWEEN_BLOCKS)

    # Sync articles
    if articles:
        print("\n--- Articles → Firestore ---"); n=sync_arts(db,articles,kw,gc,cit)
    else: n=0

    # Enrichir les tags des articles existants sans tags
    # print("\n--- Enrichissement tags existants ---")
    # enrich_tags_existing(db)

    # Synthèse Groq
    if articles and GROQ_API_KEY:
        print("\n--- Synthèse ---")
        pts=synthesis_groq(articles)
        if pts:
            sync_synth(db,pts)
        else:
            print("  Synthèse Groq indisponible, pas de mise à jour (on garde l'ancienne)")
        print(f"  ⏳ Pause {GROQ_PAUSE_BETWEEN_BLOCKS}s...")
        time.sleep(GROQ_PAUSE_BETWEEN_BLOCKS)

    # Timeline Groq
    if articles and GROQ_API_KEY:
        print("\n--- Timeline ---")
        tl=timeline_groq(articles)
        if tl: sync_timeline(db,tl)
        print(f"  ⏳ Pause {GROQ_PAUSE_BETWEEN_BLOCKS}s...")
        time.sleep(GROQ_PAUSE_BETWEEN_BLOCKS)

    # Airlines Groq + AviationStack temps réel
    realtime_data=None
    if AVIATIONSTACK_API_KEY:
        print("\n--- AviationStack (temps réel) ---")
        realtime_data=fetch_aviationstack()

    if articles and GROQ_API_KEY:
        print("\n--- Airlines (Groq) ---")
        al=airlines_groq(articles)
        if al: sync_airlines(db,al,realtime_data)
        elif realtime_data: sync_airlines(db,[],realtime_data)
        print(f"  ⏳ Pause {GROQ_PAUSE_BETWEEN_BLOCKS}s...")
        time.sleep(GROQ_PAUSE_BETWEEN_BLOCKS)
    elif realtime_data:
        # Même sans Groq, on stocke les données temps réel
        sync_airlines(db,[],realtime_data)

    # Introduction Groq
    if articles and GROQ_API_KEY:
        print("\n--- Introduction ---")
        intro=intro_groq(articles)
        if intro: sync_intro(db,intro)
        print(f"  ⏳ Pause {GROQ_PAUSE_BETWEEN_BLOCKS}s...")
        time.sleep(GROQ_PAUSE_BETWEEN_BLOCKS)

    # Finance
    print("\n--- Finance ---")
    fd=fetch_fin()
    if fd: sync_fin(db,fd)

    # Featured article
    if articles:
        print("\n--- Article à la une ---")
        featured=None
        for a in articles[:15]:
            img=a.get("image_url","")
            if img and check_image_url(img):
                featured={"title":a["title"],"link":a["link"],"description":a.get("description",""),"image_url":img,"author":a.get("author",""),"pub_date":a["pub_date"].isoformat() if a.get("pub_date") else ""}
                print(f"  Featured : {a['title'][:60]}... (image OK)")
                break
            elif img:
                print(f"  Skip : {a['title'][:40]}... (image inaccessible)")
        if featured:
            db.collection("config").document("radar").set({"featured_article":featured},merge=True)
        else:
            print("  Aucun article avec photo valide trouvé")

    # MAE
    print("\n--- France Diplomatie ---")
    mae=scrape_mae()
    if mae and GROQ_API_KEY:
        print("\n--- MAE Groq ---")
        conseils=mae_groq(mae)
        if conseils:
            for ck,c in conseils.items():
                if ck in mae and c: mae[ck]["conseil_tourisme"]=c
    if mae: sync_mae(db,mae,ex_mae)

    upd_cfg(db,n)
    print("\n"+"="*50+"\nSync terminée\n"+"="*50)
			
			print("CONSTANTES OK, lancement main...", flush=True)
if __name__=="__main__": main()
