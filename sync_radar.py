#!/usr/bin/env python3
"""Radar Crise Moyen-Orient — v7
Architecture delta : scrape tout au premier lancement, puis seulement les nouveaux articles.
Les blocs IA (synthèse, timeline, airlines, actions) travaillent sur TOUS les articles en base.
"""
import json,hashlib,os,re,sys,time
from datetime import datetime,timezone
from pathlib import Path
import feedparser,requests,yfinance as yf
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials,firestore

RSS_URL=os.getenv("RSS_URL","https://www.tourmag.com/xml/syndication.rss?t=crise+golfe")
TAGS_PAGE_URL="https://www.tourmag.com/tags/crise+golfe/"
CONFLICT_START_DATE=os.getenv("CONFLICT_START_DATE","2025-10-01")
GROQ_API_KEY=os.getenv("GROQ_API_KEY","")
FINANCE_SYMBOLS={"brent":{"symbol":"BZ=F","label":"Brent (baril)","currency":"$","sector":"commodity"},"eurusd":{"symbol":"EURUSD=X","label":"EUR / USD","currency":"","sector":"forex"},"AF.PA":{"symbol":"AF.PA","label":"Air France-KLM","currency":"€","sector":"aerien"},"TUI1.DE":{"symbol":"TUI1.DE","label":"TUI Group","currency":"€","sector":"to"},"AC.PA":{"symbol":"AC.PA","label":"Accor","currency":"€","sector":"hotellerie"},"BKNG":{"symbol":"BKNG","label":"Booking Holdings","currency":"$","sector":"ota"},"CCL":{"symbol":"CCL","label":"Carnival Corp","currency":"$","sector":"croisiere"},"AMS.MC":{"symbol":"AMS.MC","label":"Amadeus IT","currency":"€","sector":"tech"},"AIR.PA":{"symbol":"AIR.PA","label":"Airbus","currency":"€","sector":"aerien"},"RYA.IR":{"symbol":"RYA.IR","label":"Ryanair","currency":"€","sector":"aerien"}}
MAE_SLUGS={"israel":"israel-palestine","liban":"liban","iran":"iran","irak":"irak","syrie":"syrie","jordanie":"jordanie","egypte":"egypte","turquie":"turquie","arabie_saoudite":"arabie-saoudite","emirats":"emirats-arabes-unis","qatar":"qatar","oman":"oman","bahrein":"bahrein","koweit":"koweit","yemen":"yemen","chypre":"chypre","grece":"grece"}
MAE_LABELS={"israel":"Israël / Palestine","liban":"Liban","iran":"Iran","irak":"Irak","syrie":"Syrie","jordanie":"Jordanie","egypte":"Égypte","turquie":"Turquie","arabie_saoudite":"Arabie Saoudite","emirats":"Émirats Arabes Unis","qatar":"Qatar","oman":"Oman","bahrein":"Bahreïn","koweit":"Koweït","yemen":"Yémen","chypre":"Chypre","grece":"Grèce"}
MAE_BASE="https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/conseils-par-pays-destination/"
ALERT_LEVELS=[("formellement déconseillé","formellement_deconseille","red"),("déconseillé sauf raison impérative","deconseille_sauf_ri","orange"),("déconseillé sauf raison","deconseille_sauf_ri","orange"),("vigilance renforcée","vigilance_renforcee","yellow"),("vigilance normale","vigilance_normale","green")]
MAE_GENERIC=["urgence attentat","vigilance renforcée pour les ressortissants français à l'étranger","appel à la vigilance maximale"]
HDR={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept":"text/html,*/*","Accept-Language":"fr-FR,fr;q=0.9"}
KEYWORDS_PATH=Path(__file__).parent/"keywords.json"
GROQ_PAUSE=30
GROQ_PAUSE_RETRY=45
INITIAL_THRESHOLD=10  # Si moins de X articles en base → run initial (scrape all pages)

def init_fb():
    sa=os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa: sys.exit("ERREUR: FIREBASE_SERVICE_ACCOUNT")
    firebase_admin.initialize_app(credentials.Certificate(json.loads(sa))); return firestore.client()
def load_kw():
    with open(KEYWORDS_PATH,"r",encoding="utf-8") as f: return json.load(f)
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
    except: return False
def gid(l): return hashlib.md5(l.encode()).hexdigest()[:16]

# ══════════════════════════════════════════════════
# FIRESTORE — Charger les articles existants
# ══════════════════════════════════════════════════
def load_existing_articles(db):
    """Charge tous les articles déjà en base Firestore."""
    arts=[]
    try:
        for doc in db.collection("articles").order_by("pub_date",direction=firestore.Query.DESCENDING).stream():
            d=doc.to_dict()
            d["_id"]=doc.id
            arts.append(d)
    except Exception as e:
        print(f"  Erreur lecture articles : {e}")
    return arts

def get_existing_links(db):
    """Retourne un set des liens d'articles déjà en base (rapide)."""
    links=set()
    try:
        for doc in db.collection("articles").stream():
            d=doc.to_dict()
            if d.get("link"): links.add(d["link"])
    except: pass
    return links

# ══════════════════════════════════════════════════
# SCRAPING — Page unique (RSS/HTML) et All Pages
# ══════════════════════════════════════════════════
def parse_html_page(hb):
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
    return arts

def parse_rss_page():
    """Parse le RSS ou la page 1 HTML. Retourne ~20 articles."""
    try:
        r=requests.get(RSS_URL,timeout=30,headers=HDR); r.raise_for_status(); raw=r.content
        if b"<!DOCTYPE" in raw[:500] or b"<html" in raw[:500].lower():
            arts=parse_html_page(raw)
            print(f"  HTML page 1 : {len(arts)} articles"); return arts
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
        print(f"  RSS : {len(arts)} articles"); return arts
    except Exception as ex: print(f"  ERREUR RSS : {ex}"); return []

def scrape_all_pages():
    """Scrape TOUTES les pages de résultats TourMaG (pour le run initial)."""
    all_arts=[]; seen=set()
    print(f"  === SCRAPING COMPLET (toutes les pages) ===")
    # Page 1
    try:
        r=requests.get(TAGS_PAGE_URL,timeout=30,headers=HDR)
        if r.status_code!=200: print(f"  HTTP {r.status_code}"); return []
        arts=parse_html_page(r.content)
        for a in arts:
            if a["link"] not in seen: seen.add(a["link"]); all_arts.append(a)
        per_page=len(arts)
        print(f"  Page 1 → {len(all_arts)} articles")
        if not arts or per_page==0: return all_arts

        # Pagination TourMaG : ?debut_resultats=20&start_liste=20,40,60...
        page=2
        for offset in range(per_page, per_page*20, per_page):
            url=f"{TAGS_PAGE_URL}?debut_resultats={per_page}&start_liste={offset}"
            try:
                r2=requests.get(url,timeout=30,headers=HDR)
                if r2.status_code!=200:
                    print(f"  Page {page} : HTTP {r2.status_code}, arrêt")
                    break
                arts2=parse_html_page(r2.content)
                new=0
                for a in arts2:
                    if a["link"] not in seen: seen.add(a["link"]); all_arts.append(a); new+=1
                print(f"  Page {page} → {new} nouveaux (total: {len(all_arts)})")
                if new==0:
                    print(f"  Plus de nouveaux articles, arrêt pagination")
                    break
                page+=1
                time.sleep(0.5)
            except Exception as e:
                print(f"  Erreur page {page}: {e}"); break
    except Exception as e: print(f"  Erreur page 1: {e}")
    all_arts.sort(key=lambda a: a.get("pub_date") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    print(f"  TOTAL SCRAPING : {len(all_arts)} articles")
    return all_arts

# ══════════════════════════════════════════════════
# IMAGES
# ══════════════════════════════════════════════════
def scrape_og_image(url):
    try:
        r=requests.get(url,timeout=10,headers=HDR)
        if r.status_code!=200: return ""
        soup=BeautifulSoup(r.content,"html.parser")
        og=soup.find("meta",property="og:image")
        if og and og.get("content"): return vimg(og["content"])
        tw=soup.find("meta",attrs={"name":"twitter:image"})
        if tw and tw.get("content"): return vimg(tw["content"])
        body=soup.find("div",class_="contenu") or soup.find("article") or soup
        for img in body.find_all("img"):
            src=img.get("src",""); w=img.get("width","")
            if src and vimg(src) and (not w or int(w or 0)>100): return vimg(src)
        return ""
    except Exception as e: print(f"  og:image ERR {url[:40]}: {e}"); return ""

def enrich_images(articles):
    enriched=0
    for a in articles:
        if not a.get("image_url"):
            img=scrape_og_image(a["link"])
            if img: a["image_url"]=img; enriched+=1
            time.sleep(0.3)
    print(f"  Images enrichies : {enriched}"); return articles

# ══════════════════════════════════════════════════
# SCRAPE ARTICLE CONTENT (pour citations)
# ══════════════════════════════════════════════════
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
        citations_fancy=re.findall(r'[«\u201c](.{30,800}?)[»\u201d]',full_text)
        citations_straight=re.findall(r'"([^"]{30,800}?)"',full_text)
        all_cit=list(dict.fromkeys(citations_fancy+citations_straight))
        if all_cit: extras+="\n--- CITATIONS ENTRE GUILLEMETS ---\n"+"\n".join(f'• «{c}»' for c in all_cit[:10])
        italics=[tag.get_text(strip=True) for tag in body.find_all(["em","i"]) if len(tag.get_text(strip=True))>40 and not tag.get_text(strip=True).startswith("©")]
        if italics: extras+="\n--- PASSAGES EN ITALIQUE ---\n"+"\n".join(f'• {c}' for c in italics[:6])
        noms=re.findall(r'(?:selon|explique|confie|déclare|témoigne|affirme|raconte|précise|indique|souligne)\s+([A-ZÀ-Ü][a-zà-ü]+\s+[A-ZÀ-Ü][a-zà-ü]+(?:\s*,\s*[^.«»"]{5,60})?)',full_text)
        if noms: extras+="\n--- PERSONNES CITÉES ---\n"+"\n".join(f'• {n}' for n in noms[:6])
        return full_text[:4000]+extras
    except Exception as e: print(f"  Scrape article ERR : {e}"); return ""

# ══════════════════════════════════════════════════
# GROQ — Appels IA
# ══════════════════════════════════════════════════
def gcall(msgs,mt=2000,retries=3):
    if not GROQ_API_KEY: return None
    for attempt in range(retries):
        try:
            r=requests.post("https://api.groq.com/openai/v1/chat/completions",headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},json={"model":"llama-3.3-70b-versatile","messages":msgs,"max_tokens":mt,"temperature":0.3},timeout=60)
            if r.status_code==429:
                wait=max(int(r.headers.get("Retry-After",GROQ_PAUSE_RETRY)),GROQ_PAUSE_RETRY)
                print(f"  Groq 429 — attente {wait}s ({attempt+1}/{retries})"); time.sleep(wait); continue
            r.raise_for_status(); return r.json()["choices"][0]["message"]["content"]
        except requests.exceptions.HTTPError as e:
            if '429' in str(e): time.sleep(GROQ_PAUSE_RETRY); continue
            print(f"  Groq ERR ({attempt+1}/{retries}): {e}")
            if attempt<retries-1: time.sleep(10)
        except Exception as e:
            print(f"  Groq ERR ({attempt+1}/{retries}): {e}")
            if attempt<retries-1: time.sleep(10)
    return None

def pj(t):
    if not t: print("  pj: réponse vide"); return None
    c=t.strip()
    if c.startswith("```"): c=re.sub(r'^```\w*\n?','',c).rstrip('`').strip()
    try: return json.loads(c)
    except:
        m=re.search(r'\[.*\]',c,re.DOTALL)
        if m:
            try: return json.loads(m.group())
            except: pass
        print(f"  pj: JSON invalide — {c[:150]}"); return None

def classify_groq(articles):
    """Classifie par lots de 20."""
    cats="institutionnel, aerien, croisiere, juridique, solutions, temoignages, geopolitique, economie, destinations, edito"
    cat_desc="""- institutionnel : MAE, diplomatie, rapatriements, conseils voyageurs
- aerien : compagnies, vols, suspensions, reprises, surcharges, aéroports
- croisiere : paquebots, ports, mer Rouge, canal de Suez
- juridique : droits clients, annulations, remboursements, assurance, force majeure
- solutions : initiatives TO, reprogrammations, destinations alternatives, EDV/SETO
- temoignages : récits agents de voyage, réceptifs, salons pros, interviews
- geopolitique : analyses géopolitiques, conflits, tensions, diplomatie, frappes
- economie : données économiques, études, sondages, devises, impact chiffré
- destinations : impact sur des destinations spécifiques, état du tourisme dans un pays
- edito : éditorial, billet d'humeur, chronique, opinion, tribune
- general : si aucune
RÈGLE SPÉCIALE : si l'auteur est "Josette Sicsic" ou si titre contient "édito"/"éditorial"/"billet", classifie en edito."""
    all_results={}
    batch_size=20
    for start in range(0,len(articles),batch_size):
        batch=articles[start:start+batch_size]
        items=[f"{start+j}. {a['title']} — {a.get('description','')[:120]} — auteur: {a.get('author','')}" for j,a in enumerate(batch)]
        prompt=f"Classifie chaque article dans UNE catégorie : {cats}, general.\n{cat_desc}\nArticles :\n{chr(10).join(items)}\nJSON uniquement : [{{\"id\":{start},\"cat\":\"aerien\"}}]"
        r=pj(gcall([{"role":"user","content":prompt}]))
        if r and isinstance(r,list):
            for c in r:
                if "id" in c: all_results[c["id"]]=c["cat"]
            print(f"  Classif batch {start}-{start+len(batch)-1} : {len([c for c in r if 'id' in c])}")
        else: print(f"  Classif batch {start}-{start+len(batch)-1} : ÉCHEC")
        if start+batch_size<len(articles): time.sleep(GROQ_PAUSE)
    print(f"  Classif total : {len(all_results)}/{len(articles)}")
    return all_results if all_results else None

def synthesis_groq(all_articles):
    """Synthèse basée sur TOUS les articles en base (les 15 plus récents)."""
    items=[f"- {a.get('title','')}: {a.get('description','')[:200]}" for a in all_articles[:15]]
    prompt=f"""Tu es journaliste spécialisé tourisme. À partir des articles ci-dessous, rédige EXACTEMENT 6 points de synthèse sur la crise au Moyen-Orient destinés aux agents de voyage français.

RÈGLES IMPÉRATIVES :
1. Chaque point est un objet JSON avec "tag" (catégorie) et "text" (le paragraphe)
2. Les 6 tags OBLIGATOIRES dans cet ordre : AÉRIEN, GÉOPOLITIQUE, DESTINATIONS, JURIDIQUE, TOUR-OPÉRATEURS, CONSEIL
3. Le texte fait entre 40 et 60 mots (3 lignes minimum). Un texte d'une seule phrase est REJETÉ.
4. Le texte est une VRAIE ANALYSE avec des faits concrets, des noms propres
5. IMPORTANT : mets les mots-clés et informations importantes entre balises <strong> pour les mettre en gras. Au moins 3-4 mots/expressions en gras par paragraphe.

EXEMPLE de format attendu :
{{"tag":"AÉRIEN","text":"Les compagnies européennes comme <strong>Air France</strong> et <strong>Lufthansa</strong> maintiennent la <strong>suspension de leurs liaisons</strong> vers Beyrouth, Téhéran et certaines villes irakiennes. <strong>Emirates</strong> et Qatar Airways ont réduit leurs fréquences sur plusieurs routes régionales, compliquant les correspondances vers le Golfe."}}

Un texte comme "Les compagnies sont touchées par la crise." est TROP COURT et sera REJETÉ.

Articles récents :
{chr(10).join(items)}

Réponds UNIQUEMENT avec un JSON array de 6 objets. Rien d'autre."""
    r=pj(gcall([{"role":"user","content":prompt}],mt=3500))
    if r and isinstance(r,list) and len(r)>=3:
        # Accepter les deux formats : objets {tag,text} ou strings simples
        result=[]
        for p in r:
            if isinstance(p,dict) and 'text' in p:
                result.append(p)
            elif isinstance(p,str) and len(p)>30:
                result.append({"tag":"INFO","text":p})
        if len(result)>=3: print(f"  Synthèse : {len(result)} pts"); return result[:6]
        # Fallback strings
        titles={a.get('title','').lower().strip() for a in all_articles}
        filtered=[p for p in r if isinstance(p,str) and p.lower().strip() not in titles and len(p)>30]
        if len(filtered)>=3: return [{"tag":"INFO","text":p} for p in filtered[:6]]
    return None

def citations_groq(articles_with_content):
    items=[f'{i}. Titre: "{a["title"]}"\nAuteur (JOURNALISTE): {a.get("author","")}\nContenu:\n{content[:2500]}' for i,(a,content) in enumerate(articles_with_content)]
    prompt=f"""Extrais des VRAIES CITATIONS VERBATIM depuis ces articles.
PRIORITÉ : guillemets « » " " > passages en italique > verbes introducteurs (explique, confie, déclare...)
RÈGLES : citation intégrale 2-5 phrases, nom de la PERSONNE QUI PARLE (pas le journaliste), fonction + entreprise.
Si pas de citation de professionnel du tourisme → citation="" nom="" fonction=""
Articles :
{chr(10).join(items)}
JSON : [{{"id":0,"citation":"...","nom":"Prénom Nom","fonction":"Poste, Entreprise"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=3000))
    if r and isinstance(r,list):
        return {c["id"]:{"citation":c.get("citation",""),"nom":c.get("nom",""),"fonction":c.get("fonction","")} for c in r if "id" in c and c.get("citation","")}
    return None

def timeline_groq(all_articles):
    def safe_date(d):
        if not d: return '?'
        if hasattr(d,'isoformat'): return d.isoformat()[:10]
        if hasattr(d,'toDate'): return d.toDate().isoformat()[:10]
        try: return str(d)[:10]
        except: return '?'
    items=[f"- [{safe_date(a.get('pub_date',''))}] {a.get('title','')}" for a in all_articles[:25]]
    prompt=f"""À partir de ces articles sur la crise au Moyen-Orient, extrais les 8-10 événements clés dans l'ordre chronologique.

RÈGLES :
- Chaque événement = VRAIE PHRASE complète (sujet + verbe + complément), commençant par une majuscule
- Utilise des NOMS PROPRES (Air France, Emirates, Liban, Quai d'Orsay...)
- 8-18 mots par événement
- Utilise les vraies dates des articles

Articles :
{chr(10).join(items)}

JSON uniquement : [{{"date":"2026-03-01","event":"Air France suspend tous ses vols vers Beyrouth et Téhéran."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=1500))
    if r and isinstance(r,list) and len(r)>=3: print(f"  Timeline : {len(r)} events"); return r
    print("  Timeline : ÉCHEC Groq")
    return None

def airlines_groq(all_articles):
    aero=[a for a in all_articles if a.get("category")=="aerien" or a.get("_cat")=="aerien"]
    if not aero: return None
    items=[f"- {a.get('title','')}: {a.get('description','')[:120]}" for a in aero[:10]]
    prompt=f"""Statut des compagnies aériennes au Moyen-Orient. Pour chaque : nom, statut (suspendu/perturbé/opérationnel), détail court.
Articles :
{chr(10).join(items)}
JSON : [{{"compagnie":"Air France","statut":"suspendu","detail":"Vols suspendus vers le Liban et l'Iran"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}]))
    if r and isinstance(r,list): print(f"  Airlines : {len(r)}"); return r
    return None

def actions_groq(all_articles):
    """Génère le bloc 'Que faire concrètement' — 4 conseils pratiques contextualisés."""
    items=[f"- {a.get('title','')}" for a in all_articles[:20]]
    prompt=f"""Tu es consultant expert tourisme. À partir de la situation actuelle de crise au Moyen-Orient, rédige 4 conseils pratiques et actionnables pour les agents de voyage français.

Chaque conseil comporte :
- Un titre court (5-8 mots)
- Une description pratique (25-40 mots) avec des actions concrètes
- Une icône emoji pertinente

Les 4 conseils doivent couvrir : vérification conditions/contrats, destinations alternatives, suivi alertes officielles, communication clients.

Contextualise avec les faits récents :
{chr(10).join(items)}

JSON uniquement : [{{"icon":"📋","title":"Vérifier les conditions","description":"Consultez les CGV..."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=1500))
    if r and isinstance(r,list) and len(r)>=3: print(f"  Actions : {len(r)} conseils"); return r[:4]
    return None

def mae_groq(mae_data):
    items=[f"- country_key={k} | {v['label']}: niveau={v['level']}. Contenu: {v.get('full_content',v.get('summary',''))[:400]}" for k,v in mae_data.items()]
    prompt=f"""Expert tourisme. Pour chaque pays, fiche conseil 2-3 phrases pour agent de voyage.
IMPORTANT: utilise EXACTEMENT la country_key comme "country".
Pays :
{chr(10).join(items)}
JSON : [{{"country":"liban","conseil_tourisme":"..."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=3000))
    if r and isinstance(r,list):
        m={c["country"]:c.get("conseil_tourisme","") for c in r if "country" in c}
        print(f"  MAE Groq : {len(m)}"); return m
    return None

# ══════════════════════════════════════════════════
# CLASSIFICATION / DÉTECTION
# ══════════════════════════════════════════════════
def det_countries(a,kw):
    text=(a.get("title","")+" "+a.get("description","")).lower()
    return [ck for ck,ckws in kw.get("countries_detect",{}).items() if not ck.startswith("_") and any(k.lower() in text for k in ckws)]
def classif_kw(a,kw):
    text=(a.get("title","")+" "+a.get("description","")).lower()
    scores={cat:sum(1 for k2 in kw[cat]["keywords"] if k2.lower() in text) for cat in kw if cat!="countries_detect"}
    scores={k:v for k,v in scores.items() if v>0}
    return max(scores,key=scores.get) if scores else "general"

# ══════════════════════════════════════════════════
# FINANCE
# ══════════════════════════════════════════════════
def fetch_fin():
    res={}
    for key,cfg in FINANCE_SYMBOLS.items():
        try:
            h=yf.Ticker(cfg["symbol"]).history(start=CONFLICT_START_DATE)
            if h.empty: continue
            cur,st2=float(h["Close"].iloc[-1]),float(h["Close"].iloc[0])
            chg=round(((cur-st2)/st2)*100,2); fx=cfg["sector"]=="forex"
            res[key]={"symbol":cfg["symbol"],"label":cfg["label"],"currency":cfg["currency"],"sector":cfg["sector"],"current_price":round(cur,4 if fx else 2),"start_price":round(st2,4 if fx else 2),"change_pct":chg,"history":[{"date":d.strftime("%Y-%m-%d"),"close":round(float(r["Close"]),2)} for d,r in h.iterrows()],"last_update":datetime.now(timezone.utc).isoformat()}
            print(f"  {cfg['label']} ({chg:+.2f}%)")
        except Exception as e: print(f"  Finance ERR {cfg['symbol']}: {e}")
    return res

# ══════════════════════════════════════════════════
# MAE
# ══════════════════════════════════════════════════
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

# ══════════════════════════════════════════════════
# FIRESTORE — Sync
# ══════════════════════════════════════════════════
def sync_arts(db,articles,kw,gc,cit):
    ref=db.collection("articles"); n=0; img_updated=0
    for i,a in enumerate(articles):
        if not a.get("link"): continue
        did=gid(a["link"])
        existing=ref.document(did).get()
        if existing.exists:
            ed=existing.to_dict()
            if not ed.get("image_url") and a.get("image_url"):
                ref.document(did).update({"image_url":a["image_url"]}); img_updated+=1
            continue
        cat=gc[i] if gc and i in gc else classif_kw(a,kw)
        doc={"title":a["title"],"link":a["link"],"description":a.get("description",""),"image_url":a.get("image_url",""),"author":a.get("author",""),"pub_date":a["pub_date"],"category":cat,"countries":det_countries(a,kw),"created_at":firestore.SERVER_TIMESTAMP}
        if cit and i in cit:
            doc["citation"]=cit[i].get("citation",""); doc["citation_nom"]=cit[i].get("nom",""); doc["citation_fonction"]=cit[i].get("fonction","")
        ref.document(did).set(doc); n+=1; print(f"  + [{cat}] {a['title'][:60]}...")
    print(f"  Articles : {n} nouveaux, {img_updated} images mises à jour"); return n

def sync_fin(db,d):
    for k,v in d.items(): db.collection("market_data").document(k).set(v)
def sync_mae(db,d,ex):
    for k,v in d.items():
        if not v.get("conseil_tourisme") and ex.get(k,{}).get("conseil_tourisme"): v["conseil_tourisme"]=ex[k]["conseil_tourisme"]
        db.collection("mae_alerts").document(k).set(v)
def sync_synth(db,p): db.collection("config").document("synthesis").set({"points":p,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_timeline(db,t): db.collection("config").document("timeline").set({"events":t,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_airlines(db,a): db.collection("config").document("airlines").set({"airlines":a,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_actions(db,a): db.collection("config").document("actions").set({"actions":a,"generated_at":datetime.now(timezone.utc).isoformat()})
def upd_cfg(db,n): db.collection("config").document("radar").set({"last_sync":datetime.now(timezone.utc).isoformat(),"conflict_start_date":CONFLICT_START_DATE,"rss_url":RSS_URL,"last_new_articles":n},merge=True)

# ══════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════
def main():
    print("="*50+f"\nRadar v7 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"+"="*50)
    db=init_fb(); kw=load_kw()

    # 1. Charger les articles existants en base
    print("\n--- Articles existants en base ---")
    existing_links=get_existing_links(db)
    nb_existing=len(existing_links)
    is_initial=nb_existing<INITIAL_THRESHOLD
    print(f"  {nb_existing} articles en base → {'RUN INITIAL' if is_initial else 'RUN INCRÉMENTAL'}")

    # 2. Scraper
    print("\n--- Scraping ---")
    if is_initial:
        scraped=scrape_all_pages()
    else:
        # Mode incrémental : RSS + page 1 HTML pour ne rien rater
        scraped=parse_rss_page()
        print("  Complément page HTML...")
        try:
            r=requests.get(TAGS_PAGE_URL,timeout=30,headers=HDR)
            if r.status_code==200:
                html_arts=parse_html_page(r.content)
                seen={a["link"] for a in scraped if a.get("link")}
                added=0
                for a in html_arts:
                    if a.get("link") and a["link"] not in seen:
                        scraped.append(a); seen.add(a["link"]); added+=1
                if added: print(f"  +{added} articles via HTML (total: {len(scraped)})")
                else: print(f"  Aucun article supplémentaire via HTML")
        except Exception as e: print(f"  HTML ERR: {e}")

    # Filtrer les articles déjà en base
    new_articles=[a for a in scraped if a.get("link") and a["link"] not in existing_links]
    print(f"  {len(new_articles)} nouveaux articles à traiter")

    # 3. Enrichir les images des nouveaux articles
    if new_articles:
        missing=[a for a in new_articles if not a.get("image_url")]
        if missing:
            print(f"\n--- Enrichissement images ({len(missing)} sans image) ---")
            enrich_images(new_articles)

    # 4. Classifier SEULEMENT les nouveaux articles
    gc=None
    if new_articles and GROQ_API_KEY:
        print(f"\n--- Classification Groq ({len(new_articles)} articles) ---")
        gc=classify_groq(new_articles)
        time.sleep(GROQ_PAUSE)

    # Post-classification : forcer edito
    if gc is None: gc={}
    for i,a in enumerate(new_articles):
        author=(a.get("author","") or "").lower().strip()
        title_desc=(a.get("title","")+" "+a.get("description","")).lower()
        if "josette sicsic" in author: gc[i]="edito"
        elif any(k in title_desc for k in ["édito","editorial","éditorial","billet d'humeur"]): gc[i]="edito"
    for i,a in enumerate(new_articles): a["_cat"]=gc[i] if i in gc else classif_kw(a,kw)

    # 5. Citations : seulement sur les nouveaux témoignages
    cit=None
    if new_articles and GROQ_API_KEY:
        temo_idx=[(i,a) for i,a in enumerate(new_articles) if gc.get(i)=="temoignages"][:3]
        if temo_idx:
            print(f"\n--- Citations ({len(temo_idx)} témoignages) ---")
            awc=[]
            for i,a in temo_idx:
                content=scrape_article_content(a["link"]); awc.append((a,content)); time.sleep(0.5)
            raw_cit=citations_groq(awc)
            if raw_cit:
                cit={}
                for li,gi in enumerate([i for i,_ in temo_idx]):
                    if li in raw_cit: cit[gi]=raw_cit[li]
            time.sleep(GROQ_PAUSE)

    # 6. Sync nouveaux articles → Firestore
    n=0
    if new_articles:
        print("\n--- Nouveaux articles → Firestore ---")
        n=sync_arts(db,new_articles,kw,gc,cit)
    elif scraped:
        # Même si 0 nouveaux, mettre à jour les images manquantes
        print("\n--- Mise à jour images ---")
        sync_arts(db,scraped,kw,{},None)

    # 7. Charger TOUS les articles en base pour les blocs IA
    print("\n--- Chargement articles complets pour blocs IA ---")
    all_articles=load_existing_articles(db)
    print(f"  {len(all_articles)} articles en base")

    # 8. Blocs IA (sur TOUS les articles)
    if all_articles and GROQ_API_KEY:
        print("\n--- Synthèse ---")
        pts=synthesis_groq(all_articles)
        if pts: sync_synth(db,pts)
        else: print("  Pas de mise à jour")
        time.sleep(GROQ_PAUSE)

        print("\n--- Timeline ---")
        tl=timeline_groq(all_articles)
        if tl: sync_timeline(db,tl)
        else: print("  Timeline Groq indisponible, on garde l'ancienne en base")
        time.sleep(GROQ_PAUSE)

        print("\n--- Airlines ---")
        al=airlines_groq(all_articles)
        if al: sync_airlines(db,al)
        time.sleep(GROQ_PAUSE)

        # Actions : régénérer seulement si > 24h
        actions_doc=db.collection("config").document("actions").get()
        should_regen_actions=True
        if actions_doc.exists:
            ad=actions_doc.to_dict()
            gen_at=ad.get("generated_at","")
            if gen_at:
                try:
                    last_gen=datetime.fromisoformat(gen_at.replace("Z","+00:00"))
                    if (datetime.now(timezone.utc)-last_gen).total_seconds()<86400:
                        should_regen_actions=False
                        print("\n--- Actions : à jour (< 24h) ---")
                except: pass
        if should_regen_actions:
            print("\n--- Actions Groq ---")
            act=actions_groq(all_articles)
            if act: sync_actions(db,act)
            time.sleep(GROQ_PAUSE)

    # 9. Finance
    print("\n--- Finance ---")
    fd=fetch_fin()
    if fd: sync_fin(db,fd)

    # 10. Featured article
    if all_articles:
        print("\n--- Article à la une ---")
        featured=None
        for a in all_articles[:15]:
            img=a.get("image_url","")
            if img and check_image_url(img):
                pub=a.get("pub_date","")
                if hasattr(pub,"isoformat"): pub=pub.isoformat()
                elif hasattr(pub,"__str__"): pub=str(pub)
                featured={"title":a.get("title",""),"link":a.get("link",""),"description":a.get("description",""),"image_url":img,"author":a.get("author",""),"pub_date":pub}
                print(f"  Featured : {a.get('title','')[:60]}..."); break
        if featured: db.collection("config").document("radar").set({"featured_article":featured},merge=True)

    # 11. MAE
    print("\n--- France Diplomatie ---")
    ex_mae={}
    try:
        for doc in db.collection("mae_alerts").stream(): ex_mae[doc.id]=doc.to_dict()
    except: pass
    mae=scrape_mae()
    if mae and GROQ_API_KEY:
        print("\n--- MAE Groq ---")
        conseils=mae_groq(mae)
        if conseils:
            for ck,c in conseils.items():
                if ck in mae and c: mae[ck]["conseil_tourisme"]=c
    if mae: sync_mae(db,mae,ex_mae)

    upd_cfg(db,n)
    print("\n"+"="*50+f"\nSync terminée — {n} nouveaux articles\n"+"="*50)

if __name__=="__main__": main()
