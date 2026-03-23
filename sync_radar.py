#!/usr/bin/env python3
"""Radar Crise Moyen-Orient — v6
Adds: full article scraping for temoignages, timeline, airline status via Groq
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
FINANCE_SYMBOLS={"brent":{"symbol":"BZ=F","label":"Brent (baril)","currency":"$","sector":"commodity"},"eurusd":{"symbol":"EURUSD=X","label":"EUR / USD","currency":"","sector":"forex"},"AF.PA":{"symbol":"AF.PA","label":"Air France-KLM","currency":"€","sector":"aerien"},"TUI1.DE":{"symbol":"TUI1.DE","label":"TUI Group","currency":"€","sector":"to"},"AC.PA":{"symbol":"AC.PA","label":"Accor","currency":"€","sector":"hotellerie"},"BKNG":{"symbol":"BKNG","label":"Booking Holdings","currency":"$","sector":"ota"},"CCL":{"symbol":"CCL","label":"Carnival Corp","currency":"$","sector":"croisiere"},"AMS.MC":{"symbol":"AMS.MC","label":"Amadeus IT","currency":"€","sector":"tech"},"AIR.PA":{"symbol":"AIR.PA","label":"Airbus","currency":"€","sector":"aerien"},"RYA.IR":{"symbol":"RYA.IR","label":"Ryanair","currency":"€","sector":"aerien"}}
MAE_SLUGS={"israel":"israel-palestine","liban":"liban","iran":"iran","irak":"irak","syrie":"syrie","jordanie":"jordanie","egypte":"egypte","turquie":"turquie","arabie_saoudite":"arabie-saoudite","emirats":"emirats-arabes-unis","qatar":"qatar","oman":"oman","bahrein":"bahrein","koweit":"koweit","yemen":"yemen","chypre":"chypre","grece":"grece"}
MAE_LABELS={"israel":"Israël / Palestine","liban":"Liban","iran":"Iran","irak":"Irak","syrie":"Syrie","jordanie":"Jordanie","egypte":"Égypte","turquie":"Turquie","arabie_saoudite":"Arabie Saoudite","emirats":"Émirats Arabes Unis","qatar":"Qatar","oman":"Oman","bahrein":"Bahreïn","koweit":"Koweït","yemen":"Yémen","chypre":"Chypre","grece":"Grèce"}
MAE_BASE="https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/conseils-par-pays-destination/"
ALERT_LEVELS=[("formellement déconseillé","formellement_deconseille","red"),("déconseillé sauf raison impérative","deconseille_sauf_ri","orange"),("déconseillé sauf raison","deconseille_sauf_ri","orange"),("vigilance renforcée","vigilance_renforcee","yellow"),("vigilance normale","vigilance_normale","green")]
MAE_GENERIC=["urgence attentat","vigilance renforcée pour les ressortissants français à l'étranger","appel à la vigilance maximale"]
HDR={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept":"text/html,*/*","Accept-Language":"fr-FR,fr;q=0.9"}
KEYWORDS_PATH=Path(__file__).parent/"keywords.json"

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
    """Vérifie qu'une URL d'image est réellement accessible (HEAD request)."""
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
    """Récupère l'image og:image d'un article."""
    try:
        r=requests.get(url,timeout=10,headers=HDR)
        if r.status_code!=200: return ""
        soup=BeautifulSoup(r.content,"html.parser")
        # og:image est le plus fiable
        og=soup.find("meta",property="og:image")
        if og and og.get("content"):
            return vimg(og["content"])
        # Fallback: twitter:image
        tw=soup.find("meta",attrs={"name":"twitter:image"})
        if tw and tw.get("content"):
            return vimg(tw["content"])
        # Fallback: première grande image dans le contenu
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
    """Pour les articles sans image, tente de récupérer og:image."""
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
        # Pré-extraire les citations entre guillemets pour aider Groq
        citations=re.findall(r'[«""\u201c](.{30,500}?)[»""\u201d]',full_text)
        if citations:
            cit_block="\n--- CITATIONS TROUVÉES ENTRE GUILLEMETS ---\n"+"\n".join(f'• «{c}»' for c in citations[:8])
            return full_text[:4000]+cit_block
        return full_text[:5000]
    except Exception as e:
        print(f"  Scrape article ERREUR : {e}"); return ""

# ── Groq ──
def gcall(msgs,mt=2000,retries=2):
    if not GROQ_API_KEY: return None
    for attempt in range(retries):
        try:
            r=requests.post("https://api.groq.com/openai/v1/chat/completions",headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},json={"model":"llama-3.3-70b-versatile","messages":msgs,"max_tokens":mt,"temperature":0.3},timeout=60)
            r.raise_for_status(); return r.json()["choices"][0]["message"]["content"]
        except Exception as e:
            print(f"  Groq ERREUR (tentative {attempt+1}/{retries}) : {e}")
            if attempt<retries-1: time.sleep(3)
    return None
def pj(t):
    if not t: print("  pj: réponse Groq vide"); return None
    c=t.strip()
    if c.startswith("```"): c=re.sub(r'^```\w*\n?','',c).rstrip('`').strip()
    try: return json.loads(c)
    except Exception as e:
        print(f"  pj: JSON invalide — {e}")
        print(f"  pj: début réponse = {c[:200]}")
        # Tenter d'extraire un array JSON même si entouré de texte
        m=re.search(r'\[.*\]',c,re.DOTALL)
        if m:
            try: return json.loads(m.group())
            except: pass
        return None

def classify_groq(articles):
    cats="institutionnel, aerien, croisiere, juridique, solutions, temoignages, contexte, edito"
    items=[f"{i}. {a['title']} — {a.get('description','')[:100]}" for i,a in enumerate(articles)]
    prompt=f"""Classifie chaque article dans UNE catégorie : {cats}, general.
- institutionnel : MAE, diplomatie, rapatriements, conseils voyageurs
- aerien : compagnies, vols, suspensions, reprises, surcharges, aéroports, équipages
- croisiere : paquebots, ports, mer Rouge, canal de Suez
- juridique : droits clients, annulations, remboursements, assurance, force majeure
- solutions : initiatives TO, reprogrammations, destinations alternatives, EDV/SETO
- temoignages : récits agents de voyage, réceptifs, salons pros, vie quotidienne pros
- contexte : analyses géopolitiques, données économiques, études, intentions voyage
- edito : éditorial, billet d'humeur, chronique, opinion, inventaire peurs/craintes, "même pas peur"
- general : si aucune
Articles :
{chr(10).join(items)}
JSON uniquement : [{{"id":0,"cat":"aerien"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}]))
    if r and isinstance(r,list):
        m={c["id"]:c["cat"] for c in r if "id" in c}; print(f"  Groq classif : {len(m)}"); return m
    return None

def synthesis_groq(articles):
    items=[f"- {a['title']}: {a.get('description','')[:200]}" for a in articles[:15]]
    prompt=f"""Tu es journaliste spécialisé tourisme. À partir des articles ci-dessous, rédige EXACTEMENT 6 paragraphes de synthèse sur la crise au Moyen-Orient destinés aux agents de voyage français.

RÈGLES IMPÉRATIVES :
- Chaque paragraphe fait entre 35 et 50 mots (environ 3 lignes), PAS un titre d'article recopié
- Chaque paragraphe est une VRAIE ANALYSE RÉDIGÉE avec des faits concrets, des noms de compagnies/pays/acteurs, et une conséquence pratique pour l'agent de voyage
- Couvre 6 angles différents : aérien, destinations impactées, juridique/annulations, initiatives TO, contexte géopolitique, conseil pratique
- Utilise le présent de l'indicatif
- Exemple de BON format : "Les agents de voyage français doivent être prêts à adapter les itinéraires de leurs clients en raison de la crise au Moyen-Orient, qui affecte les vols et les déplacements dans la région. Les compagnies comme Emirates et Qatar Airways ont réduit leurs fréquences."
- Exemple de MAUVAIS format : "Air France : suspension des vols vers le Liban" (trop court, c'est un titre)

Articles récents :
{chr(10).join(items)}

Réponds UNIQUEMENT avec un JSON array de 6 strings. Rien d'autre."""
    r=pj(gcall([{"role":"user","content":prompt}],mt=2500))
    if r and isinstance(r,list) and len(r)>=3:
        titles_lower={a['title'].lower().strip() for a in articles}
        filtered=[p for p in r if isinstance(p,str) and p.lower().strip() not in titles_lower and len(p)>30]
        if len(filtered)>=3:
            print(f"  Synthèse : {len(filtered)} pts (filtrés)"); return filtered[:6]
        print(f"  Synthèse : {len(r)} pts (non filtrés)"); return r[:6]
    return None

def citations_groq(articles_with_content):
    """Extract REAL verbatim citations from full article content."""
    items=[]
    for i,(a,content) in enumerate(articles_with_content):
        items.append(f'{i}. Titre: "{a["title"]}"\nAuteur article (JOURNALISTE — NE JAMAIS CITER): {a.get("author","")}\nContenu complet: {content[:2000]}')
    prompt=f"""Tu dois extraire des VRAIES CITATIONS VERBATIM (mot pour mot) depuis le contenu des articles ci-dessous.

RÈGLES ABSOLUMENT NON NÉGOCIABLES :
1. Une citation est un DISCOURS DIRECT : des mots réellement prononcés par quelqu'un, généralement entre guillemets « » ou " " dans l'article
2. Le nom est celui de la PERSONNE QUI PARLE dans la citation (un professionnel du tourisme : agent, directeur d'agence, responsable TO, réceptif, hôtelier...), JAMAIS celui du journaliste qui écrit l'article
3. Recopie la citation INTÉGRALEMENT telle qu'elle apparaît dans le texte, entre 2 et 5 phrases. Ne tronque RIEN, ne résume RIEN.
4. Si l'article ne contient AUCUNE citation entre guillemets d'un professionnel du tourisme, renvoie citation="" nom="" fonction=""
5. NE REFORMULE PAS. NE RÉSUME PAS. Recopie mot pour mot.

CRITÈRES DE REJET (renvoyer citation vide) :
- L'article ne contient que des propos du journaliste → citation vide
- Les seules citations sont celles d'un politique ou militaire → citation vide
- Tu ne trouves pas de guillemets dans le texte → citation vide

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
- Chaque événement doit être rédigé comme une VRAIE PHRASE avec sujet, verbe, complément, commençant par une majuscule et se terminant par un point
- Utilise des NOMS PROPRES (compagnies aériennes, pays, organisations, personnes)
- La phrase fait entre 8 et 18 mots
- Exemples de BON format : "Air France suspend tous ses vols vers Beyrouth et Téhéran.", "Le Quai d'Orsay déconseille formellement les voyages au Liban.", "Celestyal Cruises annule ses croisières en Méditerranée orientale."
- Exemples de MAUVAIS format : "Tensions au Moyen-Orient" (pas une phrase), "Turquie : activités touristiques normales" (style télégraphique, pas une phrase)
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
    ref=db.collection("articles"); n=0; img_updated=0
    for i,a in enumerate(articles):
        if not a["link"]: continue
        did=gid(a["link"])
        existing=ref.document(did).get()
        if existing.exists:
            # Mettre à jour l'image si elle manquait et qu'on en a une maintenant
            ed=existing.to_dict()
            if not ed.get("image_url") and a.get("image_url"):
                ref.document(did).update({"image_url":a["image_url"]})
                img_updated+=1
                print(f"  ✎ image ajoutée : {a['title'][:50]}...")
            continue
        cat=gc[i] if gc and i in gc else classif_kw(a,kw)
        doc={"title":a["title"],"link":a["link"],"description":a.get("description",""),"image_url":a.get("image_url",""),"author":a.get("author",""),"pub_date":a["pub_date"],"category":cat,"countries":det_countries(a,kw),"created_at":firestore.SERVER_TIMESTAMP}
        if cit and i in cit:
            doc["citation"]=cit[i].get("citation","")
            doc["citation_nom"]=cit[i].get("nom","")
            doc["citation_fonction"]=cit[i].get("fonction","")
        ref.document(did).set(doc); n+=1; print(f"  + [{cat}] {a['title'][:60]}...")
    print(f"Articles : {n} nouveaux sur {len(articles)}, {img_updated} images mises à jour"); return n

def sync_fin(db,d):
    for k,v in d.items(): db.collection("market_data").document(k).set(v)
def sync_mae(db,d,ex):
    for k,v in d.items():
        if not v.get("conseil_tourisme") and ex.get(k,{}).get("conseil_tourisme"):
            v["conseil_tourisme"]=ex[k]["conseil_tourisme"]
        db.collection("mae_alerts").document(k).set(v)
def sync_synth(db,p): db.collection("config").document("synthesis").set({"points":p,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_timeline(db,t): db.collection("config").document("timeline").set({"events":t,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_airlines(db,a): db.collection("config").document("airlines").set({"airlines":a,"generated_at":datetime.now(timezone.utc).isoformat()})
def upd_cfg(db,n): db.collection("config").document("radar").set({"last_sync":datetime.now(timezone.utc).isoformat(),"conflict_start_date":CONFLICT_START_DATE,"rss_url":RSS_URL,"last_new_articles":n},merge=True)

# ── Main ──
def main():
    print("="*50+f"\nRadar v6 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"+"="*50)
    db=init_fb(); kw=load_kw()

    # Existing MAE for protection
    ex_mae={}
    try:
        for doc in db.collection("mae_alerts").stream(): ex_mae[doc.id]=doc.to_dict()
    except: pass

    print("\n--- RSS ---")
    articles=parse_rss()

    # Enrichir les images manquantes via og:image
    if articles:
        missing=[a for a in articles if not a.get("image_url")]
        if missing:
            print(f"\n--- Enrichissement images ({len(missing)} sans image) ---")
            enrich_images(articles)

    # Classification
    gc=None
    if articles and GROQ_API_KEY:
        print("\n--- Classification Groq ---"); gc=classify_groq(articles)

    # Tag articles with category for airline extraction
    for i,a in enumerate(articles):
        a["_cat"]=gc[i] if gc and i in gc else classif_kw(a,kw)

    # Citations: scrape full content of top 3 temoignages
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

    # Sync articles
    if articles:
        print("\n--- Articles → Firestore ---"); n=sync_arts(db,articles,kw,gc,cit)
    else: n=0

    # Synthèse
    if articles and GROQ_API_KEY:
        print("\n--- Synthèse ---")
        pts=synthesis_groq(articles)
        if pts:
            sync_synth(db,pts)
        else:
            print("  Synthèse Groq indisponible, pas de mise à jour (on garde l'ancienne)")

    # Timeline
    if articles and GROQ_API_KEY:
        print("\n--- Timeline ---")
        tl=timeline_groq(articles)
        if tl: sync_timeline(db,tl)

    # Airlines status
    if articles and GROQ_API_KEY:
        print("\n--- Airlines ---")
        al=airlines_groq(articles)
        if al: sync_airlines(db,al)

    # Finance
    print("\n--- Finance ---")
    fd=fetch_fin()
    if fd: sync_fin(db,fd)

    # Featured article (article à la une avec photo vérifiée)
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

if __name__=="__main__": main()
