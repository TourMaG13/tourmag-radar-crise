#!/usr/bin/env python3
"""Radar Crise Moyen-Orient — v7
Basé sur v6.3 (qui fonctionnait) + corrections + FlightAware + 17 indicateurs
"""
import json,hashlib,os,re,sys,time
from datetime import datetime,timezone,timedelta
from pathlib import Path
import feedparser,requests,yfinance as yf
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials,firestore

RSS_URL=os.getenv("RSS_URL","https://www.tourmag.com/xml/syndication.rss?t=crise+golfe")
CONFLICT_START_DATE=os.getenv("CONFLICT_START_DATE","2025-10-01")
ANTHROPIC_API_KEY=os.getenv("ANTHROPIC_API_KEY","")
FLIGHTAWARE_API_KEY=os.getenv("FLIGHTAWARE_API_KEY","")
ME_AIRPORTS={"BEY":"Beyrouth","TLV":"Tel-Aviv","THR":"Téhéran","IKA":"Téhéran (Imam Khomeini)","AMM":"Amman","CAI":"Le Caire","IST":"Istanbul","DXB":"Dubaï","DOH":"Doha","RUH":"Riyad","JED":"Djeddah","MCT":"Mascate","BAH":"Bahreïn","KWI":"Koweït","AUH":"Abu Dhabi","SSH":"Charm el-Cheikh","HRG":"Hurghada","LCA":"Larnaca","AYT":"Antalya","BGW":"Bagdad","DAM":"Damas","SAH":"Sanaa"}
FINANCE_SYMBOLS={"brent":{"symbol":"BZ=F","label":"Brent (baril)","currency":"$","sector":"commodity"},"eurusd":{"symbol":"EURUSD=X","label":"EUR / USD","currency":"","sector":"forex"},"AF.PA":{"symbol":"AF.PA","label":"Air France-KLM","currency":"€","sector":"aerien"},"TUI1.DE":{"symbol":"TUI1.DE","label":"TUI Group","currency":"€","sector":"to"},"AC.PA":{"symbol":"AC.PA","label":"Accor","currency":"€","sector":"hotellerie"},"BKNG":{"symbol":"BKNG","label":"Booking Holdings","currency":"$","sector":"ota"},"CCL":{"symbol":"CCL","label":"Carnival Corp","currency":"$","sector":"croisiere"},"AMS.MC":{"symbol":"AMS.MC","label":"Amadeus IT","currency":"€","sector":"tech"},"AIR.PA":{"symbol":"AIR.PA","label":"Airbus","currency":"€","sector":"aerien"},"RYA.IR":{"symbol":"RYA.IR","label":"Ryanair","currency":"€","sector":"aerien"},"IAG.L":{"symbol":"IAG.L","label":"IAG (British Airways)","currency":"£","sector":"aerien"},"LHA.DE":{"symbol":"LHA.DE","label":"Lufthansa","currency":"€","sector":"aerien"},"EXPE":{"symbol":"EXPE","label":"Expedia","currency":"$","sector":"ota"},"MAR":{"symbol":"MAR","label":"Marriott","currency":"$","sector":"hotellerie"},"RCL":{"symbol":"RCL","label":"Royal Caribbean","currency":"$","sector":"croisiere"},"HLT":{"symbol":"HLT","label":"Hilton","currency":"$","sector":"hotellerie"},"GC=F":{"symbol":"GC=F","label":"Or (once)","currency":"$","sector":"commodity"}}
MAE_SLUGS={"israel":"israel-palestine","liban":"liban","iran":"iran","irak":"irak","syrie":"syrie","jordanie":"jordanie","egypte":"egypte","turquie":"turquie","arabie_saoudite":"arabie-saoudite","emirats":"emirats-arabes-unis","qatar":"qatar","oman":"oman","yemen":"yemen","chypre":"chypre","grece":"grece"}
MAE_SLUGS_ALT={"bahrein":"https://www.diplomatie.gouv.fr/fr/dossiers-pays/bahrein/","koweit":"https://www.diplomatie.gouv.fr/fr/dossiers-pays/koweit/"}
MAE_LABELS={"israel":"Israël / Palestine","liban":"Liban","iran":"Iran","irak":"Irak","syrie":"Syrie","jordanie":"Jordanie","egypte":"Égypte","turquie":"Turquie","arabie_saoudite":"Arabie Saoudite","emirats":"Émirats Arabes Unis","qatar":"Qatar","oman":"Oman","bahrein":"Bahreïn","koweit":"Koweït","yemen":"Yémen","chypre":"Chypre","grece":"Grèce"}
MAE_BASE="https://www.diplomatie.gouv.fr/fr/conseils-aux-voyageurs/conseils-par-pays-destination/"
ALERT_LEVELS=[("formellement déconseillé","formellement_deconseille","red"),("déconseillé sauf raison impérative","deconseille_sauf_ri","orange"),("déconseillé sauf raison","deconseille_sauf_ri","orange"),("sont déconseillés","deconseille","orange"),("est déconseillé","deconseille","orange"),("voyages déconseillés","deconseille","orange"),("vigilance renforcée","vigilance_renforcee","yellow"),("vigilance normale","vigilance_normale","green")]
MAE_GENERIC=["urgence attentat","vigilance renforcée pour les ressortissants français à l'étranger","appel à la vigilance maximale"]
HDR={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept":"text/html,*/*","Accept-Language":"fr-FR,fr;q=0.9"}
KEYWORDS_PATH=Path(__file__).parent/"keywords.json"
EDITO_TAGS=["expert","spokojny","guena","remi duchange","futuroscopie","eric didier","mazzola","duthion","camille le guilloux","cousin","jean pinard","messager","habibou","du boucher","daniel borja","delporte","gallo","jansen","pointet","ramond","rodolphe lenoir","guillaume vigneron"]
AI_PAUSE=5

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

def has_edito_tag(tags):
    if not tags: return False
    tags_lower=[t.lower().strip() for t in tags]
    for et in EDITO_TAGS:
        for t in tags_lower:
            if et in t: return True
    return False

def scrape_tags(url):
    try:
        r=requests.get(url,timeout=10,headers=HDR)
        if r.status_code!=200: return []
        soup=BeautifulSoup(r.content,"html.parser")
        tags=[]
        for el in soup.find_all(string=re.compile(r'Tags?\s*:',re.IGNORECASE)):
            parent=el.find_parent()
            if parent:
                for a in parent.find_all("a"):
                    t=a.get_text(strip=True).lower()
                    if t and 1<len(t)<80: tags.append(t)
                break
        if not tags:
            meta=soup.find("meta",attrs={"name":"keywords"})
            if meta and meta.get("content"):
                tags=[t.strip().lower() for t in meta["content"].split(",") if 1<len(t.strip())<80]
        return tags
    except: return []

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
    print(f"  HTML fallback : {len(arts)} articles",flush=True); return arts

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
        print(f"  RSS : {len(arts)} articles",flush=True); return arts
    except Exception as ex: print(f"  ERREUR RSS : {ex}",flush=True); return []

def scrape_og_image(url):
    try:
        r=requests.get(url,timeout=10,headers=HDR)
        if r.status_code!=200: return ""
        soup=BeautifulSoup(r.content,"html.parser")
        og=soup.find("meta",property="og:image")
        if og and og.get("content"): return vimg(og["content"])
        return ""
    except: return ""

def enrich_images(articles):
    n=0
    for a in articles:
        if not a.get("image_url"):
            img=scrape_og_image(a["link"])
            if img: a["image_url"]=img; n+=1
            time.sleep(0.2)
    print(f"  Images enrichies : {n}",flush=True)

def scrape_article_content(url):
    try:
        r=requests.get(url,timeout=15,headers=HDR)
        if r.status_code!=200: return ""
        soup=BeautifulSoup(r.content,"html.parser")
        body=soup.find("div",class_="contenu") or soup.find("article") or soup
        paras=[p.get_text(strip=True) for p in body.find_all("p") if len(p.get_text(strip=True))>20]
        full=" ".join(paras)
        extras=""
        cits=re.findall(r'[«"\u201c](.{30,800}?)[»"\u201d]',full)
        if cits: extras+="\n--- CITATIONS ---\n"+"\n".join(f'• «{c}»' for c in cits[:8])
        return full[:4000]+extras
    except: return ""

def gcall(msgs,mt=2000,retries=3):
    if not ANTHROPIC_API_KEY: return None
    user_content=""
    for m in msgs:
        if m["role"]=="user": user_content+=m["content"]+"\n"
    for attempt in range(retries):
        try:
            r=requests.post("https://api.anthropic.com/v1/messages",headers={"x-api-key":ANTHROPIC_API_KEY,"content-type":"application/json","anthropic-version":"2023-06-01"},json={"model":"claude-haiku-4-5-20251001","max_tokens":mt,"messages":[{"role":"user","content":user_content}]},timeout=90)
            if r.status_code==429:
                w=int(r.headers.get("retry-after","30"))
                print(f"  Claude 429 — {w}s",flush=True); time.sleep(w); continue
            if r.status_code==529:
                print(f"  Claude 529 (overloaded) — 30s",flush=True); time.sleep(30); continue
            r.raise_for_status()
            data=r.json()
            text=""
            for block in data.get("content",[]):
                if block.get("type")=="text": text+=block.get("text","")
            return text
        except Exception as e:
            print(f"  Claude ERR ({attempt+1}): {e}",flush=True)
            if attempt<retries-1: time.sleep(10)
    return None

def pj(t):
    if not t: return None
    c=t.strip()
    if c.startswith("```"): c=re.sub(r'^```\w*\n?','',c).rstrip('`').strip()
    try: return json.loads(c)
    except:
        m=re.search(r'\[.*\]',c,re.DOTALL)
        if m:
            try: return json.loads(m.group())
            except: pass
        return None

def classify_groq(articles):
    items=[f"{i}. {a['title']} — {a.get('description','')[:120]} — auteur: {a.get('author','')}" for i,a in enumerate(articles)]
    prompt=f"""Classifie chaque article dans UNE catégorie : institutionnel, aerien, croisiere, juridique, solutions, temoignages, geopolitique, economie, destinations, edito, general.
Si auteur "Josette Sicsic" ou titre contient "édito"/"éditorial"/"billet" → edito.
Articles :
{chr(10).join(items)}
JSON : [{{"id":0,"cat":"aerien"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}]))
    if r and isinstance(r,list):
        m={c["id"]:c["cat"] for c in r if "id" in c}; print(f"  Classif : {len(m)}",flush=True); return m
    return None

def synthesis_groq(articles):
    items=[f"- {a['title']}: {a.get('description','')[:200]}" for a in articles[:15]]
    prompt=f"""Tu es un journaliste français spécialisé dans le tourisme professionnel. Rédige 6 points de synthèse sur la crise au Moyen-Orient destinés à des agents de voyage.

CONSIGNES DE RÉDACTION IMPÉRATIVES :
- Chaque point doit être rédigé dans un français irréprochable, fluide et naturel.
- Écris des phrases complètes avec sujet, verbe et complément. Ne jamais omettre les pronoms, articles ou prépositions.
- Utilise des tournures journalistiques élégantes, pas de style télégraphique ni de mots-clés alignés.
- Chaque point fait 40 mots MAXIMUM. Sois concis mais élégant.
- Mets en **gras** uniquement 1 à 2 mots-clés importants par point (pas plus).
- Utilise des noms propres (compagnies, pays, institutions) pour être concret.
- Varie les tags : n'utilise pas deux fois le même tag dans les 6 points.

Tags disponibles : AÉRIEN, GÉOPOLITIQUE, DESTINATIONS, JURIDIQUE, TOUR-OPÉRATEURS, CONSEIL, CROISIÈRE, ÉCONOMIE, HÔTELLERIE, ASSURANCE, COMPAGNIES, TECHNOLOGIE.

Format : JSON array de 6 objets avec "tag" et "text".

Articles récents à synthétiser :
{chr(10).join(items)}

Réponds UNIQUEMENT avec le JSON, sans commentaire."""
    r=pj(gcall([{"role":"user","content":prompt}],mt=2500))
    if r and isinstance(r,list) and len(r)>=3:
        titles={a['title'].lower().strip() for a in articles}
        out=[]
        for p in r:
            if isinstance(p,dict) and p.get("text"):
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p["text"])
                if txt.lower().strip() not in titles and len(txt)>20:
                    out.append({"tag":p.get("tag","INFO"),"text":txt})
            elif isinstance(p,str) and len(p)>30:
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p)
                if txt.lower().strip() not in titles:
                    out.append({"tag":"INFO","text":txt})
        if len(out)>=3: print(f"  Synthèse : {len(out)} pts",flush=True); return out[:6]
    return None

def citations_groq(awc):
    items=[f'{i}. "{a["title"]}"\nAuteur: {a.get("author","")}\n{content[:2500]}' for i,(a,content) in enumerate(awc)]
    prompt=f"""Extrais citations VERBATIM. Nom=personne qui parle (pas journaliste). Fonction+entreprise.
{chr(10).join(items)}
JSON : [{{"id":0,"citation":"...","nom":"Prénom Nom","fonction":"Poste, Entreprise"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=3000))
    if r and isinstance(r,list):
        return {c["id"]:{"citation":c.get("citation",""),"nom":c.get("nom",""),"fonction":c.get("fonction","")} for c in r if "id" in c and c.get("citation","")}
    return None

def timeline_groq(articles):
    items=[f"- [{a.get('pub_date','').isoformat()[:10] if a.get('pub_date') else '?'}] {a['title']}" for a in articles[:25]]
    prompt=f"""Tu es journaliste français. Extrais les 6-8 événements LES PLUS RÉCENTS de la crise au Moyen-Orient à partir de ces articles, dans l'ordre chronologique.

RÈGLES IMPÉRATIVES :
- UNIQUEMENT les événements récents (derniers jours/semaines). Pas d'événements anciens.
- Chaque événement est UNE PHRASE COMPLÈTE en français fluide avec SUJET + VERBE + COMPLÉMENT
- Utilise des noms propres (Air France, Israël, Emirates, Quai d'Orsay...)
- 10 à 18 mots par phrase
- Utilise les vraies dates des articles

Articles (du plus récent au plus ancien) :
{chr(10).join(items)}

JSON uniquement : [{{"date":"2026-03-28","event":"Air France prolonge la suspension de ses vols vers Téhéran."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=1500))
    if r and isinstance(r,list):
        for ev in r:
            if not ev.get("date"): ev["date"]="9999-99-99"
        r.sort(key=lambda e:e.get("date","9999"))
        print(f"  Timeline : {len(r)}",flush=True); return r
    return None

def airlines_groq(articles):
    aero=[a for a in articles if a.get("_cat")=="aerien"]
    if not aero: return None
    items=[f"- {a['title']}: {a.get('description','')[:120]}" for a in aero[:10]]
    prompt=f"""Statut compagnies aériennes Moyen-Orient. Nom, statut (suspendu/perturbé/opérationnel), détail.
{chr(10).join(items)}
JSON : [{{"compagnie":"Air France","statut":"suspendu","detail":"Vols suspendus vers Liban et Iran"}}]"""
    r=pj(gcall([{"role":"user","content":prompt}]))
    if r and isinstance(r,list): print(f"  Airlines : {len(r)}",flush=True); return r
    return None

def mae_groq(mae_data):
    items=[f"- country_key={k} | {v['label']}: {v['level']}. {v.get('full_content',v.get('summary',''))[:400]}" for k,v in mae_data.items()]
    prompt=f"""Expert tourisme. Pour chaque pays, rédige un conseil pratique de 2-3 phrases pour un agent de voyage français.

RÈGLES :
- Mentionne les risques concrets et zones à éviter
- NE RÉPÈTE PAS le niveau d'alerte (vigilance renforcée, déconseillé, etc.) car il est déjà affiché séparément
- N'écris PAS en MAJUSCULES (pas de VIGILANCE, SUSPENDU, VENDABLE, etc.)
- N'utilise PAS les mots "vendable" ou "à suspendre"
- Donne des conseils pratiques : quoi dire au client, alternatives, précautions
- Utilise EXACTEMENT la country_key comme "country"

Pays :
{chr(10).join(items)}
JSON : [{{"country":"liban","conseil_tourisme":"Les frappes touchent le sud et la banlieue de Beyrouth. L'aéroport fonctionne par intermittence. Orientez les clients vers Chypre ou la Grèce."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=3000))
    if r and isinstance(r,list):
        return {c["country"]:c.get("conseil_tourisme","") for c in r if "country" in c}
    return None

def det_countries(a,kw):
    text=(a["title"]+" "+a.get("description","")).lower()
    return [ck for ck,ckws in kw.get("countries_detect",{}).items() if not ck.startswith("_") and any(k.lower() in text for k in ckws)]

def classif_kw(a,kw):
    text=(a["title"]+" "+a.get("description","")).lower()
    scores={cat:sum(1 for k2 in kw[cat]["keywords"] if k2.lower() in text) for cat in kw if cat!="countries_detect"}
    scores={k:v for k,v in scores.items() if v>0}
    return max(scores,key=scores.get) if scores else "general"

# ===================== FLIGHTAWARE =====================

FLIGHTAWARE_DESTINATIONS={"DXB":"Dubaï","DOH":"Doha (Hamad)","AUH":"Abu Dhabi","TLV":"Tel-Aviv (Ben Gourion)","MCT":"Mascate","AMM":"Amman (Queen Alia)"}

def _fa_format_time(dt_str):
    """Convertit un datetime UTC en heure Paris HHhMM"""
    if not dt_str: return ""
    try:
        t=datetime.fromisoformat(dt_str.replace("Z","+00:00"))
        try:
            from zoneinfo import ZoneInfo as ZI
        except ImportError:
            from backports.zoneinfo import ZoneInfo as ZI
        return t.astimezone(ZI("Europe/Paris")).strftime("%Hh%M")
    except: return ""

def _fa_classify(f):
    """Classifie un vol FlightAware depuis un segment aplati"""
    if f.get("cancelled"): return "cancelled","Annulé"
    if f.get("diverted"): return "diverted","Dérouté"
    actual_off=f.get("actual_off","")
    actual_on=f.get("actual_on","")
    progress=f.get("progress_percent",0) or 0
    if actual_on: return "landed","Atterri"
    if actual_off: return "active","En vol"
    return "scheduled","Programmé"

def _fa_build_detail(f,status):
    """Construit le détail affiché pour un vol"""
    if status=="cancelled": return "Annulé"
    if status=="diverted": return "Dérouté"
    scheduled=f.get("scheduled_out","")
    actual=f.get("actual_out","") or f.get("actual_off","")
    estimated=f.get("estimated_out","")
    progress=f.get("progress_percent",0) or 0
    delay=f.get("departure_delay",0) or 0
    if status=="active":
        dep_time=_fa_format_time(actual or scheduled)
        detail=f"En vol ({progress}%)" if progress else "En vol"
        if dep_time: detail+=f" · Départ {dep_time}"
        return detail
    if status=="landed":
        landed_time=_fa_format_time(f.get("actual_on","") or f.get("actual_in",""))
        return f"Atterri · {landed_time}" if landed_time else "Atterri"
    # scheduled
    sched_time=_fa_format_time(scheduled)
    if delay and delay>300:
        est_time=_fa_format_time(estimated)
        return f"Retardé · {est_time or sched_time} (+{delay//60}min)"
    gate=f.get("gate_origin","")
    terminal=f.get("terminal_origin","")
    detail=f"Programmé · {sched_time}" if sched_time else "Programmé"
    if terminal: detail+=f" · T{terminal}"
    if gate: detail+=f" Porte {gate}"
    return detail

def _fa_get_airline(f):
    """Récupère le nom de la compagnie"""
    op=f.get("operator","")
    if op: return op
    op_iata=f.get("operator_iata","")
    if op_iata: return op_iata
    return f.get("ident_iata","") or f.get("ident","") or "Inconnu"

def fetch_flightaware(db):
    if not FLIGHTAWARE_API_KEY: return None
    # Guard horaire : uniquement entre 6h et 22h heure de Paris
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    paris_now=datetime.now(ZoneInfo("Europe/Paris"))
    if paris_now.hour<6 or paris_now.hour>=22:
        print(f"  FlightAware : hors plage horaire ({paris_now.strftime('%Hh%M')} Paris), skip",flush=True)
        return None
    # Vérifier si dernier check < 3h
    try:
        doc=db.collection("config").document("airlines").get()
        if doc.exists:
            d=doc.to_dict()
            rt=d.get("realtime",{})
            last=rt.get("last_check","")
            if last:
                try:
                    last_dt=datetime.fromisoformat(last.replace("Z","+00:00"))
                    diff=(datetime.now(timezone.utc)-last_dt).total_seconds()
                    if diff<10800:
                        print(f"  FlightAware : dernier check il y a {int(diff//60)}min, skip (min 3h)",flush=True)
                        return rt
                except: pass
    except: pass

    try:
        headers={"x-apikey":FLIGHTAWARE_API_KEY,"Accept":"application/json"}

        def _fetch_by_dest(direction_label):
            print(f"  FlightAware {direction_label}...",flush=True)
            now_utc=datetime.now(timezone.utc)
            start_of_day=now_utc.strftime("%Y-%m-%dT00:00:00Z")
            end_of_day=now_utc.strftime("%Y-%m-%dT23:59:59Z")
            dests=[]
            for iata,city in FLIGHTAWARE_DESTINATIONS.items():
                if direction_label=="departs":
                    url=f"https://aeroapi.flightaware.com/aeroapi/airports/CDG/flights/to/{iata}"
                else:
                    url=f"https://aeroapi.flightaware.com/aeroapi/airports/{iata}/flights/to/CDG"
                print(f"    {city} ({iata})...",flush=True)
                try:
                    r=requests.get(url,params={"type":"Airline","start":start_of_day,"end":end_of_day},headers=headers,timeout=30)
                    if r.status_code==429:
                        print(f"    Rate limit 429 — attente 60s",flush=True)
                        time.sleep(60)
                        r=requests.get(url,params={"type":"Airline","start":start_of_day,"end":end_of_day},headers=headers,timeout=30)
                    if r.status_code!=200:
                        print(f"    HTTP {r.status_code}: {r.text[:200]}",flush=True)
                        continue
                    data=r.json()
                    raw_flights=data.get("flights",[])
                    if not raw_flights:
                        # Chercher dans d'autres clés possibles
                        for k in data:
                            if isinstance(data[k],list) and len(data[k])>0:
                                raw_flights=data[k]; break

                    # Aplatir les segments : chaque élément contient "segments"
                    flights=[]
                    for item in raw_flights:
                        segs=item.get("segments",[])
                        if segs and isinstance(segs,list):
                            for seg in segs:
                                flights.append(seg)
                        elif "ident" in item:
                            # Pas de segments, format plat
                            flights.append(item)

                    print(f"    → {len(flights)} vols (aplatis)",flush=True)
                    if not flights: continue

                    # Parser chaque vol
                    dest_flights=[]
                    for f in flights:
                        if f.get("position_only"): continue
                        status,status_label=_fa_classify(f)
                        flight_num=f.get("ident_iata","") or f.get("ident","")
                        # Filtrer les codeshares : numéro à 4 chiffres ou plus = codeshare
                        digits=re.sub(r'[^0-9]','',flight_num)
                        if len(digits)>=4: continue
                        airline=_fa_get_airline(f)
                        detail=_fa_build_detail(f,status)
                        dest_flights.append({
                            "airline":airline,
                            "flight":flight_num,
                            "status":status,
                            "status_label":detail
                        })

                    # Dédupliquer par numéro de vol
                    seen={}
                    STATUS_PRIORITY={"active":5,"landed":4,"cancelled":3,"diverted":3,"scheduled":2,"unknown":0}
                    for fl in dest_flights:
                        fn=fl["flight"]
                        if not fn: continue
                        if fn not in seen or STATUS_PRIORITY.get(fl["status"],0)>STATUS_PRIORITY.get(seen[fn]["status"],0):
                            seen[fn]=fl
                    deduped=list(seen.values())
                    if deduped:
                        dests.append({"city":city,"iata":iata,"flights":deduped})
                        print(f"    → {len(deduped)} vols uniques",flush=True)
                except Exception as e:
                    print(f"    ERR {iata}: {e}",flush=True)
                time.sleep(10)
            return dests

        departs=_fetch_by_dest("departs")
        time.sleep(10)
        retours=_fetch_by_dest("retours")

        result={"departs":departs,"retours":retours,"destinations":departs,"last_check":datetime.now(timezone.utc).isoformat()}
        print(f"  FlightAware résultat: {len(departs)} destinations départ, {len(retours)} destinations retour",flush=True)
        return result
    except Exception as e:
        print(f"  FlightAware ERR: {e}",flush=True)
        import traceback; traceback.print_exc()
        return None

# ===================== FIN FLIGHTAWARE =====================

def fetch_fin():
    res={}
    for key,cfg in FINANCE_SYMBOLS.items():
        try:
            h=yf.Ticker(cfg["symbol"]).history(start=CONFLICT_START_DATE)
            if h.empty: continue
            cur,st2=float(h["Close"].iloc[-1]),float(h["Close"].iloc[0])
            chg=round(((cur-st2)/st2)*100,2); fx=cfg["sector"]=="forex"
            res[key]={"symbol":cfg["symbol"],"label":cfg["label"],"currency":cfg["currency"],"sector":cfg["sector"],"current_price":round(cur,4 if fx else 2),"start_price":round(st2,4 if fx else 2),"change_pct":chg,"history":[{"date":d.strftime("%Y-%m-%d"),"close":round(float(r["Close"]),2)} for d,r in h.iterrows()],"last_update":datetime.now(timezone.utc).isoformat()}
            print(f"  {cfg['label']} ({chg:+.2f}%)",flush=True)
        except Exception as e: print(f"  Finance ERR {cfg['symbol']}: {e}",flush=True)
    return res

def scrape_mae():
    res={}
    # Construire la liste complète des pays avec leur URL
    all_countries={}
    for ck,slug in MAE_SLUGS.items():
        all_countries[ck]=f"{MAE_BASE}{slug}/"
    for ck,url in MAE_SLUGS_ALT.items():
        all_countries[ck]=url
    for ck,url in all_countries.items():
        try:
            r=requests.get(url,timeout=15,headers=HDR)
            if r.status_code!=200: res[ck]=_mfb(ck,url,f"HTTP {r.status_code}"); continue
            soup=BeautifulSoup(r.content,"html.parser")
            full_text=soup.get_text().replace('\n',' ').lower()
            ap=[p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True))>=15]
            rel=[t for t in ap if any(k in t.lower() for k in ["déconseillé","vigilance","quitter","se rendre","recommandé","risque","frappes","prudence","sécurité","zone","éviter","déplacement","frontière","aéroport"]) and not any(g in t.lower() for g in MAE_GENERIC)]
            rt=" ".join(rel).lower()
            found=[(lt,cd,co) for lt,cd,co in ALERT_LEVELS if lt in rt]
            # Fallback : chercher dans le texte complet de la page si rien trouvé dans les paragraphes filtrés
            if not found:
                found=[(lt,cd,co) for lt,cd,co in ALERT_LEVELS if lt in full_text]
            if found:
                ip=len(found)>1
                if ip: least,worst=found[-1],found[0]; ll=f"{least[0].capitalize()} (certaines zones : {worst[0]})"; lc,lcl=least[1],least[2]
                else: ll,lc,lcl=found[0][0].capitalize(),found[0][1],found[0][2]; ip=False
            else: ll,lc,lcl,ip="Non déterminé","unknown","gray",False
            fc=" ".join(rel)[:1500]; ss=" ".join(rel[:3])[:500]
            upd=""; um=re.search(r'Dernière mise à jour[^\d]*(\d{1,2}\s+\w+\s+\d{4})',soup.get_text().replace('\n',' '))
            if um: upd=um.group(1).strip()
            res[ck]={"country":ck,"label":MAE_LABELS.get(ck,ck),"level":ll,"level_code":lc,"color":lcl,"is_partial":ip,"summary":ss,"full_content":fc,"url":url,"last_update_mae":upd,"conseil_tourisme":"","last_scraped":datetime.now(timezone.utc).isoformat()}
        except Exception as e: res[ck]=_mfb(ck,url,str(e)[:200])
    print(f"  MAE : {len(res)} pays",flush=True); return res

def _mfb(ck,url,msg):
    return {"country":ck,"label":MAE_LABELS.get(ck,ck),"level":"Indisponible","level_code":"unknown","color":"gray","is_partial":False,"summary":msg,"full_content":"","url":url,"last_update_mae":"","conseil_tourisme":"","last_scraped":datetime.now(timezone.utc).isoformat()}

def sync_arts(db,articles,kw,gc,cit):
    ref=db.collection("articles"); n=0
    for i,a in enumerate(articles):
        if not a.get("link"): continue
        did=gid(a["link"])
        tags=a.get("_tags",[])
        existing=ref.document(did).get()
        if existing.exists:
            ed=existing.to_dict()
            updates={}
            if not ed.get("image_url") and a.get("image_url"): updates["image_url"]=a["image_url"]
            if not ed.get("tags") and tags: updates["tags"]=tags
            if has_edito_tag(tags) and ed.get("category")!="edito": updates["category"]="edito"
            if updates: ref.document(did).update(updates)
            continue
        cat=gc.get(i,classif_kw(a,kw)) if gc else classif_kw(a,kw)
        if has_edito_tag(tags) and cat!="edito": cat="edito"
        doc={"title":a["title"],"link":a["link"],"description":a.get("description",""),"image_url":a.get("image_url",""),"author":a.get("author",""),"pub_date":a["pub_date"],"category":cat,"countries":det_countries(a,kw),"tags":tags,"created_at":firestore.SERVER_TIMESTAMP}
        if cit and i in cit:
            doc["citation"]=cit[i].get("citation",""); doc["citation_nom"]=cit[i].get("nom",""); doc["citation_fonction"]=cit[i].get("fonction","")
        ref.document(did).set(doc); n+=1; print(f"  + [{cat}] {a['title'][:55]}",flush=True)
    print(f"  {n} nouveaux",flush=True); return n

def sync_fin(db,d):
    for k,v in d.items(): db.collection("market_data").document(k).set(v)
def sync_mae(db,d,ex):
    for k,v in d.items():
        if not v.get("conseil_tourisme") and ex.get(k,{}).get("conseil_tourisme"): v["conseil_tourisme"]=ex[k]["conseil_tourisme"]
        db.collection("mae_alerts").document(k).set(v)
def sync_synth(db,p): db.collection("config").document("synthesis").set({"points":p,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_timeline(db,t): db.collection("config").document("timeline").set({"events":t,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_conseils(db,c): db.collection("config").document("conseils_pratiques").set({"conseils":c,"generated_at":datetime.now(timezone.utc).isoformat()})
def sync_airlines(db,a,rt=None):
    doc={"airlines":a,"generated_at":datetime.now(timezone.utc).isoformat()}
    if rt: doc["realtime"]=rt
    db.collection("config").document("airlines").set(doc)
def upd_cfg(db,n): db.collection("config").document("radar").set({"last_sync":datetime.now(timezone.utc).isoformat(),"conflict_start_date":CONFLICT_START_DATE,"rss_url":RSS_URL,"last_new_articles":n},merge=True)

CONSEILS_ICONS={
    "annulation":'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg>',
    "destination":'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>',
    "alerte":'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18M3 10h18M5 6l7-3 7 3M4 10v11M20 10v11M8 14v3M12 14v3M16 14v3"/></svg>',
    "assurance":'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>',
    "client":'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>',
    "finance":'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1v22M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
    "information":'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>',
    "avion":'<svg width="16" height="16" viewBox="0 0 24 24" fill="#92400E" stroke="none"><path d="M21 16v-2l-8-5V3.5A1.5 1.5 0 0 0 11.5 2 1.5 1.5 0 0 0 10 3.5V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5l8 2.5z"/></svg>'
}

def conseils_groq(articles):
    items=[f"- {a['title']}: {a.get('description','')[:150]}" for a in articles[:15]]
    icons_list=", ".join(CONSEILS_ICONS.keys())
    prompt=f"""Tu es un expert du tourisme professionnel français. Génère exactement 3 conseils pratiques et concrets pour les agents de voyage, en lien avec la crise au Moyen-Orient.

CONSIGNES :
- Chaque conseil a un titre court (4-6 mots) et un texte explicatif (15-25 mots).
- Les conseils doivent être actionnables et directement utiles pour un agent de voyage.
- Le ton est professionnel et rassurant, pas alarmiste.
- Chaque conseil a une icône parmi : {icons_list}
- Varie les icônes entre les 3 conseils.

Articles récents :
{chr(10).join(items)}

Réponds UNIQUEMENT en JSON : [{{"icon":"annulation","titre":"Vérifier les CGV","texte":"Consultez les conditions de force majeure de vos TO partenaires avant de confirmer les réservations."}}]"""
    r=pj(gcall([{"role":"user","content":prompt}],mt=1500))
    if r and isinstance(r,list) and len(r)>=2:
        out=[]
        for c in r[:3]:
            if isinstance(c,dict) and c.get("titre") and c.get("texte"):
                icon_key=c.get("icon","information")
                if icon_key not in CONSEILS_ICONS: icon_key="information"
                out.append({"icon":icon_key,"titre":c["titre"],"texte":c["texte"]})
        if len(out)>=2: print(f"  Conseils : {len(out)}",flush=True); return out
    return None

def main():
    print("="*50,flush=True)
    print(f"Radar v7 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",flush=True)
    print("="*50,flush=True)
    db=init_fb(); kw=load_kw()

    ex_mae={}
    try:
        for doc in db.collection("mae_alerts").stream(): ex_mae[doc.id]=doc.to_dict()
    except: pass

    print("\n--- RSS ---",flush=True)
    articles=parse_rss()
    if not articles: print("  Aucun article",flush=True)

    print("  Complément HTML...",flush=True)
    try:
        r=requests.get("https://www.tourmag.com/tags/crise+golfe/",timeout=30,headers=HDR)
        if r.status_code==200:
            html_arts=parse_html_fb(r.content)
            seen={a["link"] for a in articles if a.get("link")}
            added=0
            for a in html_arts:
                if a.get("link") and a["link"] not in seen:
                    articles.append(a); seen.add(a["link"]); added+=1
            if added: print(f"  +{added} via HTML (total: {len(articles)})",flush=True)
    except Exception as e: print(f"  HTML ERR: {e}",flush=True)

    if articles:
        missing=[a for a in articles if not a.get("image_url")]
        if missing:
            print(f"\n--- Images ({len(missing)}) ---",flush=True)
            enrich_images(articles)

    if articles:
        existing_links=set()
        try:
            for doc in db.collection("articles").stream():
                d=doc.to_dict()
                if d.get("link"): existing_links.add(d["link"])
        except: pass
        new_only=[a for a in articles if a.get("link") and a["link"] not in existing_links]
        if new_only:
            print(f"\n--- Tags ({len(new_only)} nouveaux) ---",flush=True)
            for a in new_only:
                a["_tags"]=scrape_tags(a["link"])
                time.sleep(0.2)
        else:
            print("\n--- Tags : 0 nouveaux ---",flush=True)

    gc=None
    if articles and ANTHROPIC_API_KEY:
        print("\n--- Classification ---",flush=True)
        gc=classify_groq(articles)
        time.sleep(AI_PAUSE)
    if gc is None: gc={}

    for i,a in enumerate(articles):
        author=(a.get("author","") or "").lower()
        td=(a.get("title","")+" "+a.get("description","")).lower()
        if "josette sicsic" in author: gc[i]="edito"
        elif any(kw_e in td for kw_e in ["édito","editorial","éditorial","billet d'humeur","futuroscopie","expert"]): gc[i]="edito"
        tags=a.get("_tags",[])
        if has_edito_tag(tags) and gc.get(i)!="edito": gc[i]="edito"
    for i,a in enumerate(articles): a["_cat"]=gc.get(i,classif_kw(a,kw))

    cit=None
    if articles and ANTHROPIC_API_KEY:
        temo=[(i,a) for i,a in enumerate(articles) if gc.get(i)=="temoignages"][:3]
        if temo:
            print(f"\n--- Citations ({len(temo)}) ---",flush=True)
            awc=[(a,scrape_article_content(a["link"])) for _,a in temo]
            cit_raw=citations_groq(awc)
            if cit_raw:
                cit={}
                for li,gi in enumerate([i for i,_ in temo]):
                    if li in cit_raw: cit[gi]=cit_raw[li]
            time.sleep(AI_PAUSE)

    if articles:
        print("\n--- Firestore ---",flush=True)
        n=sync_arts(db,articles,kw,gc,cit)
    else: n=0

    all_articles=[]
    try:
        for doc in db.collection("articles").order_by("pub_date",direction=firestore.Query.DESCENDING).limit(30).stream():
            all_articles.append(doc.to_dict())
        print(f"\n--- {len(all_articles)} articles en base pour IA ---",flush=True)
    except Exception as e:
        print(f"  Erreur chargement articles: {e}",flush=True)
        all_articles=articles

    if all_articles and ANTHROPIC_API_KEY:
        print("\n--- Synthèse ---",flush=True)
        pts=synthesis_groq(all_articles)
        if pts: sync_synth(db,pts)
        time.sleep(AI_PAUSE)

    if all_articles and ANTHROPIC_API_KEY:
        print("\n--- Conseils pratiques ---",flush=True)
        cpr=conseils_groq(all_articles)
        if cpr: sync_conseils(db,cpr)
        time.sleep(AI_PAUSE)

    if all_articles and ANTHROPIC_API_KEY:
        print("\n--- Timeline ---",flush=True)
        tl=timeline_groq(all_articles)
        if tl: sync_timeline(db,tl)
        time.sleep(AI_PAUSE)

    rt=None
    if FLIGHTAWARE_API_KEY:
        print("\n--- FlightAware ---",flush=True)
        rt=fetch_flightaware(db)
    if rt:
        print(f"  Airlines sync: {len(rt.get('departs',[]))} departs, {len(rt.get('retours',[]))} retours",flush=True)
        sync_airlines(db,[],rt)

    print("\n--- Finance ---",flush=True)
    fd=fetch_fin()
    if fd: sync_fin(db,fd)

    if articles:
        print("\n--- Featured ---",flush=True)
        for a in articles[:15]:
            img=a.get("image_url","")
            if img and check_image_url(img):
                pub=a["pub_date"].isoformat() if a.get("pub_date") else ""
                db.collection("config").document("radar").set({"featured_article":{"title":a["title"],"link":a["link"],"description":a.get("description",""),"image_url":img,"author":a.get("author",""),"pub_date":pub}},merge=True)
                print(f"  {a['title'][:50]}",flush=True); break

    print("\n--- MAE ---",flush=True)
    mae=scrape_mae()
    if mae and ANTHROPIC_API_KEY:
        print("\n--- MAE Groq ---",flush=True)
        conseils=mae_groq(mae)
        if conseils:
            for ck,c in conseils.items():
                if ck in mae and c: mae[ck]["conseil_tourisme"]=c
    if mae: sync_mae(db,mae,ex_mae)

    upd_cfg(db,n)
    print(f"\nTerminé — {n} nouveaux articles",flush=True)

if __name__=="__main__": main()
