# ══════════════════════════════════════════════════
# CORRECTION sync_radar.py — REMPLACER toute la fonction synthesis_groq par ceci :
# ══════════════════════════════════════════════════

def synthesis_groq(articles):
    items=[f"- {a['title']}: {a.get('description','')[:200]}" for a in articles[:15]]
    example_json = '[{"tag":"AÉRIEN","text":"**Air France** prolonge la suspension de ses vols vers Téhéran et Beyrouth jusqu\'à fin mai 2026."}]'
    prompt=f"""Tu es journaliste tourisme. Rédige 6 points de synthèse sur la crise au Moyen-Orient pour des agents de voyage français.

RÈGLES :
- Chaque point = UNE SEULE PHRASE de 20 à 30 mots maximum
- La phrase doit être percutante, concrète, avec un nom propre ou un chiffre
- Mets en **gras** 1 à 2 mots-clés (nom de compagnie, pays, chiffre)
- Tags dans cet ordre exact :
  1. "AÉRIEN" 2. "GÉOPOLITIQUE" 3. "DESTINATIONS" 4. "JURIDIQUE" 5. "TOUR-OPÉRATEURS" 6. "CONSEIL"

Articles récents :
{chr(10).join(items)}

Réponds UNIQUEMENT avec un JSON array de 6 objets : {example_json}. Rien d'autre."""
    r=pj(gcall([{"role":"user","content":prompt}],mt=2500))
    if r and isinstance(r,list) and len(r)>=3:
        titles_lower={a['title'].lower().strip() for a in articles}
        processed=[]
        for p in r:
            if isinstance(p,dict) and p.get("text"):
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p["text"])
                tag=p.get("tag","INFO")
                if txt.lower().strip() not in titles_lower and len(txt)>30:
                    processed.append({"tag":tag,"text":txt})
            elif isinstance(p,str) and len(p)>30:
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p)
                if txt.lower().strip() not in titles_lower:
                    processed.append({"tag":"INFO","text":txt})
        if len(processed)>=3:
            print(f"  Synthèse : {len(processed)} pts (avec tags et bold)"); return processed[:6]
        # Fallback
        fallback=[]
        for p in r:
            if isinstance(p,dict) and p.get("text"):
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p["text"])
                fallback.append({"tag":p.get("tag","INFO"),"text":txt})
            elif isinstance(p,str):
                txt=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',p)
                fallback.append({"tag":"INFO","text":txt})
        print(f"  Synthèse : {len(fallback)} pts (fallback)"); return fallback[:6]
    return None
