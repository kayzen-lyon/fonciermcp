"""
tools/analyze_parcel.py v4 — Parallélisme total, timeout agressif, DVF multi-source
"""
import asyncio, logging, time, math, io, csv, gzip, os
import httpx

logger = logging.getLogger(__name__)

async def analyze_parcel(address: str, dvf_radius: int = 500) -> dict:
    t0 = time.time()
    geo = await _geocode(address)
    if not geo:
        return {"success":False,"erreur":f"Adresse introuvable: {address}","address":address}
    lat, lon = geo["lat"], geo["lon"]
    code_insee = geo.get("code_insee","")
    tasks = await asyncio.gather(
        _cadastre(lat, lon),
        _plu(lat, lon),
        _dvf_multi(lat, lon, dvf_radius, code_insee),
        _risks(lat, lon),
        _dpe(lat, lon, code_insee),
        _batiments(lat, lon),
        _commune(code_insee),
        return_exceptions=True
    )
    keys = ["cadastre","plu","dvf","risks","dpe","batiments","commune"]
    result = {"success":True,"meta":{
        "address_input":address,"address_normalized":geo.get("label",address),
        "geocoding_score":geo.get("score",0),"coordinates":{"lat":lat,"lon":lon},
        "code_insee":code_insee,"temps_secondes":round(time.time()-t0,1)
    }}
    for k,v in zip(keys,tasks):
        result[k] = v if not isinstance(v,Exception) else {"erreur":str(v)[:80]}
    return result

async def _geocode(address: str) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get("https://api-adresse.data.gouv.fr/search/",params={"q":address,"limit":1})
            features = r.json().get("features",[])
            if not features: return None
            f=features[0]; props=f.get("properties",{}); coords=f.get("geometry",{}).get("coordinates",[0,0])
            return {"lat":coords[1],"lon":coords[0],"label":props.get("label",""),
                "score":props.get("score",0),"code_insee":props.get("citycode",""),
                "postcode":props.get("postcode",""),"city":props.get("city","")}
    except: return None

async def _cadastre(lat, lon) -> dict:
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r = await c.get("https://apicarto.ign.fr/api/cadastre/parcelle",params={"lon":lon,"lat":lat,"_limit":1})
            if r.status_code!=200: return {"erreur":f"IGN {r.status_code}"}
            feats=r.json().get("features",[])
            if not feats: return {"erreur":"Parcelle non trouvée"}
            p=feats[0].get("properties",{})
            return {"reference":f"{p.get('codecommune','')}{p.get('section','')}{p.get('numero','')}",
                "section":p.get("section",""),"numero":p.get("numero",""),
                "surface_m2":p.get("contenance"),"code_insee":p.get("codecommune",""),"source":"IGN API Carto"}
    except Exception as e: return {"erreur":str(e)[:80]}

async def _plu(lat, lon) -> dict:
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.get("https://apicarto.ign.fr/api/gpu/zone-urba",params={"lon":lon,"lat":lat})
            if r.status_code!=200: return {"erreur":f"GPU {r.status_code}"}
            feats=r.json().get("features",[])
            if not feats: return {"zone_principale":"Non couvert","constructibilite":"Inconnu"}
            best=feats[0].get("properties",{}); tz=best.get("typezone","")
            constructible=tz.startswith("U") and not tz.startswith("AU")
            return {"zone_principale":tz,"libelle":best.get("libelle",""),
                "constructibilite":"Constructible" if constructible else ("À urbaniser" if tz.startswith("AU") else "Non constructible"),
                "constructibilite_detail":best.get("libelong",""),
                "document_urbanisme":{"type":best.get("typedoc",""),"etat":best.get("etat",""),
                    "date_approbation":best.get("dateappr",""),"lien_reglement":best.get("urlfic","")},
                "source":"Géoportail Urbanisme — IGN/DGALN"}
    except Exception as e: return {"erreur":str(e)[:80]}

async def _dvf_multi(lat, lon, radius, code_insee) -> dict:
    normi_token = os.getenv("NORMI_TOKEN","")
    if normi_token:
        r = await _dvf_normi(lat, lon, radius, normi_token)
        if r.get("success"): return r
    r = await _dvf_csv(lat, lon, radius, code_insee)
    if r.get("success"): return r
    return {"success":False,"disponible":False,
        "message":"DVF temporairement indisponible.",
        "lien_direct":f"https://explore.data.gouv.fr/fr/immobilier?lat={lat}&lng={lon}&zoom=16",
        "source":"DVF — DGFiP"}

async def _dvf_normi(lat, lon, radius, token) -> dict:
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.get("https://api.normi.fr/v1/transactions",
                params={"lat":lat,"lng":lon,"radius":radius,"limit":50},
                headers={"Authorization":f"Bearer {token}"})
            if r.status_code!=200: return {"success":False}
            data=r.json(); transactions=data.get("transactions",data.get("data",[]))
            if not transactions: return {"success":True,"ventes_logements":[],"stats":{},"count":0,"source":"Normi DVF"}
            return _build_dvf(transactions, radius, "Normi DVF — DGFiP (certifié)")
    except Exception as e: logger.debug(f"Normi: {e}"); return {"success":False}

async def _dvf_csv(lat, lon, radius, code_insee) -> dict:
    if not code_insee or len(code_insee)<5: return {"success":False}
    dep=code_insee[:2]; 
    if dep=="97": dep=code_insee[:3]
    url=f"https://files.data.gouv.fr/geo-dvf/latest/csv/{dep}/communes/{code_insee}.csv.gz"
    try:
        async with httpx.AsyncClient(timeout=15.0,follow_redirects=True) as c:
            r=await c.get(url)
            if r.status_code!=200: return {"success":False}
            content=gzip.decompress(r.content).decode("utf-8","replace")
            rows=list(csv.DictReader(io.StringIO(content)))
            if not rows: return {"success":True,"ventes_logements":[],"stats":{},"count":0,"source":"DVF CSV"}
            def dist(la2,lo2):
                R=6371000; dlat=math.radians(float(la2)-lat); dlon=math.radians(float(lo2)-lon)
                a=math.sin(dlat/2)**2+math.cos(math.radians(lat))*math.cos(math.radians(float(la2)))*math.sin(dlon/2)**2
                return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))
            ventes,seen=[],set()
            for row in rows:
                mid=row.get("id_mutation","")
                if mid and mid in seen: continue
                if mid: seen.add(mid)
                nature=row.get("nature_mutation","Vente")
                if nature not in ("Vente","Adjudication","Expropriation"): continue
                la2,lo2=row.get("latitude",""),row.get("longitude","")
                try:
                    if la2 and lo2 and dist(la2,lo2)>radius: continue
                except: pass
                px_s=row.get("valeur_fonciere","").replace(",",".").replace(" ","")
                sf_s=row.get("surface_reelle_bati","").replace(",",".")
                if not px_s: continue
                try: px=float(px_s); sf=float(sf_s) if sf_s else None
                except: continue
                if px<10000 or px>20000000: continue
                pm2=round(px/sf) if sf and sf>5 else None
                if pm2 and (pm2<200 or pm2>30000): pm2=None
                ventes.append({"date":row.get("date_mutation",""),"type_bien":row.get("type_local","Bien"),
                    "nature":nature,"valeur_euros":int(px),"surface_bati_m2":int(sf) if sf else None,
                    "prix_m2":pm2,"pieces":row.get("nombre_pieces_principales",""),"code_postal":row.get("code_postal","")})
            ventes.sort(key=lambda x:x.get("date",""),reverse=True)
            return _build_dvf(ventes, radius, "DVF CSV — DGFiP / data.gouv.fr (certifié)")
    except Exception as e: logger.debug(f"DVF CSV: {e}"); return {"success":False}

def _build_dvf(ventes, radius, source):
    pm2=sorted([v["prix_m2"] for v in ventes if v.get("prix_m2") and v["prix_m2"]>100])
    stats={}
    if pm2:
        mid=len(pm2)//2; dates=[v.get("date","") for v in ventes if v.get("date")]
        stats={"prix_m2_median":pm2[mid],"prix_m2_moyen":round(sum(pm2)/len(pm2)),
            "prix_m2_min":pm2[0],"prix_m2_max":pm2[-1],"nb_transactions":len(pm2),
            "periode":f"{min(dates)[:7]} - {max(dates)[:7]}" if dates else ""}
    return {"success":True,"disponible":True,"ventes_logements":ventes[:20],"stats":stats,"count":len(ventes),"radius_m":radius,"source":source}

async def _risks(lat, lon) -> dict:
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            rs=await asyncio.gather(
                c.get(f"https://georisques.gouv.fr/api/v1/gaspar/risques?latlon={lon},{lat}&rayon=500"),
                c.get(f"https://georisques.gouv.fr/api/v1/radon?lon={lon}&lat={lat}"),
                c.get(f"https://georisques.gouv.fr/api/v1/zonage_argiles?lon={lon}&lat={lat}"),
                return_exceptions=True)
            result={"source":"Géorisques — BRGM"}
            if not isinstance(rs[0],Exception) and rs[0].status_code==200:
                d=rs[0].json(); raw=d.get("data",d.get("features",[]))
                risques=[{"code":r.get("codeRisque",r.get("code_risque","")),"libelle":r.get("libelleRisque",r.get("libelle_risque",r.get("libelle","")))} for r in (raw if isinstance(raw,list) else []) if r.get("libelleRisque") or r.get("libelle")]
                result["risques_gaspar"]=risques; result["nb_risques"]=len(risques)
            if not isinstance(rs[1],Exception) and rs[1].status_code==200:
                d=rs[1].json(); cl=d.get("data",[{}])[0].get("classeRadon","") if d.get("data") else d.get("classe_radon","")
                if cl: result["radon"]={"classe":str(cl),"libelle":{"1":"Faible","2":"Notable","3":"Élevé"}.get(str(cl),"Cl."+str(cl))}
            if not isinstance(rs[2],Exception) and rs[2].status_code==200:
                d=rs[2].json(); exp=d.get("data",[{}])[0].get("exposition","") if d.get("data") else d.get("exposition","")
                if exp: result["argiles"]={"exposition":exp}
            return result
    except Exception as e: return {"erreur":str(e)[:80]}

async def _dpe(lat, lon, code_insee) -> dict:
    dep=code_insee[:2] if code_insee else "69"
    if dep=="97" and code_insee: dep=code_insee[:3]
    slug=f"dpe-{dep}"
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r=await c.get(f"https://data.ademe.fr/data-fair/api/v1/datasets/{slug}/lines",
                params={"size":8,"sort":"-Date_réception_DPE","geo_distance":f"{lon},{lat},300"})
            if r.status_code!=200: return {"erreur":f"ADEME {r.status_code}","nb_dpe_proches":0}
            dpes_raw=r.json().get("results",[])
            dpes=[{"etiquette_dpe":d.get("Etiquette_DPE",d.get("etiquette_dpe","")),
                "etiquette_ges":d.get("Etiquette_GES",d.get("etiquette_ges","")),
                "adresse":d.get("Adresse_(BAN)",d.get("adresse","")),
                "distance_m":round(float(d.get("_geo_distance",0))) if d.get("_geo_distance") else None}
                for d in dpes_raw if d.get("Etiquette_DPE") or d.get("etiquette_dpe")]
            return {"dpe_adresse":dpes,"nb_dpe_proches":len(dpes),"source":f"ADEME DPE dep.{dep}"}
    except Exception as e: return {"erreur":str(e)[:80],"nb_dpe_proches":0}

async def _batiments(lat, lon) -> dict:
    try:
        async with httpx.AsyncClient(timeout=6.0) as c:
            r=await c.get("https://data.geopf.fr/wfs/ows",params={
                "SERVICE":"WFS","VERSION":"2.0.0","REQUEST":"GetFeature",
                "TYPENAMES":"BDTOPO_V3:batiment","COUNT":"10","OUTPUTFORMAT":"application/json","CRS":"CRS:84",
                "BBOX":f"{lon-0.003},{lat-0.003},{lon+0.003},{lat+0.003},CRS:84"})
            if r.status_code!=200: return {"erreur":f"BDTOPO {r.status_code}","nb_batiments":0}
            feats=r.json().get("features",[])
            bats=[{"usage":p.get("usage_1",p.get("USAGE1","")),"nb_logements":p.get("nombre_logements"),
                "nb_etages":p.get("nombre_d_etages"),"hauteur":p.get("hauteur"),"etat":p.get("etat_de_l_objet","En service")}
                for f in feats[:8] for p in [f.get("properties",{})] if p.get("usage_1") or p.get("USAGE1")]
            return {"batiments":bats,"nb_batiments":len(bats),"nb_avec_logements":sum(1 for b in bats if b.get("nb_logements")),"source":"IGN BDTOPO v3"}
    except Exception as e: return {"erreur":str(e)[:80],"nb_batiments":0}

async def _commune(code_insee) -> dict:
    if not code_insee: return {}
    try:
        async with httpx.AsyncClient(timeout=4.0) as c:
            r=await c.get(f"https://geo.api.gouv.fr/communes/{code_insee}",params={"fields":"nom,population,surface,departement,region,codesPostaux"})
            if r.status_code!=200: return {}
            d=r.json()
            return {"nom":d.get("nom",""),"population":d.get("population"),"surface_km2":round(d.get("surface",0)/100,1),
                "departement":d.get("departement",{}).get("nom",""),"region":d.get("region",{}).get("nom",""),
                "codes_postaux":d.get("codesPostaux",[]),"source":"INSEE Géo"}
    except Exception as e: return {"erreur":str(e)[:60]}
