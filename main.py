import logging, os, time, asyncio
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

app = FastAPI(title="Lex Foncier", docs_url=None, redoc_url=None)

_ip_usage: dict = {}
FREE_LIMIT = int(os.getenv("FREE_LIMIT", "3"))

def _get_ip(req: Request) -> str:
    return req.headers.get("X-Forwarded-For","").split(",")[0].strip() or (req.client.host if req.client else "unknown")

def _check_quota(ip: str) -> tuple[bool, int]:
    day = int(time.time() // 86400)
    e = _ip_usage.get(ip, {"day":0,"count":0})
    if e["day"] != day: e = {"day":day,"count":0}
    c = e["count"]; e["count"]+=1; _ip_usage[ip]=e
    if len(_ip_usage) > 3000:
        for k in list(_ip_usage.keys())[:500]: del _ip_usage[k]
    return c < FREE_LIMIT, c

@app.get("/health")
async def health():
    return {"status":"ok","service":"LexFoncier","version":"4.0"}

@app.get("/diagnostic")
async def diagnostic():
    import httpx, time as t
    urls = [
        ("BAN", "https://api-adresse.data.gouv.fr/search/?q=Lyon&limit=1"),
        ("IGN_carto", "https://apicarto.ign.fr/api/cadastre/commune?code_insee=69123&_limit=1"),
        ("GPU_IGN", "https://apicarto.ign.fr/api/gpu/zone-urba?lon=4.83&lat=45.76"),
        ("Georisques", "https://georisques.gouv.fr/api/v1/gaspar/risques?latlon=4.83,45.76&rayon=500"),
        ("ADEME_69", "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-69/lines?size=1"),
        ("INSEE_geo", "https://geo.api.gouv.fr/communes?code=69123"),
        ("DVF_files", "https://files.data.gouv.fr/geo-dvf/latest/csv/69/communes/69123.csv.gz"),
    ]
    results = {}
    async with httpx.AsyncClient(timeout=6) as client:
        for name, url in urls:
            t0=t.time()
            try:
                r=await client.get(url)
                results[name]={"status":r.status_code,"ms":round((t.time()-t0)*1000),"ok":r.status_code<400}
            except Exception as e:
                results[name]={"status":0,"error":str(e)[:50],"ok":False}
    return results

@app.post("/api/demo")
async def demo_api(request: Request):
    body = await request.json()
    address = body.get("address","").strip()
    dvf_radius = min(int(body.get("dvf_radius",500)), 1000)
    api_key = request.headers.get("X-API-Key","") or body.get("api_key","")
    if len(address) < 5:
        raise HTTPException(400, "Adresse trop courte")
    if not api_key:
        ip = _get_ip(request)
        allowed, used = _check_quota(ip)
        if not allowed:
            return JSONResponse({"success":False,"code":"QUOTA_EXCEEDED",
                "message":f"Limite gratuite atteinte ({FREE_LIMIT} analyses/jour).",
                "cta_url":"/tarifs.html","demo_url":"/rapport-demo",
                "used":used,"limit":FREE_LIMIT}, status_code=429)
    try:
        from tools.analyze_parcel import analyze_parcel
        t0 = time.time()
        result = await analyze_parcel(address, dvf_radius)
        result["_perf"] = {"total_ms": round((time.time()-t0)*1000)}
        if not api_key:
            ip = _get_ip(request)
            used_now = _ip_usage.get(ip,{}).get("count",0)
            result["_quota"] = {"used":used_now,"limit":FREE_LIMIT,"remaining":max(0,FREE_LIMIT-used_now),"mode":"free"}
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"Demo error: {e}", exc_info=True)
        return JSONResponse({"success":False,"erreur":str(e),"address":address}, status_code=500)

@app.get("/rapport")
@app.post("/api/rapport")
async def rapport_endpoint(request: Request):
    if request.method == "GET":
        address = request.query_params.get("address","")
        dvf_radius = int(request.query_params.get("radius",500))
    else:
        body = await request.json()
        address = body.get("address","").strip()
        dvf_radius = int(body.get("dvf_radius",500))
    if len(address) < 5:
        raise HTTPException(400, "address manquant")
    try:
        from tools.analyze_parcel import analyze_parcel
        from rapport import generate_rapport
        data = await analyze_parcel(address, dvf_radius)
        return HTMLResponse(generate_rapport(data))
    except Exception as e:
        return HTMLResponse(f"<h1>Erreur</h1><p>{e}</p>", 500)

def _serve(name):
    f = "static/"+name
    return HTMLResponse(open(f).read() if os.path.exists(f) else f"<h1>{name} introuvable</h1>")

@app.get("/", response_class=HTMLResponse)
async def index(): return _serve("index.html")
@app.get("/tarifs.html", response_class=HTMLResponse)
async def tarifs(): return _serve("tarifs.html")
@app.get("/fonctionnalites.html", response_class=HTMLResponse)
async def fonctionnalites(): return _serve("fonctionnalites.html")
@app.get("/rapport-demo", response_class=HTMLResponse)
async def rapport_demo():
    f="static/rapport-demo.html"
    return HTMLResponse(open(f).read() if os.path.exists(f) else "<h1>Demo non disponible</h1>")
@app.get("/notaires.html", response_class=HTMLResponse)
async def notaires(): return _serve("notaires.html")
@app.get("/agents.html", response_class=HTMLResponse)
async def agents(): return _serve("agents.html")
@app.get("/promoteurs.html", response_class=HTMLResponse)
async def promoteurs(): return _serve("promoteurs.html")
@app.get("/api.html", response_class=HTMLResponse)
async def api_page(): return _serve("api.html")
@app.get("/mentions-legales.html", response_class=HTMLResponse)
async def mentions(): return _serve("mentions-legales.html")

if os.path.isdir("static/assets"):
    app.mount("/assets", StaticFiles(directory="static/assets"), name="assets")

if __name__ == "__main__":
    port = int(os.getenv("PORT","8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
