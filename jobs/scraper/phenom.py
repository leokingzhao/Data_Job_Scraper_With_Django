# jobs/scraper/oracle.py
from __future__ import annotations
from typing import List, Dict, Tuple
from urllib.parse import urlparse
from .keywords import KEYWORDS, KEY_SUBSTRINGS
import os, re, requests
from bs4 import BeautifulSoup

ATS_MAX_KW = int(os.getenv("ATS_MAX_KW", "4"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "12"))

def _keep(t: str) -> bool:
    t = (t or "").lower()
    return any(k in t for k in KEY_SUBSTRINGS)

def _log(*a):
    try:
        if os.getenv("VERBOSE", "0") == "1":
            print(*a, flush=True)
    except Exception:
        pass

class OracleCloudScraper:
    def handles(self, url_or_company) -> bool:
        url = getattr(url_or_company, "data_query_url", None) or getattr(url_or_company, "careers_url", None) or str(url_or_company) or ""
        u = urlparse(url.lower())
       
        return (
            u.netloc.endswith("oraclecloud.com")
            or "/hcmui/" in u.path
            or "/candidateexperience/" in u.path
        )

    def _derive(self, careers_url: str) -> Tuple[str|None, str|None, str]:
        u = urlparse(careers_url or "")
        origin = f"{u.scheme}://{u.netloc}" if u.netloc else None
        m = re.search(r"/sites/([^/]+)/?", u.path)
        site = (m.group(1) if m else "CX")
        return origin, site, "en"

    def _site_from_preheat(self, url: str, site_default: str) -> str:
        m = re.search(r"/sites/([^/]+)/", url)
        return m.group(1) if m else site_default

    def _api(self, origin: str, lang: str, site: str) -> str:
        return f"{origin}/hcmUI/CandidateExperience/{lang}/sites/{site}/requisitions"

    def _quick_probe(self, s: requests.Session, api: str) -> int:
        
        try:
            r = s.get(api, params={"keyword":"data","limit":1,"offset":0}, timeout=min(HTTP_TIMEOUT, 8), headers={"Accept":"application/json"})
            return r.status_code
        except Exception:
            return 0

    def _collect(self, items, base_detail: str) -> List[Dict]:
        out=[]; seen=set()
        for it in items or []:
            title = (it.get("Title") or it.get("title") or it.get("PostingTitle") or "").strip()
            if not title or not _keep(title):
                continue
            rid = it.get("Id") or it.get("RequisitionId") or it.get("IdValue")
            url = it.get("ExternalURL") or it.get("url")
            if not url and rid:
                url = f"{base_detail}/{rid}"
            if not url or url in seen:
                continue
            seen.add(url)
            out.append({"title": title, "apply_url": url, "source": "oracle-api", "snippet": None})
        return out

    def fetch(self, company, _session) -> List[Dict]:
        out: List[Dict] = []
        base_url = getattr(company, "data_query_url", None) or getattr(company, "careers_url", None)
        if not base_url or not self.handles(base_url):
            return out

        s = requests.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"})

        origin, site, lang = self._derive(base_url)
        if not origin:
            return out

        
        try:
            pre = s.get(base_url, timeout=min(HTTP_TIMEOUT, 10))
            pre.raise_for_status()
            site = self._site_from_preheat(pre.url, site)
        except Exception:
            pass

        api = self._api(origin, lang, site)
        base_detail = f"{origin}/hcmUI/CandidateExperience/{lang}/sites/{site}/requisition"

      
        status = self._quick_probe(s, api)
        if status == 404:
            _log("ORC fast-fail 404:", api)
            return out

        tried = 0
        for kw in KEYWORDS:
            if tried >= ATS_MAX_KW:
                break
            tried += 1
            try:
                r = s.get(api, params={"keyword":kw,"limit":50,"offset":0}, timeout=HTTP_TIMEOUT, headers={"Accept":"application/json"})
                _log("ORC api:", r.status_code, r.url)
                if r.status_code == 404:
           
                    break
                if r.status_code == 200 and "application/json" in r.headers.get("content-type",""):
                    data = r.json() or {}
                    items = data.get("items") or data.get("requisitions") or data.get("data") or []
                    got = self._collect(items, base_detail)
                    if got:
                        return got
            except Exception:
                continue

        try:
            r = s.get(base_url, timeout=min(HTTP_TIMEOUT, 10))
            if r.status_code != 200:
                return out
            soup = BeautifulSoup(r.text, "html.parser")
            tmp=[]
            for a in soup.select('a[href*="/requisition/"]'):
                t = a.get_text(" ", strip=True) or ""
                if not _keep(t):
                    continue
                href = a.get("href") or ""
                if not href:
                    continue
                url = href if href.startswith("http") else f"{origin}{href}"
                tmp.append({"title": t or "Data Scientist", "apply_url": url, "source": "oracle-html", "snippet": None})
            seen=set(); dedup=[]
            for h in tmp:
                if h["apply_url"] in seen: continue
                seen.add(h["apply_url"]); dedup.append(h)
            return dedup
        except Exception:
            return out
