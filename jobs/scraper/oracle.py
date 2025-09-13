from __future__ import annotations
from typing import List, Dict, Tuple
from urllib.parse import urlparse
from .keywords import KEYWORDS, KEY_SUBSTRINGS
import re, requests
from bs4 import BeautifulSoup

def _keep(t: str) -> bool:
    t = (t or "").lower()
    return any(k in t for k in KEY_SUBSTRINGS)

def _log(*a): 
    try: print(*a, flush=True)
    except Exception: pass

class OracleCloudScraper:
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
            if not url: 
                continue
            if url in seen: 
                continue
            seen.add(url)
            out.append({"title": title, "apply_url": url, "source": "oracle-api", "snippet": None})
        return out

    def fetch(self, company, _session) -> List[Dict]:
        out: List[Dict] = []
        if not company.careers_url: 
            return out

        origin, site, lang = self._derive(company.careers_url)
        if not origin:
            return out

        s = requests.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"})
        try:
            pre = s.get(company.careers_url, timeout=10)
            pre.raise_for_status()
            pre_url = pre.url
        except Exception:
            pre_url = company.careers_url

        site = self._site_from_preheat(pre_url, site)
        api = self._api(origin, lang, site)
        base_detail = f"{origin}/hcmUI/CandidateExperience/{lang}/sites/{site}/requisition"

        for kw in KEYWORDS:
            try:
                r = s.get(api, params={"keyword":kw,"limit":50,"offset":0}, timeout=10, headers={"Accept":"application/json"})
                _log("ORC DEBUG api:", r.status_code, r.url)
                if r.status_code == 200 and r.headers.get("content-type","").startswith("application/json"):
                    data = r.json() or {}
                    items = data.get("items") or data.get("requisitions") or data.get("data") or []
                    got = self._collect(items, base_detail)
                    if got:
                        return got
            except Exception as e:
                _log("ORC DEBUG api EXC:", repr(e))

        try:
            r = s.get(pre_url, timeout=10)
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
        except Exception as e:
            _log("ORC DEBUG html EXC:", repr(e))

        return out
