# jobs/scraper/successfactors.py
from __future__ import annotations
from typing import List, Dict
from urllib.parse import urlparse, parse_qs, urljoin
from .keywords import KEYWORDS, KEY_SUBSTRINGS
import os, requests
from bs4 import BeautifulSoup

ATS_MAX_KW = int(os.getenv("ATS_MAX_KW", "4"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "12"))

def _keep(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in KEY_SUBSTRINGS)

def _log(*a):
    try:
        if os.getenv("VERBOSE", "0") == "1":
            print(*a, flush=True)
    except Exception:
        pass

class SuccessFactorsScraper:
    def handles(self, url_or_company) -> bool:
        url = getattr(url_or_company, "data_query_url", None) or getattr(url_or_company, "careers_url", None) or str(url_or_company) or ""
        u = urlparse(url.lower())
        return ("successfactors.com" in u.netloc) or ("careersection" in u.path) or ("sfcareer" in u.path)

    def _derive(self, careers_url: str):
        u = urlparse(careers_url or "")
        origin = f"{u.scheme}://{u.netloc}" if u.netloc else None
        qs = parse_qs(u.query)
        company = (qs.get("company") or [""])[0] or None
        return origin, company

    def _api_search(self, s: requests.Session, origin: str, company: str|None):
        path = "/careersection/rest/jobboard/search"
        url  = origin + path
        out=[]

        
        try:
            r0 = s.get(url, params={"company":company or "", "keyword":"data", "lang":"en_US", "location":""},
                       headers={"Accept":"application/json"}, timeout=min(HTTP_TIMEOUT, 8))
            if r0.status_code == 404:
                _log("SF fast-fail 404:", r0.url)
                return []
        except Exception:
            return []

        tried = 0
        for kw in KEYWORDS:
            if tried >= ATS_MAX_KW:
                break
            tried += 1
            try:
                r = s.get(url, params={"company":company or "", "keyword":kw, "lang":"en_US", "location":""},
                          headers={"Accept":"application/json"}, timeout=HTTP_TIMEOUT)
                _log("SF api:", r.status_code, r.url)
                if r.status_code == 404:
                    break
                if r.status_code != 200 or "application/json" not in r.headers.get("content-type",""):
                    continue
                data = r.json() or {}
                items = data.get("jobPostings") or data.get("jobs") or data.get("requisitionList") or []
                for p in items:
                    title = (p.get("title") or p.get("jobTitle") or p.get("displayJobTitle") or "").strip()
                    href  = (p.get("externalPath") or p.get("jobUrl") or p.get("url") or p.get("jobPostingUrl") or "")
                    if not title or not _keep(title):
                        continue
                    if not href:
                        jobid = p.get("jobId") or p.get("id")
                        if jobid:
                            href = f"/careersection/jobdetail.ftl?job={jobid}"
                    if not href: 
                        continue
                    full = href if href.startswith("http") else urljoin(origin, href.lstrip("/"))
                    out.append({"title": title, "apply_url": full, "source": "successfactors-api", "snippet": None})
            except Exception:
                continue

        seen=set(); dedup=[]
        for h in out:
            if h["apply_url"] in seen: continue
            seen.add(h["apply_url"]); dedup.append(h)
        return dedup

    def _html_search(self, s: requests.Session, careers_url: str):
        try:
            r = s.get(careers_url, timeout=min(HTTP_TIMEOUT, 10))
            _log("SF html:", r.status_code, r.url)
            if r.status_code != 200:
                return []
            soup = BeautifulSoup(r.text, "html.parser")
            out=[]
            for a in soup.select('a[href*="job"]'):
                t = a.get_text(" ", strip=True)
                href = a.get("href") or ""
                if not href: continue
                if not _keep(t) and "scientist" not in (href or "").lower():
                    continue
                full = href if href.startswith("http") else urljoin(r.url, href.lstrip("/"))
                out.append({"title": t or "Data Scientist", "apply_url": full, "source": "successfactors-html", "snippet": None})
            #
            seen=set(); dedup=[]
            for h in out:
                if h["apply_url"] in seen: continue
                seen.add(h["apply_url"]); dedup.append(h)
            return dedup
        except Exception:
            return []

    def fetch(self, company, _session) -> List[Dict]:
        out: List[Dict] = []
        base = getattr(company, "data_query_url", None) or getattr(company, "careers_url", None)
        if not base or not self.handles(base):
            return out
        origin, comp = self._derive(base)
        if not origin:
            return out
        s = requests.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"})
        out = self._api_search(s, origin, comp)
        if out:
            return out
        return self._html_search(s, base)
