from __future__ import annotations
from typing import List, Dict, Tuple
from urllib.parse import urlparse, urlunparse, urlencode
from .keywords import KEYWORDS, KEY_SUBSTRINGS
import re, requests
from bs4 import BeautifulSoup

def _tenant_host(netloc: str) -> str:
    host = netloc.lower()
    host = re.sub(r"^internal\-", "", host)
    host = re.sub(r"^(?:careers\-|jobs\-)", "", host)
    return host

def _keep(t: str) -> bool:
    t = (t or "").lower()
    return any(k in t for k in KEY_SUBSTRINGS)

def _brand_from_host(host: str) -> str | None:
    """
    careers.heb.com -> heb
    jobs.tapestry.com -> tapestry
    www.bestbuy.com -> bestbuy
    """
    h = (host or "").lower()
    h = re.sub(r'^(www|careers|jobs|work|careers\-|jobs\-|work\-)\.', '', h)
    parts = h.split('.')
    return parts[0] if len(parts) >= 2 else None

class ICIMSScraper:
    def _search_url(self, careers_url: str) -> str:
        u = urlparse(careers_url or "")
        host = (u.netloc or "").lower()
        scheme = u.scheme or "https"

        if host.endswith("icims.com"):
            return f"{scheme}://{host}/jobs/search?ss=1"

        brand = _brand_from_host(host)
        if brand:
            return f"https://{brand}.icims.com/jobs/search?ss=1"

        return f"{scheme}://{host}/jobs/search?ss=1"

    def fetch(self, company, _session) -> List[Dict]:
        out: List[Dict] = []
        if not company.careers_url:
            return out

        s = requests.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"})

        search = self._search_url(company.careers_url)

        def _parse_listing(url: str) -> List[Dict]:
            try:
                r = s.get(url, timeout=12)
                if r.status_code != 200:
                    return []
                soup = BeautifulSoup(r.text, "html.parser")
                tmp=[]

                anchors = soup.select('a.iCIMS_Anchor[href*="/jobs/"], a[href*="/jobs/"]')
                for a in anchors:
                    t = a.get_text(" ", strip=True) or ""
                    href = a.get("href") or ""
                    if not href or not _keep(t):
                        continue
                    if href.startswith("http"):
                        full = href
                    else:
                        base = r.url.split("/jobs/")[0]
                        full = f"{base}/jobs/{href.split('/jobs/')[-1]}"
                    tmp.append({"title": t or "Data Scientist", "apply_url": full, "source":"icims-html", "snippet": None})

                seen=set(); dedup=[]
                for h in tmp:
                    if h["apply_url"] in seen: 
                        continue
                    seen.add(h["apply_url"]); dedup.append(h)
                return dedup
            except Exception:
                return []

        collected=[]
        for kw in KEYWORDS:
            page = 1
            for _ in range(3):
                try:
                    r = s.get(
                        search,
                        params={"searchKeyword": kw, "searchLocation":"", "ss":"1", "pr": str(page)},
                        timeout=12
                    )
                    if r.status_code != 200:
                        break
                    batch = _parse_listing(r.url)
                    if not batch:
                        break
                    collected.extend(batch)

                    soup = BeautifulSoup(r.text, "html.parser")
                    has_next = bool(soup.select_one('a[aria-label="Next"], a[rel="next"]'))
                    if not has_next:
                        break
                    page += 1
                except Exception:
                    break
            if collected:
                break

        if collected:

            return collected[:300]

        dq = getattr(company, "data_query_url", None)
        if dq:
            try:
                r = s.get(dq, timeout=12)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    tmp=[]
                    for a in soup.select('a[href]'):
                        t = a.get_text(" ", strip=True) or ""
                        href = a.get("href") or ""
                        if not href: 
                            continue
                        low = t.lower() + " " + href.lower()
                        if not _keep(low):
                            continue
                        full = href if href.startswith("http") else r.url.split("?")[0].rstrip("/") + "/" + href.lstrip("/")
                        tmp.append({"title": t or "Data Scientist", "apply_url": full, "source":"icims-fallback", "snippet": None})
                    seen=set(); dedup=[]
                    for h in tmp:
                        if h["apply_url"] in seen: 
                            continue
                        seen.add(h["apply_url"]); dedup.append(h)
                    return dedup[:200]
            except Exception:
                pass

        return out

