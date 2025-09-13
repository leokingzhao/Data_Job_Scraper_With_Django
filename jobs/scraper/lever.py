from __future__ import annotations
from typing import List, Dict
from urllib.parse import urlparse
from .keywords import KEY_SUBSTRINGS
import requests

def handles(self, url_or_company) -> bool:
    url = getattr(url_or_company, "careers_url", url_or_company) or ""
    return ("lever.co" in url.lower()) or (getattr(url_or_company, "ats_type", "") == "lever")

def _keep(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in KEY_SUBSTRINGS)

class LeverScraper:
    def _org(self, url: str) -> str|None:
        u = urlparse(url or "")
        parts = [p for p in u.path.split("/") if p]
        if u.netloc.endswith("jobs.lever.co") and parts:
            return parts[0]
        return None

    def fetch(self, company, session) -> List[Dict]:
        out: List[Dict] = []
        org = self._org(company.careers_url or "")
        if not org: return out
        api = f"https://api.lever.co/v0/postings/{org}?mode=json"
        try:
            r = session.get(api, timeout=10)
            if r.status_code != 200: return out
            posts = r.json() or []
            for p in posts:
                title = (p.get("text") or p.get("title") or "").strip()
                if not _keep(title):
                    continue
                url = p.get("hostedUrl") or p.get("applyUrl") or p.get("url")
                if not url: continue
                out.append({"title": title, "apply_url": url, "source":"lever", "snippet": None})
        except Exception:
            return out
        seen=set(); dedup=[]
        for h in out:
            if h["apply_url"] in seen: continue
            seen.add(h["apply_url"]); dedup.append(h)
        return dedup
