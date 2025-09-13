from __future__ import annotations
from typing import List, Dict
from urllib.parse import urlparse
from .keywords import KEY_SUBSTRINGS
import requests

def _keep(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in KEY_SUBSTRINGS)

class SmartRecruitersScraper:
    def _company(self, url: str) -> str|None:
        u = urlparse(url or "")
        parts = [p for p in u.path.split("/") if p]
        if u.netloc.endswith("smartrecruiters.com") and parts:
            return parts[0]
        return None

    def fetch(self, company, session) -> List[Dict]:
        out: List[Dict] = []
        comp = self._company(company.careers_url or "")
        if not comp: return out
        api = f"https://api.smartrecruiters.com/v1/companies/{comp}/postings"
        try:
            r = session.get(api, params={"q":"data","limit":200,"offset":0}, timeout=10) 
            if r.status_code != 200: return out
            data = r.json() or {}
            jobs = data.get("content") or data.get("data") or []
            for j in jobs:
                title = (j.get("name") or "").strip()
                if not _keep(title): 
                    continue
                url = j.get("referralUrl") or j.get("applyUrl") or j.get("postingUrl") or j.get("externalPath")
                if not url: continue
                out.append({"title": title, "apply_url": url, "source":"smartrecruiters", "snippet": None})
        except Exception:
            return out
        seen=set(); dedup=[]
        for h in out:
            if h["apply_url"] in seen: continue
            seen.add(h["apply_url"]); dedup.append(h)
        return dedup
