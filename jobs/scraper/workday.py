# jobs/scraper/workday.py

from __future__ import annotations
import os
import json
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
import time
import requests


US_ONLY = os.getenv("WD_US_ONLY", "1") == "1"

US_STATE_ABBR = {
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la","me","md","ma","mi",
    "mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut",
    "vt","va","wa","wv","wi","wy","dc"
}

TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))
MAX_PAGES = int(os.getenv("WD_MAX_PAGES", "6"))
TERMS = [t.strip() for t in (os.getenv("WD_TERMS") or "data").split(",") if t.strip()]

SEARCH_TERMS = ["data","analytics","machine learning","ml","business intelligence"]
TERMS_RE = re.compile(r"(data|analytics|machine learning|ml|business intelligence)", re.I)

class WorkdayScraper:
    def _session(self, session: Optional[requests.Session], referer: str) -> requests.Session:
        s = session or requests.Session()
        s.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
            "Accept-Language": "en-US,en;q=0.9",
        })
        return s

    def handles(self, url: str) -> bool:
        
        if not url:
            return False
        u = urlparse(url)
        host = (u.netloc or "").lower()
        path = (u.path or "").lower()
        
        return (
            "myworkdayjobs.com" in host
            or "workday" in host
            or "wd" in host
            or "/wday/" in path
            or "/workday" in path
        )

    def _build_api(self, careers_url: str) -> Tuple[str, str, str]:
        u = urlparse(careers_url)
        scheme = u.scheme or "https"
        netloc = u.netloc
        tenant = (netloc.split(".") or [""])[0]
        parts = [seg for seg in u.path.split("/") if seg]
        if parts and re.match(r"^[a-z]{2}-[A-Z]{2}$", parts[0]):
            parts = parts[1:]
        site = None
        if "details" in parts:
            i = parts.index("details")
            if i > 0:
                site = parts[i - 1]
        if not site and "job" in parts:
            i = parts.index("job")
            if i > 0:
                site = parts[i - 1]
        if not site and parts:
            cleaned = [x for x in parts if x not in ("wday","cxs","api","job","jobs","details")]
            if cleaned:
                site = cleaned[0]
        if site == "jobs" and len(parts) >= 2:
            j = parts.index("jobs")
            if j > 0:
                site = parts[j - 1]
        if not tenant or not site:
            raise ValueError(f"Cannot parse Workday tenant/site from: {careers_url}")
        api = f"{scheme}://{netloc}/wday/cxs/{tenant}/{site}/jobs"
        api = re.sub(r"/jobs/?$", "/jobs", api)
        return api, netloc, site

    def _is_us(self, locations_text: str) -> bool:
        t = (locations_text or "").lower()
        if any(k in t for k in ("united states","usa","u.s.","u.s.a","us (remote)","remote - us","remote, us","remote in the us")):
            return True
        for ab in US_STATE_ABBR:
            if f", {ab}" in t or f" {ab})" in t or f" {ab} " in t or f"-{ab} " in t:
                return True
        non_us = (
            "canada","united kingdom","uk","germany","france","spain","portugal","italy","netherlands","belgium",
            "switzerland","austria","ireland","sweden","norway","denmark","finland","poland","czech","slovak",
            "romania","hungary","turkey","israel","uae","saudi","qatar","egypt","south africa","nigeria","kenya",
            "mexico","brazil","argentina","chile","peru","colombia","australia","new zealand","india","china",
            "japan","korea","singapore","hong kong","taiwan","malaysia","indonesia","thailand","philippines"
        )
        if any(k in t for k in non_us):
            return False
        return True

    def _apply_url(self, host: str, site: str, external_path: str) -> Optional[str]:
        if not external_path:
            return None
        p = external_path if external_path.startswith("/") else f"/{external_path}"
        if p.startswith("/en-") or p.startswith(f"/{site}/"):
            return f"https://{host}{p}"
        return f"https://{host}/{site}{p}"

    def fetch(self, company, session: Optional[requests.Session] = None) -> List[Dict]:
        # 1) 入口：优先 data_query_url，其次 careers_url
        entry_url = getattr(company, "data_query_url", None) or (company.careers_url or "")
        try:
            api, host, site = self._build_api(entry_url)
        except Exception:
            return []
        s = self._session(session, entry_url or f"https://{host}/{site}")
        out: List[Dict] = []
        seen = set()

        limit = 20
        terms = TERMS if TERMS else ["data"]  # TERMS 来自环境变量 WD_TERMS / 默认 "data"

        # 4) 关键词 + 翻页拉取
        for term in terms:
            offset = 0
            for _ in range(MAX_PAGES):
                payload = {"limit": limit, "offset": offset, "searchText": term, "appliedFacets": {}}
                try:
                    r = s.post(api, json=payload, timeout=TIMEOUT)

                    # 明确错误或临时错误重试
                    if r.status_code in (400, 404):
                        break
                    if r.status_code in (502, 503, 504):
                        r = s.post(api, json=payload, timeout=TIMEOUT)

                    r.raise_for_status()
                    j = r.json() or {}
                except Exception:
                    break

                postings = j.get("jobPostings") or []
                if not postings:
                    break

                for p in postings:
                    title = (p.get("title") or "").strip()
                    loc = (p.get("locationsText") or "").strip()
                    if US_ONLY and not self._is_us(loc):
                        continue
                    url = self._apply_url(host, site, p.get("externalPath") or "")
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    out.append({
                        "title": title or "Data Role",
                        "apply_url": url,
                        "source": "workday-api",
                        "snippet": loc,
                    })

                offset += limit
                if len(postings) < limit:
                    break

        return out

