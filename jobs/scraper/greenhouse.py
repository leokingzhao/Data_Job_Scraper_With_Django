# jobs/scraper/greenhouse.py
from __future__ import annotations
from typing import List, Dict, Optional
from urllib.parse import urlparse, urljoin
import re, json
from .base import vlog
from .base import BaseScraper, categorize_title

GH_FOR_RE   = re.compile(r"[?&]for=([a-z0-9\-_]+)", re.I)
GH_DATA_RE  = re.compile(r'data-gh-(?:for|org)\s*=\s*["\']([a-z0-9\-_]+)["\']', re.I)
GH_LINK_RE  = re.compile(r'https?://boards\.greenhouse\.io/(?:embed/)?([a-z0-9\-_]+)(?:/|["\'?])', re.I)
PROBE_PATHS = ("/careers/jobs", "/careers", "/jobs", "/search")

class GreenhouseScraper(BaseScraper):
    name = "greenhouse-api"

    def handles(self, url_or_company) -> bool:
        url = getattr(url_or_company, "careers_url", url_or_company) or ""
        u = (url or "").lower()
        return ("greenhouse.io" in u) or (getattr(url_or_company, "ats_type", "") == "greenhouse")

    def _token_from_boards_url(self, url: str) -> Optional[str]:
        u = urlparse(url or "")
        if "greenhouse.io" not in (u.netloc or ""):
            return None
        parts = [p for p in (u.path or "").split("/") if p]
        return parts[-1] if parts else None

    def _token_from_html(self, html: str) -> Optional[str]:
        if not html:
            return None
        m = GH_FOR_RE.search(html) or GH_DATA_RE.search(html) or GH_LINK_RE.search(html)
        return m.group(1) if m else None

    def _probe_common_paths(self, url: str, session) -> Optional[str]:
        u = urlparse(url or "")
        origin = f"{u.scheme}://{u.netloc}" if u.netloc else None
        if not origin:
            return None
        for p in PROBE_PATHS:
            try:
                r = session.get(urljoin(origin, p), timeout=10)
                if r.status_code == 200:
                    t = self._token_from_html(r.text or "")
                    if t:
                        return t
            except Exception:
                continue
        return None

    def _guess_tokens(self, company, url: str) -> List[str]:

        host = (urlparse(url).netloc or "").lower()
        brand = (host.split(".", 1)[0] or "").strip("-_.")
        name = (getattr(company, "name", "") or "").strip().lower()

        def norm(s: str) -> str:
            s = re.sub(r"[^a-z0-9\-]+", "-", s.lower()).strip("-")
            s = re.sub(r"-(inc|llc|ltd|co|corp|corporation|company)$", "", s)
            return s

        cands = []
        for raw in {brand, name, name.replace("&", "and")}:
            if not raw:
                continue
            slug = norm(raw)
            if slug:
                cands.extend({slug, slug.replace("-", ""), slug.split("-")[0]})

        seen = set(); out=[]
        for t in cands:
            if t and t not in seen:
                seen.add(t); out.append(t)
        return out[:6]

    def _api_ok(self, session, token: str) -> bool:
        try:
            r = session.get(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true", timeout=8)
            if r.status_code != 200:
                return False
            j = r.json()
            return isinstance(j, dict) and "jobs" in j
        except Exception:
            return False

    def _board_token(self, url_or_company, session=None) -> Optional[str]:
        url = getattr(url_or_company, "careers_url", url_or_company) or ""

        token = self._token_from_boards_url(url)
        if token:
            return token

        if session is not None:
            try:
                r = session.get(url, timeout=10)
                if r.status_code == 200:
                    token = self._token_from_html(r.text or "")
                    if token:
                        return token
            except Exception:
                pass

        if session is not None:
            token = self._probe_common_paths(url, session)
            if token:
                return token

        if session is not None:
            for cand in self._guess_tokens(getattr(url_or_company, "company", url_or_company), url):
                if self._api_ok(session, cand):
                    return cand

        return None

    def fetch(self, company, session) -> List[Dict]:
        out: List[Dict] = []
        if not getattr(company, "careers_url", None):
            return out

        token = self._board_token(company, session=session)
        try:
            vlog(f"[GH DEBUG] company={getattr(company,'name',None)} token={token}", flush=True)
        except Exception:
            pass
        if not token:
            return out

        api = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
        try:
            r = session.get(api, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                return out
            data = r.json() or {}
        except Exception:
            return out

        jobs = data.get("jobs") or []
        seen = set()
        for j in jobs:
            title = (j.get("title") or "").strip()
            url = (j.get("absolute_url") or j.get("url") or "").strip()
            if not title or not url or url in seen:
                continue
            seen.add(url)
            cat = categorize_title(title)
            out.append({
                "title": title,
                "apply_url": url,
                "source": self.name,
                "snippet": None,
                "category": cat,
            })
        return out
