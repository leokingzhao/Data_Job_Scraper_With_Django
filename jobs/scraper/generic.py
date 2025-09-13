# jobs/scraper/generic.py
from __future__ import annotations
from typing import List, Dict
import os, re, json
from html import unescape
from urllib.parse import urljoin
from django.utils import timezone
from .base import BaseScraper, categorize_title
from bs4 import BeautifulSoup


KEY_HINTS = [
    "data scientist", "machine learning scientist", "ml scientist",
    "applied scientist",
    "data engineer", "machine learning engineer", "ml engineer",
    "data analyst", "analytics analyst", "bi analyst",
    "data science", "analytics", "machine learning",
]


def _terms_from_env() -> List[str]:
    raw = os.getenv("SEARCH_TERMS", os.getenv("WD_TERMS", "")) or ""
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


JOB_URL_HINT = re.compile(r"(job|jobs|opening|opportunit|requisition|position|careers?/.*job)", re.I)
PAGINATION_URL_HINT = re.compile(r"(?:/jobs/(?:page|p)/\d+/?$|[?&]page=\d+)", re.I)
NAV_TEXT_RE = re.compile(r"^(?:go to )?(?:next|previous|prev|first|last)\s+page$", re.I)


TEXT_STOPWORDS = tuple(s.lower() for s in [
    "privacy", "cookie", "your privacy choices", "legal", "terms",
    "accessibility", "feedback", "help", "support", "contact",
    "benefits", "talent community", "join our talent", "log in",
    "login", "sign in", "create account", "subscribe", "newsletter",
    "supplier", "investor", "press", "newsroom", "gift card",
    "store locator", "faq", "returns", "shipping", "about us",
])

WS = re.compile(r"\s+")
ADDR_BLOCK = re.compile(r"(address|location)\s*:?.*$", re.I)

def _clean_text(t: str) -> str:
    t = unescape(t or "").strip()
    t = re.sub(r"<.*?>", " ", t)  
    t = ADDR_BLOCK.sub("", t)  
    t = WS.sub(" ", t)         
    return t.strip(" -–·|")

class GenericScraper(BaseScraper):
    """
    """
    name = "generic-html"

    def handles(self, url: str) -> bool:
        return True 
    def fetch(self, company, session) -> List[Dict]:
        out: List[Dict] = []

        
        base_url = getattr(company, "data_query_url", None) \
        or getattr(company, "careers_url", None) \
        or getattr(company, "homepage_url", None)

        if not base_url:
            return out

        try:
            
            r = session.get(
                base_url,
                timeout=int(os.getenv("HTTP_TIMEOUT", "12")),
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Cache-Control": "no-cache",
                },
            )
            r.raise_for_status()
            html = r.text or ""
            final_url = r.url
        except Exception:
            return out

       
        terms = set([k.lower() for k in KEY_HINTS] + _terms_from_env())
        max_hits = int(os.getenv("GENERIC_MAX_HITS", "500"))
        now = timezone.now()

     
        for m in re.finditer(r'<a\s[^>]*href=["\'](.*?)["\'][^>]*>(.*?)</a>', html, re.I | re.S):
            href, text = m.group(1), m.group(2)
            anchor = _clean_text(text)
            if not anchor or len(anchor) < 6:
                continue
            low_anchor = anchor.lower()
            low_href = (href or "").lower()
            
  
            if PAGINATION_URL_HINT.search(low_href) or NAV_TEXT_RE.match(anchor):
                continue
            if any(sw in low_anchor for sw in TEXT_STOPWORDS):
                continue

            looks_like_job = bool(JOB_URL_HINT.search(low_href))
            has_term = any(k in low_anchor for k in terms) or any(k in low_href for k in terms)
            if not (looks_like_job or has_term):
                continue

            apply_url = urljoin(final_url, (href or "").strip())
            if not apply_url.startswith(("http://", "https://")):
                continue

            out.append({
                "title": anchor,
                "apply_url": apply_url,
                "source": self.name,
                "snippet": None,
                "category": categorize_title(anchor),
                "found_at": now, 
            })
            if len(out) >= max_hits:
                break


        if not out:
            soup = None
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                soup = None

            json_blobs = []

            # <script type="application/json">...</script> and <script type="application/ld+json">...</script>
            if soup is not None:
                for s in soup.find_all("script", {"type": re.compile(r"^application/(ld\+)?json$", re.I)}):
                    txt = (s.string or s.text or "").strip()
                    if not txt:
                        continue
                    try:
                        data = json.loads(txt)
                   
                        json_blobs.append(data)
                    except Exception:
                
                        last = txt.rfind("}")
                        if last > 0:
                            try:
                                json_blobs.append(json.loads(txt[:last+1]))
                            except Exception:
                                pass

          
                text_all = html
                for pat in (
                    r'__NEXT_DATA__\s*=\s*(\{.*?\})',
                    r'__NUXT__\s*=\s*(\{.*?\})',
                    r'__INITIAL_STATE__\s*=\s*(\{.*?\})',
                    r'__APOLLO_STATE__\s*=\s*(\{.*?\})',
                ):
                    for m in re.finditer(pat, text_all, re.I | re.S):
                        blob = m.group(1)
                        if not blob:
                            continue
                        try:
                            json_blobs.append(json.loads(blob))
                        except Exception:
                            last = blob.rfind("}")
                            if last > 0:
                                try:
                                    json_blobs.append(json.loads(blob[:last+1]))
                                except Exception:
                                    pass

            def add_hit(title: str, url: str, source="generic-json"):
                if not (title and url):
                    return
                apply_url = urljoin(final_url, url.strip())
                if not apply_url.startswith(("http://", "https://")):
                    return
                lt = (title or "").lower()
                lu = (url or "").lower()
                looks_like_job = bool(JOB_URL_HINT.search(lu))
                has_term = (not terms) or any(k in lt for k in terms) or any(k in lu for k in terms)
                if not (looks_like_job or has_term):
                    return
                out.append({
                    "title": _clean_text(title),
                    "apply_url": apply_url,
                    "source": source,
                    "snippet": None,
                    "category": categorize_title(title or ""),
                    "found_at": now,
                })

            def walk(node):
                if isinstance(node, dict):
                    # 1) schema.org/JobPosting（ld+json）
                    typ = node.get("@type") or node.get("type")
                    if isinstance(typ, str) and "jobposting" in typ.lower():
                        t = node.get("title") or node.get("name")
                        u = node.get("url") or node.get("sameAs") or node.get("applicationUrl")
                        if t and u:
                            add_hit(t, u, source="generic-ldjson")


                    titles = [node.get(k) for k in ("title","jobTitle","name","positionTitle","postingTitle")]
                    urls   = [node.get(k) for k in ("absolute_url","url","applyUrl","jobUrl","canonicalPath","href","link","path")]
                    title = next((t for t in titles if isinstance(t, str) and t.strip()), None)
                    url   = next((u for u in urls   if isinstance(u, str) and u.strip()), None)
                    if title and url:
                        add_hit(title, url)

                    for v in node.values():
                        walk(v)
                elif isinstance(node, list):
                    for it in node:
                        walk(it)

            for blob in json_blobs:
                try:
                    walk(blob)
                except Exception:
                    continue
                if len(out) >= max_hits:
                    break


        seen = set(); dedup = []
        for h in out:
            if h["apply_url"] in seen:
                continue
            seen.add(h["apply_url"]); dedup.append(h)
            if len(dedup) >= max_hits:
                break

        return dedup
