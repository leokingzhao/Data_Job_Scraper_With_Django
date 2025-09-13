from __future__ import annotations
from typing import Tuple, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import re, requests

CAREER_HINTS = (
    "career", "careers", "jobs", "join-us", "joinus",
    "work-with-us", "workwithus", "opportunities", "employment"
)

def _abs(base: str, href: str) -> str:
    try:
        return href if href.startswith(("http://","https://")) else urljoin(base, href)
    except Exception:
        return href

def _is_asset(u: str) -> bool:
    return any(u.lower().endswith(ext) for ext in (".css",".js",".ico",".png",".jpg",".jpeg",".gif",".svg",".pdf"))

def _detect_ats(url: str) -> Optional[str]:
    host = urlparse(url).netloc.lower()
    path = urlparse(url).path.lower()
    if "myworkdayjobs.com" in host: return "WORKDAY"
    if host.endswith("successfactors.com") and ("career" in path or "sfcareer" in path): return "SUCCESSFACTORS"
    if host.endswith("icims.com"): return "ICIMS"
    if "smartrecruiters.com" in host: return "SMARTRECRUITERS"
    if host.endswith("lever.co") or host.endswith("jobs.lever.co"): return "LEVER"
    if "phenom" in host and "cdn." not in host: return "PHENOM"   # 只认品牌站，不认 cdn
    return None

def _normalize_by_ats(url: str, ats: Optional[str]) -> str:
    u = urlparse(url)
    origin = f"{u.scheme}://{u.netloc}"
    path = u.path
    q = u.query

    if ats == "ICIMS":
        # 统一指向搜索页
        host = u.netloc
        host = re.sub(r"^internal\-", "", host)
        host = re.sub(r"^(?:fdc|fdcnm|careers\-)", "", host)  # 常见前缀去噪
        base = f"{u.scheme}://{host}"
        return f"{base}/jobs/search?ss=1"  # 搜索首页
    if ats == "SUCCESSFACTORS":
        if "careersection" in path or "sfcareer" in path or "career?" in url:
            return url
        # 常见入口
        return f"{origin}/career"
    if ats == "PHENOM":
        # 不能用 cdn，保持品牌 careers 站
        return url
    return url

def discover_from_homepage(homepage_url: str, s: requests.Session) -> Tuple[Optional[str], Optional[str]]:
    try:
        r = s.get(homepage_url, timeout=12)
        r.raise_for_status()
    except Exception:
        return None, None

    soup = BeautifulSoup(r.text, "html.parser")
    cand = []

    # 1) 明确的 <a> 链接
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        txt = (a.get_text(" ", strip=True) or "").lower()
        if not href or _is_asset(href): 
            continue
        full = _abs(r.url, href)
        low = full.lower()
        # 只收 careers/jobs 语义链接，避免杂音
        if any(h in low for h in CAREER_HINTS) or any(h in txt for h in CAREER_HINTS):
            cand.append(full)

    # 2) canonical / og:url 兜底
    for sel in [('link', {'rel':'canonical'}), ('meta', {'property':'og:url'})]:
        tag = soup.find(*sel)
        href = (tag.get("href") or tag.get("content") or "").strip() if tag else ""
        if href and any(h in href.lower() for h in CAREER_HINTS):
            cand.append(_abs(r.url, href))

    # 过滤掉静态资源、rmkcdn/cdn
    cand = [u for u in cand if not _is_asset(u)]
    cand = [u for u in cand if "rmkcdn.successfactors.com" not in u.lower()]
    cand = [u for u in cand if "cdn.phenompeople.com" not in u.lower()]

    # 评分：越像 careers 的优先
    def score(u: str) -> int:
        s = 0; low = u.lower()
        if "career" in low: s += 5
        if "jobs" in low: s += 3
        if "work" in low: s += 1
        if any(host in low for host in ("myworkdayjobs.com","successfactors.com","icims.com","smartrecruiters.com","lever.co")): s += 7
        if "cdn." in low: s -= 10
        if "rmkcdn." in low: s -= 10
        return s

    cand.sort(key=score, reverse=True)
    for u in cand:
        ats = _detect_ats(u)
        if ats:
            return _normalize_by_ats(u, ats), ats

    # 没识别到 ATS，也返回最优的 careers 链接
    if cand:
        return cand[0], "AUTO"
    return None, None
