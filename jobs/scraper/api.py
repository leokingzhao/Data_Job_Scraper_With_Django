# jobs/scraper/api.py
from __future__ import annotations
import os
from typing import List, Dict, Optional, Iterable
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse

from django.utils import timezone
from jobs.models import Company

from .workday import WorkdayScraper
from .greenhouse import GreenhouseScraper
from .lever import LeverScraper
from .successfactors import SuccessFactorsScraper
from .icims import ICIMSScraper
from .phenom import PhenomScraper
from .oracle import OracleCloudScraper
from .smartrecruiters import SmartRecruitersScraper
from .generic import GenericScraper

from .detectors import detect_ats as _detect_ats_loose

HTTP_POOL = int(os.getenv("HTTP_POOL", "64"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "15"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "0.3"))
VERBOSE = os.getenv("VERBOSE", "0") == "1"

_WHITELIST = [
    # DS 
    "data scientist", "applied scientist", "ml scientist", "machine learning scientist",
    "machine learning analyst",

    # DE
    "data engineer", "machine learning engineer", "ml engineer",

    # DA
    "data analyst", "data analytics", "business intelligence", "bi analyst",

    # intern
    "data scientist intern", "data science intern", "data analyst intern",
]
_WHITELIST_LC = [t.lower() for t in _WHITELIST]


def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=HTTP_BACKOFF,
        status_forcelist=[502, 503, 504, 429],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=HTTP_POOL, pool_maxsize=HTTP_POOL, max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": os.getenv(
            "HTTP_UA",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    })
    return s


def _keep_title(title: str) -> bool:
    t = (title or "").lower()
    return any(kw in t for kw in _WHITELIST_LC)

def _classify(title: str) -> str:
    t = (title or "").lower()

    # 
    if "intern" in t:
        if "analyst" in t:
            return "Data Analyst"   # data analyst intern -> DA
        else:
            return "Data Scientist" # ds/data science/ml intern -> DS

    # ML analyst --> DS
    if "machine learning analyst" in t:
        return "Data Scientist"

    if "data engineer" in t or "machine learning engineer" in t or "ml engineer" in t:
        return "Data Engineer"
    if ("data scientist" in t) or ("applied scientist" in t) or ("ml scientist" in t) or ("machine learning scientist" in t):
        return "Data Scientist"
    if ("data analyst" in t) or ("data analytics" in t) or ("business intelligence" in t) or ("bi analyst" in t):
        return "Data Analyst"


    return "Other"

# ---------- ATS  ----------
def _guess_ats_from_url(url: str) -> Optional[str]:
    u = (url or "").lower()
    host = urlparse(url or "").netloc.lower()

    if not url:
        return None

    # Workday
    if "myworkdayjobs.com" in host or "workday" in host or ".wd" in host or "/wday/" in u:
        return "workday"

    # Greenhouse
    if "boards.greenhouse.io" in host or host.endswith("greenhouse.io"):
        return "greenhouse"

    # Lever
    if host.endswith("lever.co") or host.endswith("jobs.lever.co"):
        return "lever"

    # SuccessFactors (rmk / careersection )
    if host.endswith("successfactors.com") or "careersection" in u or "sfcareer" in u:
        return "successfactors"

    # iCIMS
    if "icims.com" in host:
        return "icims"

    # Phenom（avoid cdn.）
    if "phenom" in host and not host.startswith("cdn."):
        return "phenom"

    # Oracle Cloud HCM
    if "/hcmui/" in u or "/candidateexperience/" in u or host.endswith("oraclecloud.com"):
        return "oracle"

    # SmartRecruiters
    if host.endswith("smartrecruiters.com"):
        return "smartrecruiters"

    return None

#
def _uniq_keep_order(items: Iterable[str]) -> List[str]:
    seen = set(); out = []
    for x in items:
        if not x or x in seen: 
            continue
        seen.add(x); out.append(x)
    return out

from urllib.parse import urlparse

def _safe_handles(scraper, company) -> bool:
    
    url = getattr(company, "data_query_url", None) or (company.careers_url or "")
    try:
        if hasattr(scraper, "handles"):
            try:
                return bool(scraper.handles(company))
            except TypeError:
                return bool(scraper.handles(url))
        return True
    except Exception:
        return False

def _build_candidates(company: Company) -> List[object]:
    entry = getattr(company, "data_query_url", None) or (company.careers_url or "")
    at1 = _guess_ats_from_url(entry)
    at2 = _guess_ats_from_url(company.careers_url or "")
    at3 = None
    try:
        det = (_detect_ats_loose(entry, None) or _detect_ats_loose(company.careers_url or "", None))
        at3 = det[0] if isinstance(det, tuple) else det
    except Exception:
        at3 = None

    order = _uniq_keep_order([
        at1, at2, at3,
        "workday", "greenhouse", "lever", "successfactors", "icims",
        "phenom", "oracle", "smartrecruiters",
        "generic",
    ])

    reg = {
        "workday": WorkdayScraper,
        "greenhouse": GreenhouseScraper,
        "lever": LeverScraper,
        "successfactors": SuccessFactorsScraper,
        "icims": ICIMSScraper,
        "phenom": PhenomScraper,
        "oracle": OracleCloudScraper,
        "smartrecruiters": SmartRecruitersScraper,
        "generic": GenericScraper,
    }

    cand = [reg[k]() for k in order if k in reg]
    cand = [sc for sc in cand if _safe_handles(sc, company)]
    if not cand:
        cand = [GenericScraper()]
    return cand




def fetch_company_jobs(company: Company, session: Optional[requests.Session] = None) -> List[Dict]:
    """
    return:
      title, apply_url, source, snippet, company_name, found_at, category
    """
    s = session or build_session()
    entry_url = getattr(company, "data_query_url", None) or (company.careers_url or "")

    if VERBOSE:
        try:
            print(f"[FETCH] {company.name} entry={entry_url}", flush=True)
        except Exception:
            pass

    out_all: List[Dict] = []
    seen = set()

    for scraper in _build_candidates(company):
        try:
            hits = scraper.fetch(company, session=s) or []
        except TypeError:
            # 
            try:
                hits = scraper.fetch(company, s) or []
            except Exception:
                hits = []
        except Exception:
            hits = []

        if not hits:
            continue

        now = timezone.now()
        for h in hits:
            title = (h.get("title") or "").strip()
            url = (h.get("apply_url") or h.get("url") or "").strip()
            if not title or not url:
                continue

            # filter whitelist
            if not _keep_title(title):
                continue

            if url in seen:
                continue
            seen.add(url)

            cat = _classify(title)
            if cat == "Other":
                continue  # 

            out_all.append({
                "title": title,
                "apply_url": url,
                "source": h.get("source") or scraper.__class__.__name__.replace("Scraper","").lower(),
                "snippet": h.get("snippet") or "",
                "company_name": getattr(company, "name", ""),
                "found_at": h.get("found_at") or now,
                "category": cat,
            })

       

    return out_all
