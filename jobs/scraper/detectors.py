# jobs/scraper/detectors.py
from __future__ import annotations
from typing import Optional, Tuple
from urllib.parse import urlparse
import re

KNOWN_WORKDAY_HOSTS = (
    "myworkdayjobs.com", "workdayjobs.com",
    ".wd1.", ".wd2.", ".wd3.", ".wd4.", ".wd5.", ".wd.", "wdp."
)

def _host(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return (url or "").lower()

def _path(url: str) -> str:
    try:
        return urlparse(url).path.lower()
    except Exception:
        return ""

def detect_ats(url: str, session=None) -> Optional[Tuple[str, Optional[str]]]:
    """

    """
    u = (url or "").strip()
    if not u:
        return None

    h = _host(u)
    p = _path(u)

    # 1) 
    if ("myworkdayjobs.com" in h) or any(tok in h for tok in ("workday",)) or any(tok in u for tok in KNOWN_WORKDAY_HOSTS) or "/wday/" in p:
        return ("workday", None)

    if "boards.greenhouse.io" in h or "greenhouse.io" in h:
        return ("greenhouse", None)

    if "jobs.lever.co" in h or h.endswith("lever.co"):
        return ("lever", None)

    if "icims.com" in h or "icims" in h:
        return ("icims", None)

    if "smartrecruiters.com" in h:
        return ("smartrecruiters", None)

    # Oracle Cloud HCM 
    if "oraclecloud.com" in h or "/hcmui/candidateexperience/" in p:
        return ("oracle", None)

    # SuccessFactors
    if "successfactors.com" in h or "careersection" in p or "/go/" in p:
        return ("successfactors", None)

    # Phenom
    if "phsearch/api/v1/search" in u:
        return ("phenom", None)


    if session:
        try:
            r = session.get(u, timeout=8)
            t = (r.text or "").lower()

            if "myworkdayjobs" in t or "/wday/cxs/" in t:
                return ("workday", None)
            if "boards.greenhouse.io" in t or "greenhouse" in t:
                return ("greenhouse", None)
            if "jobs.lever.co" in t or "lever-jobs" in t:
                return ("lever", None)
            if "icims.com" in t:
                return ("icims", None)
            if "smartrecruiters" in t:
                return ("smartrecruiters", None)
            if "hcmui/candidateexperience" in t or "oraclecloud.com" in t:
                return ("oracle", None)
            if "careersection" in t or "successfactors" in t or "rmk" in t:
                return ("successfactors", None)
            if "phenompeople" in t or "/phsearch/api/v1/search" in t or "window.phenom" in t:
                return ("phenom", None)
        except Exception:
            pass

    return None
