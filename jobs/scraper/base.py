# jobs/scraper/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import os, re

# ---- quiet logger: VERBOSE=1 时才打印 ----
VERBOSE = os.getenv("VERBOSE", "0") == "1"
def vlog(*a, **kw):
    if VERBOSE:
        try:
            print(*a, **kw, flush=True)
        except Exception:
            pass
# ---------------------------------------

# ---- 白名单：只有命中这些才允许入库 ----
# 你的明确需求：
# data scientist, data engineer, data analyst, data analytics, data science analyst,
# data scientist intern, data science intern, data analyst intern,
# machine learning analyst, machine learning
_ALLOW_PATTERNS = [
    r"\bdata\s+scientist(s)?\b",
    r"\b(applied\s+)?(machine\s+learning|ml)\s+scientist(s)?\b",
    r"\bdata\s+engineer(s)?\b",
    r"\bdata\s+analyst(s)?\b",
    r"\bdata\s+analytics\b",
    r"\bdata\s+science\s+analyst(s)?\b",
    r"\b(machine\s+learning|ml)\s+analyst(s)?\b",
    r"\bdata\s+scientist(\s*[-– ]\s*intern|s?\s+intern(ship)?)\b",
    r"\bdata\s+science(\s*[-– ]\s*intern|s?\s+intern(ship)?)\b",
    r"\bdata\s+analyst(\s*[-– ]\s*intern|s?\s+intern(ship)?)\b",
    r"\bmachine\s+learning\b",   # 单独 ML 关键词（用于 DS 归类）
]
_ALLOW_RE = re.compile("(?:%s)" % "|".join(_ALLOW_PATTERNS), re.I)

# 分类规则（含实习归类）
def classify_strict(title: str) -> Optional[str]:
    t = (title or "").lower()
    if not _ALLOW_RE.search(t):
        return None
    # 实习：先分流，避免被下面普通规则覆盖
    if re.search(r"\bdata\s+analyst.*intern|intern.*data\s+analyst", t):
        return "Data Analyst"
    if re.search(r"(data\s+scientist|data\s+science|machine\s+learning).*intern", t):
        return "Data Scientist"

    if re.search(r"\bdata\s+engineer", t):
        return "Data Engineer"
    if re.search(r"\bdata\s+analyst|data\s+analytics|data\s+science\s+analyst", t):
        return "Data Analyst"
    if re.search(r"\bdata\s+scientist|(\b(applied\s+)?(machine\s+learning|ml)\s+scientist\b)|\b(machine\s+learning|ml)\s+analyst\b|\bmachine\s+learning\b", t):
        return "Data Scientist"
    # 正常不会走到这
    return "Other"

# 兼容：老地方还可能 import 这个函数
def categorize_title(title: str) -> str:
    return classify_strict(title) or "Other"

class BaseScraper(ABC):
    name: str = "base"

    def handles(self, url: str) -> bool:
        return False

    @abstractmethod
    def fetch(self, company, session=None) -> List[Dict]:
        ...

    def make_hit(
        self,
        *,
        title: str,
        url: str,
        company=None,
        snippet: Optional[str] = None,
        source: Optional[str] = None,
    ) -> Dict:
        return {
            "title": (title or "").strip(),
            "apply_url": url,
            "company": company,
            "source": source or self.name,
            "snippet": snippet or "",
            "category": categorize_title(title or ""),
        }
