from __future__ import annotations
from typing import List, Dict
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .base import BaseScraper

class TaleoScraper(BaseScraper):
    def fetch(self, company, session) -> List[Dict]:
        out: List[Dict] = []
        resp = session.get(company.careers_url, timeout=8)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True)
            if self._match_title(text):
                out.append({
                    "title": text,
                    "apply_url": urljoin(resp.url, a["href"]),
                    "source": "taleo",
                    "snippet": None,
                })
        return out