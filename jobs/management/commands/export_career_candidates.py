from __future__ import annotations
import csv, sys, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand
from django.db.models import Q

from jobs.models import Company
from jobs.scraper.detectors import detect_ats  # 我们只用这个

CAREER_HINTS = (
    "career", "careers", "jobs", "join-us", "joinus",
    "work-with-us", "workwithus", "opportunities", "employment"
)

ASSET_EXTS = (".css",".js",".ico",".png",".jpg",".jpeg",".gif",".svg",".pdf",".woff",".woff2",".ttf")

def is_asset(u: str) -> bool:
    low = (u or "").lower()
    return any(low.endswith(ext) for ext in ASSET_EXTS)

def make_abs(base: str, href: str) -> str:
    try:
        return href if href.startswith(("http://","https://")) else urljoin(base, href)
    except Exception:
        return href

def normalize_by_ats(url: str, ats: Optional[str]) -> str:
    if not url or not ats:
        return url
    u = urlparse(url)
    origin = f"{u.scheme}://{u.netloc}" if u.netloc else ""
    path = u.path or ""

    # iCIMS：统一到搜索页
    if ats == "ICIMS":
        host = u.netloc.lower()
        host = re.sub(r"^internal\-", "", host)
        host = re.sub(r"^(?:careers\-|jobs\-)", "", host)
        base = f"{u.scheme}://{host}" if host else origin
        return f"{base}/jobs/search?ss=1"

    # SuccessFactors：优先 career/sfcareer 入口；否则回 origin/career
    if ats == "SUCCESSFACTORS":
        if "career" in path or "sfcareer" in path or "jobboard" in path:
            return url
        return f"{origin}/career" if origin else url

    # Phenom：不要 cdn 域，保持品牌 careers 站（此处仅原样返回）
    if ats == "PHENOM":
        return url

    # 其它 ATS 默认返回原链接
    return url

def score_candidate(u: str, ats: Optional[str], http_ok: bool, title: str) -> int:
    s = 0
    low = (u or "").lower()
    if "career" in low: s += 5
    if "jobs" in low: s += 3
    if ats: s += 7
    if http_ok: s += 2
    t = (title or "").lower()
    if any(k in t for k in ("career","careers","jobs")): s += 1
    if "cdn.phenompeople.com" in low: s -= 10
    if "rmkcdn.successfactors.com" in low: s -= 10
    if is_asset(low): s -= 10
    return s

def fetch_head(session: requests.Session, url: str) -> Tuple[int, str, str]:
    """GET（有些站拒绝 HEAD），返回 (status, final_url, title)"""
    try:
        r = session.get(url, timeout=10, allow_redirects=True)
        status = r.status_code
        final_url = r.url
        title = ""
        # 只解析小页面避免太慢
        if r.text:
            soup = BeautifulSoup(r.text[:150000], "html.parser")
            tt = soup.find("title")
            title = (tt.get_text(" ", strip=True) if tt else "")[:200]
        return status, final_url, title
    except Exception:
        return 0, url, ""

def discover_candidates(homepage: str, session: requests.Session, max_n: int) -> List[Dict]:
    out: List[Dict] = []
    if not homepage:
        return out
    try:
        r = session.get(homepage, timeout=12)
        r.raise_for_status()
    except Exception:
        return out

    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    cands = []

    # 1) a 标签候选
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or is_asset(href): 
            continue
        full = make_abs(r.url, href)
        low = full.lower()
        txt = (a.get_text(" ", strip=True) or "").lower()
        if any(h in low for h in CAREER_HINTS) or any(h in txt for h in CAREER_HINTS):
            if full not in seen:
                seen.add(full)
                cands.append(full)

    # 2) canonical / og:url 兜底
    for tag in soup.find_all("link", rel="canonical"):
        href = (tag.get("href") or "").strip()
        if href and any(h in href.lower() for h in CAREER_HINTS):
            full = make_abs(r.url, href)
            if full not in seen:
                seen.add(full); cands.append(full)
    og = soup.find("meta", property="og:url")
    if og:
        href = (og.get("content") or "").strip()
        if href and any(h in href.lower() for h in CAREER_HINTS):
            full = make_abs(r.url, href)
            if full not in seen:
                seen.add(full); cands.append(full)

    # 过滤静态域/CDN
    cands = [u for u in cands if not is_asset(u)]
    cands = [u for u in cands if "rmkcdn.successfactors.com" not in u.lower()]
    cands = [u for u in cands if "cdn.phenompeople.com" not in u.lower()]

    # 对每个候选取状态、标题、ATS、归一化、打分
    scored = []
    for u in cands:
        ats = detect_ats(u)
        norm = normalize_by_ats(u, ats)
        status, final_url, title = fetch_head(session, norm)
        http_ok = (200 <= status < 400)
        sc = score_candidate(norm, ats, http_ok, title)
        scored.append({
            "candidate_url": u,
            "normalized_url": norm,
            "detected_ats": ats or "",
            "http_status": status,
            "final_url": final_url,
            "page_title": title,
            "score": sc,
        })

    # 排序并返回 Top-N
    scored.sort(key=lambda x: (x["score"], -len(x["normalized_url"] or "")), reverse=True)
    return scored[:max_n]

class Command(BaseCommand):
    help = "Export Top-N career link candidates per company into a CSV for manual review."

    def add_arguments(self, parser):
        parser.add_argument("--outfile", default="career_candidates.csv")
        parser.add_argument("--parallel", type=int, default=12)
        parser.add_argument("--max-per-company", type=int, default=3)
        parser.add_argument("--only-auto", action="store_true",
                            help="Only export companies with ATS=AUTO or missing careers_url.")
        parser.add_argument("--include-empty", action="store_true",
                            help="Include companies with no homepage_url (emit no candidates).")

    def handle(self, *args, **opts):
        outfile = opts["outfile"]
        maxn = opts["max_per_company"]
        parallel = opts["parallel"]

        qs = Company.objects.all()
        if opts["only_auto"]:
            qs = qs.filter(Q(ats="AUTO") | Q(careers_url__isnull=True) | Q(careers_url=""))

        if not opts["include_empty"]:
            qs = qs.exclude(Q(homepage_url__isnull=True) | Q(homepage_url=""))

        companies = list(qs.order_by("name"))
        total = len(companies)
        self.stdout.write(f"Exporting candidates for {total} companies...")

        s_template = requests.Session()
        s_template.headers.update({"User-Agent":"Mozilla/5.0", "Accept-Language":"en-US,en;q=0.9"})

        def work(c: Company) -> List[Dict]:
            s = requests.Session()
            s.headers.update(s_template.headers)
            rows = []
            try:
                cands = discover_candidates(c.homepage_url or "", s, maxn)
                if not cands:
                    # 也导出一条空行，方便你知道这家公司需要人工特别看
                    rows.append({
                        "company_id": c.id,
                        "company_name": c.name,
                        "homepage_url": c.homepage_url or "",
                        "existing_careers_url": c.careers_url or "",
                        "existing_ats": c.ats or "",
                        "candidate_url": "",
                        "normalized_url": "",
                        "detected_ats": "",
                        "score": 0,
                        "http_status": 0,
                        "final_url": "",
                        "page_title": "",
                    })
                else:
                    for it in cands:
                        rows.append({
                            "company_id": c.id,
                            "company_name": c.name,
                            "homepage_url": c.homepage_url or "",
                            "existing_careers_url": c.careers_url or "",
                            "existing_ats": c.ats or "",
                            "candidate_url": it["candidate_url"],
                            "normalized_url": it["normalized_url"],
                            "detected_ats": it["detected_ats"],
                            "score": it["score"],
                            "http_status": it["http_status"],
                            "final_url": it["final_url"],
                            "page_title": it["page_title"],
                        })
            except Exception:
                # 出异常也给一条记录，方便人工关注
                rows.append({
                    "company_id": c.id,
                    "company_name": c.name,
                    "homepage_url": c.homepage_url or "",
                    "existing_careers_url": c.careers_url or "",
                    "existing_ats": c.ats or "",
                    "candidate_url": "",
                    "normalized_url": "",
                    "detected_ats": "",
                    "score": -999,
                    "http_status": 0,
                    "final_url": "ERROR",
                    "page_title": "",
                })
            return rows

        header = ["company_id","company_name","homepage_url","existing_careers_url","existing_ats",
                  "candidate_url","normalized_url","detected_ats","score","http_status","final_url","page_title"]

        written = 0
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            with ThreadPoolExecutor(max_workers=parallel) as ex:
                futs = {ex.submit(work, c): c for c in companies}
                for i, fut in enumerate(as_completed(futs), start=1):
                    rows = fut.result()
                    for r in rows:
                        writer.writerow(r)
                        written += 1
                    if i % 20 == 0 or i == total:
                        self.stdout.write(f"[{i}/{total}] written rows: {written}")

        self.stdout.write(self.style.SUCCESS(f"Done. companies={total}, rows={written}, file={outfile}"))
