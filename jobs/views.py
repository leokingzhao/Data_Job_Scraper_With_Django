# jobs/views.py
from __future__ import annotations
import os
import re

from datetime import timedelta
from typing import Optional, Tuple

from django.apps import apps
from django.core.paginator import Paginator
from django.db.models import F, Q
from django.http import Http404
from django.shortcuts import redirect, render
from django.utils import timezone

PAGE_SIZES = [50, 100, 200]
DAY_OPTIONS = [1, 3, 7, 14, 30]


ALLOWED_CATEGORIES = ["Data Scientist", "Data Engineer", "Data Analyst", "Other"]

NEW_BADGE_HOURS = int(os.getenv("NEW_BADGE_HOURS", "24"))

FIELD_ALIASES = {
    "title": ["title", "job_title", "posting_title", "name", "role", "position", "req_title"],
    "url": ["apply_url", "url", "job_url", "link", "href", "applyLink", "posting_url", "job_href", "jobLink"],
    "company_name": ["company_name", "employer", "org_name", "employer_name", "brand"],
    "company_fk": ["company", "employer", "organization", "org", "brand_obj"],
    "created": ["created_at", "found_at", "published_at", "posted_at", "first_seen_at",
                "updated_at", "timestamp", "ts", "date_posted", "date", "created"],
    "category": ["category", "dept", "team"],
}

def _field_names(model):
    return {f.name for f in model._meta.fields}

def _find_field(model, candidates) -> Optional[str]:
    names = _field_names(model)
    for c in candidates:
        if c in names:
            return c
    return None

def _pick_job_model() -> Tuple[object, dict]:
    try:
        app = apps.get_app_config("jobs")
    except LookupError:
        raise Http404("jobs app not installed")

    best = None
    best_score = -1
    best_map = {}

    for model in app.get_models():
        title = _find_field(model, FIELD_ALIASES["title"])
        if not title:
            continue
        url = _find_field(model, FIELD_ALIASES["url"])
        company_name = _find_field(model, FIELD_ALIASES["company_name"])
        company_fk = _find_field(model, FIELD_ALIASES["company_fk"])
        created = _find_field(model, FIELD_ALIASES["created"])
        category = _find_field(model, FIELD_ALIASES["category"])

        score = 0
        score += 4
        if url: score += 3
        if company_name: score += 2
        if company_fk: score += 1.5
        if created: score += 2
        if category: score += 0.5

        if score > best_score:
            best_score = score
            best = model
            best_map = {
                "title": title,
                "url": url,
                "company_name": company_name,
                "company_fk": company_fk,
                "created": created,
                "category": category,
            }

    if not best:
        raise Http404("No job model found")
    return best, best_map

# 分类规则（与抓取侧白名单一致）
_rule_ds = re.compile(r"\b(data\s+scientist(s)?|(applied|ml)\s+scientist|machine\s+learning(\s+analyst)?)\b", re.I)
_rule_de = re.compile(r"\bdata\s+engineer(s)?\b", re.I)
_rule_da = re.compile(r"\b(data\s+analyst(s)?|data\s+analytics|data\s+science\s+analyst|data\s+analyst\s+intern(s)?)\b", re.I)

def classify(title: str) -> str:
    t = title or ""
    if _rule_ds.search(t): return "Data Scientist"
    if _rule_de.search(t): return "Data Engineer"
    if _rule_da.search(t): return "Data Analyst"
    return "Other"

NEW_BADGE_HOURS = int(os.getenv("NEW_BADGE_HOURS", "24"))

def latest(request):

    PAGE_SIZES = [50, 100, 200]
    DAY_OPTIONS = [1, 3, 7, 14, 30]

    try:
        per_page = int(request.GET.get("size", 200))
    except ValueError:
        per_page = 200
    try:
        days = int(request.GET.get("days", 1))
    except ValueError:
        days = 1
    category = request.GET.get("category", "All")

    M, m = _pick_job_model()
    title_f = m["title"]
    url_f = m["url"]
    created_f = m["created"] 
    company_fk = m.get("company_fk")     # e.g. 'company'
    company_name_f = m.get("company_name")
    category_f = m.get("category")

  
    qs = M.objects.all()
    if days > 0:
        since = timezone.now() - timedelta(days=days)
        qs = qs.filter(**{f"{created_f}__gte": since})

    if company_fk:
        qs = qs.select_related(company_fk)

    qs = qs.order_by(f"-{created_f}")

    rows = []
    now = timezone.now()
    new_cutoff = now - timedelta(hours=NEW_BADGE_HOURS)

    for obj in qs:
        # title
        title = getattr(obj, title_f, "") or ""

        # url
        url = getattr(obj, url_f, None)
        if not url and hasattr(obj, "url"):
            url = getattr(obj, "url")

        # company 
        company = ""
        if company_fk and hasattr(obj, company_fk):
            comp_obj = getattr(obj, company_fk)
            if comp_obj is not None:
                company = getattr(comp_obj, "name", None) or getattr(comp_obj, "company_name", None) or str(comp_obj)
        if not company and company_name_f:
            company = getattr(obj, company_name_f, None) or ""
        if not company and hasattr(obj, "company_name"):
            company = getattr(obj, "company_name") or ""

        # category
        cat = getattr(obj, category_f, None) if category_f else None
        if not cat and hasattr(obj, "category"):
            cat = getattr(obj, "category")
        if not cat:
            cat = infer_category(title)

        # found / first_seen
        found = getattr(obj, created_f, None)
        first_seen = getattr(obj, "first_seen_at", None)
        is_new = bool(first_seen and first_seen >= new_cutoff)

        rows.append({
            "company": company or "",
            "title": title or "",
            "url": url or "",
            "found": found,
            "cat": cat or "Other",
            "is_new": is_new,  
        })

 
    categories = sorted({r["cat"] for r in rows if r.get("cat")})
    if category and category.lower() != "all":
        rows = [r for r in rows if r.get("cat") == category]


    paginator = Paginator(rows, per_page)
    page = paginator.get_page(request.GET.get("page") or "1")

    ctx = {
        "rows": list(page.object_list),
        "page": page,
        "total": paginator.count,
        "categories": categories,
        "category": category,
        "per_page": per_page,
        "page_sizes": PAGE_SIZES,
        "days": days,
        "day_options": DAY_OPTIONS,
        "today": timezone.localdate(),
        "new_badge_hours": NEW_BADGE_HOURS,
    }
    return render(request, "jobs/latest.html", ctx)

def home(request):
    return redirect("jobs_latest")

