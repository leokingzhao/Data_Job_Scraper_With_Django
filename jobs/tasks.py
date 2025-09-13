from __future__ import annotations
from django.utils import timezone
from celery import shared_task
from .models import Company, JobHit
from .scraper import fetch_company_jobs

@shared_task
def run_daily_scrape():
    now = timezone.now()
    companies = Company.objects.filter(is_active=True)
    for c in companies:
        hits = fetch_company_jobs(c) or []
        c.last_checked_at = now
        c.save(update_fields=["last_checked_at"])
        for h in hits:
            obj, created = JobHit.objects.update_or_create(
                company=c,
                apply_url=h.get("apply_url"),
                defaults={
                    "title": h.get("title") or "Data Scientist",
                    "source": h.get("source") or "auto",
                    "raw_snippet": h.get("snippet"),
                    "is_active": True,
                    "found_at": now,                        
                    "category": (h.get("category") or None),
                },
            )
            if created and not obj.first_seen_at:
                obj.first_seen_at = now
                obj.save(update_fields=["first_seen_at"])