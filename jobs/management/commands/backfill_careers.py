from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.management.base import BaseCommand
from django.db import transaction
from jobs.models import Company
from jobs.scraper.discovery import discover_from_homepage
from jobs.scraper.detectors import detect_ats
import requests

class Command(BaseCommand):
    help = "Backfill careers_url and ATS from homepage for companies with AUTO/empty careers_url."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0)
        parser.add_argument("--parallel", type=int, default=12)

    def handle(self, *args, **opts):
        qs = Company.objects.all()
        qs = qs.filter(ats__in=["", "AUTO"]) | qs.filter(careers_url__isnull=True) | qs.filter(careers_url__exact="")
        qs = qs.distinct().order_by("id")
        if opts["limit"]:
            qs = qs[:opts["limit"]]
        total = qs.count()
        self.stdout.write(f"Backfilling {total} companies...")

        s = requests.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.9"})

        def work(c: Company):
            cu, ats = (None, None)
            if c.homepage_url:
                try:
                    cu, ats = discover_from_homepage(c.homepage_url, s)
                except Exception:
                    pass
            if not ats and cu:
                ats = detect_ats(cu) or "AUTO"
            changed = False
            with transaction.atomic():
                if cu and (c.careers_url or "") != cu:
                    c.careers_url = cu; changed = True
                if ats and (c.ats or "AUTO") != ats:
                    c.ats = ats; changed = True
                if changed:
                    c.save(update_fields=["careers_url","ats"])
            return c.name, cu, ats, changed

        ok = 0
        with ThreadPoolExecutor(max_workers=opts["parallel"]) as ex:
            futs = {ex.submit(work, c): c for c in qs}
            for i, fut in enumerate(as_completed(futs), start=1):
                try:
                    name, cu, ats, changed = fut.result()
                    ok += 1
                    status = "updated" if changed else "no-change"
                    self.stdout.write(f"[{i}/{total}] {name}: {status} -> {ats or '-'} {cu or '-'}")
                except Exception as e:
                    self.stderr.write(f"[{i}/{total}] {futs[fut].name}: ERROR {e}")
        self.stdout.write(self.style.SUCCESS(f"Done. processed={ok}"))
