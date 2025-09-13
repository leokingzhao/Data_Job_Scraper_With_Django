from __future__ import annotations
import csv
from django.core.management.base import BaseCommand, CommandError
from jobs.models import Company

class Command(BaseCommand):
    help = "Import companies from CSV: name,homepage_url,careers_url,ats,is_active (or legacy \"active\")"

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str)

    def handle(self, *args, **opts):
        path = opts["csv_path"]
        created = updated = 0
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            required = {"name", "homepage_url"}
            if not required.issubset(reader.fieldnames or []):
                raise CommandError("CSV must include at least: name, homepage_url")
            for row in reader:
                name = row.get("name", "").strip()
                homepage_url = row.get("homepage_url", "").strip()
                careers_url = (row.get("careers_url") or "").strip() or None
                ats = (row.get("ats") or "AUTO").strip().upper()
                active = str((row.get("is_active") if row.get("is_active") is not None else row.get("active", "1"))).strip().lower() in {"1","true","t","yes","y"}
                obj, is_created = Company.objects.update_or_create(
                    name=name,
                    defaults={
                        "homepage_url": homepage_url,
                        "careers_url": careers_url,
                        "ats": ats if ats else "AUTO",
                        "is_active": active,
                    },
                )
                created += int(is_created)
                updated += int(not is_created)
        self.stdout.write(self.style.SUCCESS(f"Companies imported. created={created}, updated={updated}"))