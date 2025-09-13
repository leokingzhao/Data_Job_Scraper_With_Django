# jobs/management/commands/import_search_urls.py
import csv
from django.core.management.base import BaseCommand, CommandError
from jobs.models import Company


class Command(BaseCommand):
    help = "Import a 3-column CSV: name, homepage_url, data_query_url. Updates or creates companies."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", help="Path to CSV file (UTF-8).")
        parser.add_argument("--only-update", action="store_true", help="Only update existing companies (do not create).")

    def handle(self, *args, **opts):
        path = opts["csv_path"]
        only_update = opts["only-update"]
        count_upd = count_new = 0

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row_num, row in enumerate(reader, start=1):
                if not row or len(row) < 1:
                    continue
                name = (row[0] or "").strip()
                homepage = (row[1] or "").strip() if len(row) > 1 else ""
                data_query = (row[2] or "").strip() if len(row) > 2 else ""

                if not name:
                    self.stderr.write(self.style.WARNING(f"Row {row_num}: empty name, skipped"))
                    continue

                try:
                    obj = Company.objects.filter(name__iexact=name).first()
                    if obj:
                        # update
                        changed = False
                        if homepage and obj.homepage_url != homepage:
                            obj.homepage_url = homepage
                            changed = True
                        if data_query and obj.data_query_url != data_query:
                            obj.data_query_url = data_query
                            changed = True
                        if changed:
                            obj.save(update_fields=["homepage_url", "data_query_url"])
                            count_upd += 1
                    else:
                        if only_update:
                            continue
                        obj = Company.objects.create(
                            name=name,
                            homepage_url=homepage or None,
                            data_query_url=data_query or None,
                            ats="AUTO",
                            is_active=True,
                        )
                        count_new += 1
                except Exception as e:
                    raise CommandError(f"Row {row_num} ({name}): {e}")

        self.stdout.write(self.style.SUCCESS(f"Done. updated={count_upd}, created={count_new}"))
