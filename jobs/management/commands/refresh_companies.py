from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.management.base import BaseCommand
from django.db import transaction
from jobs.models import Company
from jobs.scraper.detectors import detect_careers_url, detect_ats, find_ats_link_in_page

def normalize_workday_url(url: str) -> str:
  
    if "myworkdayjobs.com" in url:
        return url
    return url 

class Command(BaseCommand):
    
    requires_system_checks = []
    help = "Probe & normalize companies' careers_url and ATS (homepage -> careers -> ATS)."

    # keep Django default options like --skip-checks
    super().add_arguments(parser)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--parallel", type=int, default=12)
    parser.add_argument("--only-active", action="store_true", default=True)
    parser.add_argument("--reset-auto", action="store_true", default=False,
    help="Also re-probe rows with ATS already set (not only AUTO)")

    def handle(self, *args, **opts):
        from jobs.scraper.api import build_session
        qs = Company.objects.all()
        if opts["only_active"]:
            qs = qs.filter(is_active=True)
        if not opts["reset_auto"]:
            qs = qs.filter(ats__in=["AUTO", "MANUAL"]) | qs.filter(careers_url__isnull=True) | qs.filter(careers_url__exact="")
        if opts["limit"]:
            qs = qs[: opts["limit"]]
        companies = list(qs)
        total = len(companies)
        if total == 0:
            self.stdout.write(self.style.WARNING("No companies to refresh.")); return

        self.stdout.write(self.style.HTTP_INFO(f"Refreshing {total} companies with {opts['parallel']} threads..."))

        def probe(c: Company):
            sess = build_session()
            updated = {}
            try:
        
                if not c.careers_url and c.homepage_url:
                    try:
                        r = sess.get(c.homepage_url, timeout=8)
                        r.raise_for_status()
                        cu = detect_careers_url(r.text, c.homepage_url)
                        if cu:
                            updated["careers_url"] = cu
                    except Exception:
                        pass

             
                careers = updated.get("careers_url") or c.careers_url

                #
                if careers:
                    current_ats = detect_ats(careers)
                    if (c.ats in ("AUTO", "MANUAL") or not current_ats):
                        try:
                            r = sess.get(careers, timeout=8)
                            r.raise_for_status()
                            ats_link, ats_name = find_ats_link_in_page(r.text, r.url)
                            if ats_link and ats_name:
                                updated["careers_url"] = ats_link
                                updated["ats"] = ats_name
                        except Exception:
                            pass
                    else:
                        updated.setdefault("ats", current_ats)

                
                cu = updated.get("careers_url") or c.careers_url
                if (updated.get("ats") or c.ats) == "WORKDAY" and cu and "myworkdayjobs.com" not in cu:
                    
                    try:
                        r = sess.get(cu, timeout=8); r.raise_for_status()
                        ats_link, ats_name = find_ats_link_in_page(r.text, r.url)
                        if ats_link and "myworkdayjobs.com" in ats_link:
                            updated["careers_url"] = ats_link
                            updated["ats"] = "WORKDAY"
                    except Exception:
                        pass

                if updated:
                    with transaction.atomic():
                        for k, v in updated.items():
                            setattr(c, k, v)
                        c.save(update_fields=list(updated.keys()))
                return (c.name, True, updated)
            except Exception as e:
                return (c.name, False, {"error": repr(e)})

        ok = fail = 0
        with ThreadPoolExecutor(max_workers=opts["parallel"]) as ex:
            futs = {ex.submit(probe, c): c for c in companies}
            for i, fut in enumerate(as_completed(futs), start=1):
                name, done, info = fut.result()
                if done:
                    ok += 1
                    changed = ", ".join(f"{k}={v}" for k,v in info.items()) or "no-change"
                    self.stdout.write(f"[{i}/{total}] {name}: {changed}")
                else:
                    fail += 1
                    self.stderr.write(self.style.ERROR(f"[{i}/{total}] {name}: ERROR {info.get('error')}"))

        self.stdout.write(self.style.SUCCESS(f"Done. ok={ok}, failed={fail}"))
