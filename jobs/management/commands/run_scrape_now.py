# jobs/management/commands/run_scrape_now.py
from concurrent.futures import ThreadPoolExecutor
import traceback, time, random

from django.core.management.base import BaseCommand
from django.db import transaction, OperationalError
from django.utils import timezone

from jobs.models import Company, JobHit
from jobs.scraper.api import fetch_company_jobs, build_session


class Command(BaseCommand):
    help = "Scrape jobs now."
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument("--only-active", action="store_true", default=False)
        parser.add_argument("--parallel", type=int, default=8)
        parser.add_argument("--limit", type=int, default=5000)
        parser.add_argument("--company", type=str, default=None, help="Substring match for company name")

    def handle(self, *args, **opts):
        # -------- 安静输出封装：用 -v 0/1/2/3 控制 --------
        verbosity = int(opts.get("verbosity", 1))

        def _info(msg: str):
            if verbosity >= 1:
                self.stdout.write(self.style.NOTICE(msg))

        def _warn(msg: str):
            if verbosity >= 1:
                self.stderr.write(self.style.WARNING(msg))

        def _done(msg: str):
            # 注意：这里不能再调用 _done 自己，否则递归！
            self.stdout.write(self.style.SUCCESS(msg))
        # --------------------------------------------------

        only_active = bool(opts.get("only_active"))
        parallel = max(1, int(opts["parallel"]))
        limit = int(opts["limit"])
        name_filter = (opts.get("company") or "").strip()

        qs = Company.objects.all()
        if only_active:
            qs = qs.filter(is_active=True)
        if name_filter:
            qs = qs.filter(name__icontains=name_filter)
        qs = qs.exclude(careers_url__isnull=True).exclude(careers_url="")
        companies = list(qs.order_by("name")[:limit])

        _info(f"[INFO] companies to scan: {len(companies)}, parallel={parallel}")

        session = build_session()

        # 线程里只“抓”，不写库（避免并发写锁）
        def work(c: Company):
            try:
                hits = fetch_company_jobs(c, session=session) or []
                return (c.id, hits, None)
            except Exception as e:
                return (c.id, [], f"{c.name}: {e}\n{traceback.format_exc()}")

        if parallel == 1:
            results = [work(c) for c in companies]
        else:
            with ThreadPoolExecutor(max_workers=parallel) as ex:
                results = list(ex.map(work, companies))

        id_to_company = {c.id: c for c in companies}

        def upsert_with_retry(c: Company, h: dict, max_tries=6):
            delay = 0.15
            now = timezone.now()
            for _ in range(max_tries):
                try:
                    with transaction.atomic():
                        obj, created = JobHit.objects.get_or_create(
                            company=c,
                            apply_url=h.get("apply_url"),
                            defaults={
                                "title": h.get("title") or "Data Scientist",
                                "source": h.get("source") or "auto",
                                "raw_snippet": h.get("snippet"),
                                "is_active": True,
                                "category": h.get("category"),
                                "found_at": h.get("found_at") or now,
                                "first_seen_at": now,  # 仅在创建时写入
                            },
                        )
                        if not created:
                            # 更新但不覆盖 first_seen_at
                            obj.title = h.get("title") or obj.title
                            obj.source = h.get("source") or obj.source
                            obj.raw_snippet = h.get("snippet")
                            obj.is_active = True
                            if h.get("category"):
                                obj.category = h["category"]
                            obj.found_at = h.get("found_at") or now
                            obj.save(update_fields=["title", "source", "raw_snippet", "is_active", "category", "found_at"])
                        return True
                except OperationalError:
                    time.sleep(delay + random.uniform(0, delay))  # 指数退避 + 抖动
                    delay = min(delay * 2, 2.0)
                except Exception:
                    return False
            return False

        ok = 0
        fetched = 0
        saved = 0
        for cid, hits, err in results:
            c = id_to_company.get(cid)
            if err:
                _warn(f"[WARN] {err}")
            if c is None:
                continue
            ok += 1
            fetched += len(hits)
            for h in hits:
                if upsert_with_retry(c, h):
                    saved += 1

            # 更新公司元数据
            try:
                c.last_checked_at = timezone.now()
                if hits:
                    ts = max([(h.get("found_at") or timezone.now()) for h in hits])
                    c.last_found_at = ts
                c.save(update_fields=["last_checked_at", "last_found_at"])
            except Exception:
                pass

        _done(f"Done. companies ok={ok}, hits fetched={fetched}, hits saved={saved}")
