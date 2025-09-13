# config/celery.py
import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")  # adjust if your settings module path differs

app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# Run scrape every day at 20:00 (America/Chicago handled by your server TZ or CELERY_TIMEZONE)
app.conf.beat_schedule = {
    "scrape-retail-ds-daily-20": {
        "task": "jobs.tasks.run_daily_scrape",
        "schedule": crontab(hour=20, minute=0),
        "args": (),  # you can pass (limit, parallel) if desired
    }
}
