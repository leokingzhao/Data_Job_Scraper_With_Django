# jobs/models.py
from django.db import models
from django.utils import timezone


class Company(models.Model):
    """
     We rely primarily on `data_query_url` which is
    the URL of the *search result page after you typed "data"* (or "data scientist")
    and pressed Enter. This greatly simplifies scraping across ATS/HTML differences.
    """
    name = models.CharField(max_length=200, unique=True)
    homepage_url = models.URLField(blank=True, null=True)
    careers_url = models.URLField(blank=True, null=True)
    # ATS hint: "AUTO" means decide from URL; otherwise fixed value like "WORKDAY"
    ats = models.CharField(max_length=40, default="AUTO")
    is_active = models.BooleanField(default=True, db_index=True)
    ats_type = models.CharField(max_length=32, blank=True, null=True, db_index=True)
    ats_key = models.CharField(max_length=128, blank=True, null=True)

    last_checked_at = models.DateTimeField(blank=True, null=True)
    last_found_at = models.DateTimeField(blank=True, null=True)

    # Your validated search-result URL (after typing "data" and hitting Enter).
    data_query_url = models.URLField(blank=True, null=True)

    def __str__(self) -> str:
        return self.name


CATEGORY_CHOICES = (
    ("DS", "Data Scientist"),
    ("DA", "Data Analyst"),
    ("DE", "Data Engineer"),
    ("INTERN", "Data Science Intern"),
)


class JobHit(models.Model):
    """
    A single job hit we found for a company. We dedupe by (company, apply_url).
    We also track `first_seen_at` to allow "newly found first" ordering in UI.
    """
    company = models.ForeignKey(Company, on_delete=models.CASCADE, related_name="hits")
    title = models.CharField(max_length=500)
    apply_url = models.URLField(max_length=1000)
    source = models.CharField(max_length=100, blank=True, null=True)
    raw_snippet = models.TextField(blank=True, null=True)  # optional preview
    is_active = models.BooleanField(default=True)

    # classification and timeline
    category = models.CharField(max_length=10, choices=CATEGORY_CHOICES, blank=True, null=True)
    first_seen_at = models.DateTimeField(blank=True, null=True)

    # each scrape run updates found_at for "today" tally
    found_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        unique_together = (("company", "apply_url"),)
        indexes = [
            models.Index(fields=["found_at"]),
            models.Index(fields=["category"]),
        ]

    def __str__(self) -> str:
        return f"{self.company.name} | {self.title}"
