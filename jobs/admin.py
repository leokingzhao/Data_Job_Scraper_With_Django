# jobs/admin.py
from django.contrib import admin
from .models import Company, JobHit

@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    # Use 'is_active' instead of 'active'
    list_display = ("name", "ats", "is_active", "homepage_url", "careers_url")
    list_filter = ("ats", "is_active")
    search_fields = ("name", "homepage_url", "careers_url")
    ordering = ("name",)

@admin.register(JobHit)
class JobHitAdmin(admin.ModelAdmin):
    # Keep this conservative to avoid referencing optional fields
    list_display = ("company", "title", "source", "found_at")
    list_filter = ("source", "found_at")
    search_fields = ("company__name", "title", "apply_url")
    ordering = ("-found_at",)

