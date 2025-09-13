# jobs/templatetags/urlutils.py
from urllib.parse import urlsplit
from django import template

register = template.Library()


@register.filter
def host(url: str) -> str:
    """
    Extract netloc/domain from a URL. Safe for templates.
    Usage: {{ job.apply_url|host }}
    """
    try:
        return urlsplit(url).netloc
    except Exception:
        return ""
