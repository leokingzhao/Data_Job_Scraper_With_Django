from django import template
from urllib.parse import urlsplit

register = template.Library()

@register.filter
def host(value: str) -> str:
    try:
        return urlsplit(value).netloc
    except Exception:
        return ""
