# jobs/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("latest/", views.latest, name="jobs_latest"),
]
