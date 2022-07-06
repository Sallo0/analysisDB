from django.contrib import admin
from django.urls import path
from . import views
from django.http import HttpResponse

urlpatterns = [
    path('main/', views.hello),
    path('orientdb/', admin.site.urls),
    path('postgresql/', admin.site.urls),
    path('neo4j/', admin.site.urls),
]
