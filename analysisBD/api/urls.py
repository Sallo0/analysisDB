from django.contrib import admin
from django.urls import path
from . import views
from django.http import HttpResponse

urlpatterns = [
    path('', views.hello),
    path('orientdb/', views.orient),
    path('postgresql/', views.postgre),
    path('neo4j/', views.Neo4j),

    path('neo4j/getdata', views.getNeo4jData),
    path('neo4j/createtestdata', views.createNeo4jTestData),

    path('orientdb/getdata', views.getOrientDBData),
    path('postgresql/getdata', views.getPostgreSQLData)
]
