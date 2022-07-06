from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view
from . import neo4j

def hello(request):
    return render(request, 'index.html')


def orient(request):
    return render(request, 'orientdb.html')


def postgre(request):
    return render(request, 'postgresql.html')


def Neo4j(request):
    return render(request, 'neo4j.html')


@api_view(['GET'])
def getNeo4jData(request):
    return Response(data=neo4j.getDataNeo4j(request), status=200)