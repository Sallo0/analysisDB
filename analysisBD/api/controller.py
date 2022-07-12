from django.core.serializers.json import DjangoJSONEncoder
from . import neo4j, orientdb, postgresql


def getData(request):
    if request.data['dbtype'] == "PostgreSQL":
        return postgresql.getDataPostgreSQL(request)
    elif request.data['dbtype'] == "OrientDB":
        return orientdb.getDataOrientDB(request)
    elif request.data['dbtype'] == "Neo4j":
        return neo4j.getDataNeo4j(request)
