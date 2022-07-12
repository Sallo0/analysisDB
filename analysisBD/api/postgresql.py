import psycopg2
import json
import time
from psycopg2.extras import RealDictCursor
from django.core.serializers.json import DjangoJSONEncoder

t = time
timer = 0


def queryConstructor(data):
    query = "SELECT * FROM links WHERE "

    if data['mainfilter']['Child'] != "" and data['mainfilter']['Parent'] != "":
        query += "child=" + data['mainfilter']['Child'] + " AND parent=" + data['mainfilter']['Parent']

    elif data['mainfilter']['Parent'] != "":
        query += "parent=" + data['mainfilter']['Parent']

    elif data['mainfilter']['Child'] != "":
        query += "child=" + data['mainfilter']['Child']

    query += " LIMIT 25"
    return query


def getDataPostgreSQL(request):
    print(request.data['dbtype'])
    print(request.data['mainfilter']['Child'])
    data = request.data
    to_json = {}
    host = "127.0.0.1"
    user = "postgres"
    password = "postgres"
    db_name = "postgres"
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
            )
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            time_start = t.perf_counter()
            query = queryConstructor(data)
            cursor.execute(query)
            time_end = t.perf_counter()
            result = cursor.fetchall()
            timer = time_end - time_start
            to_json = json.dumps(
                result,
                cls=DjangoJSONEncoder)
    except Exception as _ex:
        print("Error : ", _ex)
    finally:
        if connection:
            connection.close()
            print("connection closed")

    to_json += str(timer)

    return to_json