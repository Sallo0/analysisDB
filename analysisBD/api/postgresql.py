import psycopg2
import json
import time
from psycopg2.extras import RealDictCursor
from django.core.serializers.json import DjangoJSONEncoder

t = time
timer = 0


def getDataPostgreSQL(request):
    for p in request.data:
        print(p)

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
            cursor.execute("SELECT * FROM links LIMIT 10")
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