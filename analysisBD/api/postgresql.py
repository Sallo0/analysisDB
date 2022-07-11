import psycopg2
import json
from psycopg2.extras import RealDictCursor


def getDataPostgreSQL(request):
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
            cursor.execute("SELECT * FROM links LIMIT 10")
            result = cursor.fetchall()
            to_json = json.dumps(result)
    except Exception as _ex:
        print("Error : ", _ex)
    finally:
        if connection:
            connection.close()
            print("connection closed")
    return to_json