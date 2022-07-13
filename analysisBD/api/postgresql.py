import psycopg2
import json
import time
import os
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, dotenv_values
from django.core.serializers.json import DjangoJSONEncoder

t = time
timer = 0
load_dotenv()


def queryConstructor(data):
    """Создаёт sql запрос в виде строки по полученным фильтрам с сайта

        :param data: arg1
        :type data: object

        :rtype: str
        :return: SQL-строка
    """
    query = ["SELECT * FROM links WHERE "]
    sql_args = []

    if data['mainfilter']['Child'] != "" and data['mainfilter']['Parent'] != "":
        sql_args.append(f'child={data["mainfilter"]["Child"]}')
        sql_args.append(f'parent={data["mainfilter"]["Parent"]}')

    elif data['mainfilter']['Parent'] != "":
        sql_args.append(f'parent={data["mainfilter"]["Parent"]}')

    elif data['mainfilter']['Child'] != "":
        sql_args.append(f'child={data["mainfilter"]["Child"]}')

    else:
        sql_args.append(f'pk=0')

    if data['kind'] != "-":
        sql_args.append(f'kind={data["kind"]}')

    if data['date_begin'] != "":
        sql_args.append(f"date_begin >= '{data['date_begin']}'")

    if data['date_end'] != "":
        sql_args.append(f"date_end <= '{data['date_end']}'")

    if data['cost'] != "":
        sql_args.append(f'cost={data["cost"]}')

    if data['share'] != "":
        sql_args.append(f'share={data["share"]}')

    if data['child_liquidated'] != "-":
        sql_args.append(f'child_liquidated={data["child_liquidated"]}')

    query.append(" AND ".join(sql_args))

    query.append(" LIMIT 25")

    return "".join(query)


def getDataPostgreSQL(request):
    """Получает данные по запросу request из базы данных postgres и возвращает их вместе с временем,
         за которое он эти данные получил

            :param request: arg1
            :type request: object

            :rtype: dict
            :return: словарь с данными из базы postgres
    """

    data = request.data
    to_json = {}
    host = os.getenv('postgres_host')
    user = os.getenv('postgres_user')
    password = os.getenv('postgres_password')
    db_name = os.getenv('postgres_db_name')
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