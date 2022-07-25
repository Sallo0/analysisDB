import pyorient as po
import json
import time
import os
from psycopg2.extras import RealDictCursor
from django.core.serializers.json import DjangoJSONEncoder


def queryConstructor(data):
    """Создаёт sql запрос в виде строки по полученным фильтрам с сайта

    Args:
        data (dict): Json словарь полученый с фронта.
    Returns:
        str: Готовый SQL запрос.
    """
    query = ["SELECT in.id,out.id,kind,date_begin,date_end,cost,share,child_liquidated FROM Link WHERE "]
    sql_args = []

    if data['mainfilter']['Child'] != "" and data['mainfilter']['Parent'] != "":
        sql_args.append(f'in={data["mainfilter"]["Child"]}')
        sql_args.append(f'out={data["mainfilter"]["Parent"]}')

    elif data['mainfilter']['Parent'] != "":
        sql_args.append(f'out={data["mainfilter"]["Parent"]}')

    elif data['mainfilter']['Child'] != "":
        sql_args.append(f'in={data["mainfilter"]["Child"]}')

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

    print("".join(query))
    return "".join(query)

data = {'dbtype': '', 'mainfilter': {'Child': "#16:513", 'Parent': ''}, 'kind': '-', 'date_begin': '', 'date_end': '', 'cost': '', 'share': '', 'child_liquidated': '-'}


def getDataOrientDB(request):
    """Получает данные по запросу request из базы данных postgres и возвращает их вместе с временем,
         за которое он эти данные получил
    Args:
        request (object): Объект, в котором хранится json словарь с данными из запроса.
    Keyword Args:
        data (dict): Словарь с данными из запроса.
        to_json (dict): Хранит результат обращения к базе данных в формате json.

    Returns:
        json: Возвращает данные из базы PostgreSQL по запросу.
    """

    data = request.data
    print(data)
    to_json = {}
    username = "root"
    password = "tensor"
    client = po.OrientDB("localhost", 2424)
    session_id = client.connect(username, password)
    print("SessionID=", session_id)
    db_name = "analysis"
    client.db_open(db_name, username, password)
    result = client.query(queryConstructor(data))
    #result = client.query(f'SELECT id, name, face_type FROM Face LIMIT 20')
    records = list(map(lambda x: x.oRecordData, result))
    print(records)
    return records

