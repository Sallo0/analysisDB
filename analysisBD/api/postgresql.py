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
    """Создаёт sql запрос в виде строки по полученным с клиента фильтрам
         
    Args:
        data (dict): JSON-словарь полученный с клиента.

    Keyword Args:
        query (list): Лист, который хранит SQL-запрос по частям

    Returns:
        str: Готовый SQL запрос.

    """
    query = ["SELECT * FROM links WHERE "]
    sql_args = []
    type = ''

    if data['mainfilter']['Child'] != "" and data['mainfilter']['Parent'] != "":
        sql_args.append(f'child={data["mainfilter"]["Child"]}')
        sql_args.append(f'parent={data["mainfilter"]["Parent"]}')

    elif data['mainfilter']['Parent'] != "":
        type = 'child'
        sql_args.append(f'parent={data["mainfilter"]["Parent"]}')

    elif data['mainfilter']['Child'] != "":
        type = 'parent'
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
    query.append(f" ORDER BY date_end DESC, kind, {type}")

    return "".join(query)


def getDataPostgreSQL(request):
    """Получает данные (плоский список) по запросу (request) из базы данных postgres и возвращает их вместе с временем,
         за которое он эти данные получил. Данные сортируются по date_end (сначала записи, у которых отсутствует
         date_end), затем по типу связи kind, а затем по id (face_id).

    Args:
        request (object): Объект, в котором хранится json словарь с данными из запроса.

    Keyword Args:
        data (dict): Словарь с данными из запроса.
        result (RealDictCursor): Хранит результат обращения к базе данных.
        timer (int): Время, за которое выполняется запрос.
        
    Returns:
        all_data: Возвращает данные из PostgreSQL в виде JSON-строки, где в конце добавлено время выполнения запроса.

    """
    data = request.data
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
            all_data = {"result": to_json, "nodes": [], "nodes_type": ''}
            temp = to_json.split("}")
            for i in range(len(temp) - 1):
                i1, i2 = 0, 0
                if data['mainfilter']['Child'] != "" and data['mainfilter']['Parent'] != "":
                    break
                elif data['mainfilter']['Child'] != "":
                    i1 = temp[i].find("parent") + 9
                    i2 = temp[i].find("child") - 3
                    all_data["nodes_type"] = "parent"
                elif data['mainfilter']['Parent'] != "":
                    i1 = temp[i].find("child") + 8
                    i2 = temp[i].find("kind") - 3
                    all_data["nodes_type"] = "child"
                id = temp[i][i1:i2]
                cursor.execute(f'SELECT * FROM face_info WHERE face_id={int(id)} LIMIT 1')
                node = cursor.fetchall()
                json_node = json.dumps(node)
                all_data["nodes"].append(json_node)

    except Exception as _ex:
        print("Error : ", _ex)
    finally:
        if connection:
            connection.close()
            print("connection closed")

    all_data['result'] += str(timer)

    return all_data


def colenoSQL(request):
    """Получает данные (поиск через колено) по запросу (request) из базы данных postgres и возвращает их вместе с
    временем, за которое он эти данные получил. Данные сортируются по date_end (сначала записи, у которых отсутствует
             date_end между искомой вершиной и её parent), а затем по id.

    Args:
        request (object): Объект, в котором хранится json словарь с данными из запроса.

    Keyword Args:
        query (str): Строка с SQL-запросом для получения данных
        result (Dict): Хранит результат обращения к базе данных.
        timer (int): Время, за которое выполняется запрос.

    Returns:
        {result, timer}: Возвращает данные из PostgreSQL в виде JSON-объекта. Result хранит лист словарей, где каждый
        словарь содержит вершину child (брат искомой вершины) и всех parent этой вершины. Time хранит время, за
        которое были получены данные.

    """
    connection = psycopg2.connect(
                host=os.getenv('postgres_host'),
                user=os.getenv('postgres_user'),
                password=os.getenv('postgres_password'),
                database=os.getenv('postgres_db_name'),
                )
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        query = f"""
                WITH
                query1 AS
                (
                Select a.*, b.* from links a, face_info b Where child = {request.data['mainfilter']['Child']} and b.face_id = a.parent 
                )
                SELECT query1.face_id as parent_id,query1.face_type as parent_type,query1.face_name as parent_name, a.* 
                FROM query1, links b,face_info a where query1.parent = b.parent and b.child != {request.data['mainfilter']['Child']} and a.face_id = b.child 
                ORDER BY query1.date_end asc nulls first, b.child LIMIT 25 OFFSET {(request.data['page'] - 1) * 25}
                """
        time_start = t.perf_counter()
        cursor.execute(query)
        time_end = t.perf_counter()
        timer = time_end - time_start
        res = cursor.fetchall()
        result = {}
        for i in res:
            if i["face_id"] in result.keys():
                if {"pk":i["parent_id"], "face_type": i["parent_type"], "face_name":i["parent_name"]} not in result[i["face_id"]]:
                    result[i["face_id"]].append({"pk": i["parent_id"], "face_type":i["parent_type"], "face_name":i["parent_name"]})
            else:
                result[i["face_id"]] = [{"pk": i["face_id"], "face_type":i["face_type"], "face_name":i["face_name"]}]
                result[i["face_id"]].append({"pk": i["parent_id"], "face_type":i["parent_type"], "face_name":i["parent_name"]})
    cursor.close()
    connection.close()
    return {"result": result, "time": timer}
