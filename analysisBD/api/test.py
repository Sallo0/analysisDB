import psycopg2
import json
import time
import os
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, dotenv_values
from django.core.serializers.json import DjangoJSONEncoder

t = time

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
                host="46.48.3.74",
                user="postgres",
                password="postgres",
                database="postgres",
                )
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        query = f""" 
                with
query1 as
(
select parent as first_parent, date_end from links where child = {request}
),
query2 as
(
select query1.*, a.parent as second_parent, a.date_end from links a, query1 where a.child = query1.first_parent
)
select DISTINCT a.* from query2, links b, face_info a where (query2.first_parent = b.parent or query2.second_parent = b.parent and b.child != {request} and a.face_id = b.child) or query2.first_parent = a.face_id or query2.second_parent = a.face_id offset 25
                """
        time_start = t.perf_counter()
        cursor.execute(query)
        time_end = t.perf_counter()
        timer = time_end - time_start
        res = cursor.fetchall()
    cursor.close()
    connection.close()
    return res, timer

print(colenoSQL(123))