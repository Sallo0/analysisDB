import psycopg2
import json
import time
import os
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, dotenv_values
from django.core.serializers.json import DjangoJSONEncoder
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
                WITH
                query1 AS
                (
                Select a.*, b.* from links a, face_info b Where child = 11721855 and b.face_id = a.parent 
                )
                 query2 AS
                (
                Select a.*, b.* from links a, face_info b Where child = query1.parent and b.face_id = a.parent 
                )
                 query3 AS
                (
                Select a.*, b.* from links a, face_info b Where child = query2.parent and b.face_id = a.parent 
                )
                SELECT query3.face_id as parent_id,query3.face_type as parent_type,query3.face_name as parent_name, a.* 
                FROM query3, links b,face_info a 
                ORDER BY query3.date_end asc nulls first, b.child LIMIT 25 OFFSET 25
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