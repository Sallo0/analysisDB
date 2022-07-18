from dotenv import load_dotenv, dotenv_values
import os
import psycopg2
import json
import random
import time
from neo4j import GraphDatabase, basic_auth
from psycopg2.extras import RealDictCursor
from django.core.serializers.json import DjangoJSONEncoder

import threading


connection_Neo = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "123"))

driver = connection_Neo

time_obj = time
timer = 0.0

connection_SQL = psycopg2.connect(
            host='46.48.3.74',
            user='postgres',
            password='postgres',
            database='postgres'
            )


def createNodes(arr_of_nodes, start, end):
    with connection_Neo.session(database="neo4j") as session:
        for e in arr_of_nodes[start: end]:
            result_query = "".join(
                ['CREATE (p: Object {face_id: ', str(e[0]), ', face_type: "',
                 str(e[1]), '", face_name: "', str(e[2]), '"})'])
            session.run(result_query)


def wait_until_threadings_finished():
    connection_SQL.close()
    connection_Neo.close()
    time_end = time_obj.perf_counter()
    print(time_end - time_start)


with connection_SQL.cursor(name='test') as cursor:

    query = "SELECT * FROM face_info"
    cursor.execute(query)
    step = 2000
    time_start = time_obj.perf_counter()
    for i in range(5):
        res = cursor.fetchmany(16000)
        start = 0
        end = 0

        threads = []
        for thread in range(8):
            end += step
            t1 = threading.Thread(target=createNodes, args=(res, start, end))
            threads.append(t1)
            start = end

        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()

    cursor.close()
    wait_until_threadings_finished()