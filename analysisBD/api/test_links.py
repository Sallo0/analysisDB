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


def wait_until_threadings_finished():
    connection_SQL.close()
    connection_Neo.close()
    time_end = time_obj.perf_counter()
    print(time_end - time_start)


def createLinks(arr_of_links, start, end):
    with connection_Neo.session(database="neo4j") as session:
        for e in arr_of_links[start:end]:
            result_query = ['MATCH (a {face_id: ', str(e[1]), '}), (b {face_id: ', str(e[2]), '}) ']
            result_query.append('MERGE (a)-[:PARENT{kind: ')
            result_query.append(str(e[3]))
            result_query.append(', date_begin: date("')
            result_query.append(str(e[4]))
            result_query.append('"), date_end: ')

            if str(e[5]) != "None":
                result_query.append('"')
                result_query.append(str(e[5]))
                result_query.append('", cost: ')
            else:
                result_query.append(' "", cost: ')

            if str(e[6]) != "None":
                result_query.append('')
                result_query.append(str(e[6]))
                result_query.append(', share: ')
            else:
                result_query.append('"", share: ')

            if str(e[7]) != "None":
                result_query.append('')
                result_query.append(str(e[7]))
                result_query.append(', child_liquidated: "')
            else:
                result_query.append('"", child_liquidated: "')

            result_query.append(str(e[8]))
            result_query.append('"}]->(b)')
            session.run("".join(result_query))


with connection_SQL.cursor(name='test1') as cursor:
    query = "SELECT * FROM links"
    cursor.execute(query)
    step = 2000
    time_start = time_obj.perf_counter()
    for i in range(5):
        res = cursor.fetchmany(16000)
        start = 0
        end = 0
        for thread in range(8):
            end += step
            t2 = threading.Thread(target=createLinks, args=(res, start, end))
            t2.start()
            start = end
    cursor.close()
    wait_until_threadings_finished()
