from dotenv import load_dotenv, dotenv_values
import os
import psycopg2
import json
import random
import time
from neo4j import GraphDatabase, basic_auth
from psycopg2.extras import RealDictCursor
from django.core.serializers.json import DjangoJSONEncoder

t = time
timer = 0

host = "46.48.3.74"
user_SQL = "postgres"
password_SQL = "postgres"
db_name_SQL = "postgres"
user_Neo = "neo4j"
password_Neo = "12345678"
to_json = {}

connection_SQL = psycopg2.connect(
        host=host,
        user=user_SQL,
        password=password_SQL,
        database=db_name_SQL
    )

connection_Neo = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "123"))


def createNodesNeo4j():
    cursor = connection_SQL.cursor('hui')
    cursor.execute('SELECT * FROM face_info')
    data = cursor.fetchmany(10)

    with connection_Neo.session(database="neo4j") as session:
        #time_start = t.perf_counter()
        for e in data:
            result_query = "".join(['CREATE (p: Object {face_id: ', str(e[0]), ', face_type: ', str(e[1]), ', face_name: "', str(e[2]), '"})'])
            session.run(result_query)
        #time_end = t.perf_counter()
        cursor.close()


def createLinksNeo4j():
    cursor.execute('SELECT * FROM links LIMIT 20')
    data_links = cursor.fetchall()
    with connection_Neo.session(database="neo4j") as session:
        for e in data_links:
            result_query = ['MATCH (a {face_id: ', str(e['parent']), '}), (b {face_id: ', str(e['child']), '}) ']
            result_query.append('MERGE (a)-[:PARENT{kind: ')
            result_query.append(str(e['kind']))
            result_query.append(', date_begin: date("')
            result_query.append(str(e['date_begin']))
            result_query.append('"), date_end: ')

            if str(e['date_end']) != "None":
                result_query.append('"')
                result_query.append(str(e['date_end']))
                result_query.append('", cost: ')
            else:
                result_query.append(' "", cost: ')

            result_query.append(str(e['cost']))
            result_query.append(', share: ')
            result_query.append(str(e['share']))
            result_query.append(', child_liquidated: "')
            result_query.append(str(e['child_liquidated']))
            result_query.append('"}]->(b)')

            #print("".join(result_query))
            session.run("".join(result_query))


with connection_SQL.cursor(cursor_factory=RealDictCursor) as cursor:
    st = 0
    step = 100
    for i in range(1):
        p = ""
        #query = 'SELECT * FROM links WHERE pk BETWEEN ' + str(st) + ' AND ' + str((st + step))
        #query = 'SELECT * FROM links LIMIT 20'
        #cursor.execute(query)
        #data = cursor.fetchall()
        #print(data)

        #createNodesNeo4j()
        #createLinksNeo4j()

connection_SQL.close()
connection_Neo.close()


"""
            parent_id = e[1]
            child_id = e[2]
            parent_r = session.run("".join(['MATCH (n{face_id: ', str(parent_id), '}) RETURN n']))
            child_r = session.run("".join(['MATCH (n{face_id: ', str(child_id), '}) RETURN n']))

            print(child_r)

            if parent_r.single() == None and child_r.single() == None:
                result_query = ['CREATE (parent: Object { face_id: ', str(e[1]), '}), ']
                result_query.append('(child: Object { face_id: ')
                result_query.append(str(e[2]))
                result_query.append('}), ')
                result_query.append('(parent)-[:PARENT{')
                result_query.append('kind: ')
                result_query.append(str(e[3]))
                result_query.append(', date_begin: date("')
                result_query.append(str(e[4]))
                result_query.append('"), date_end: ')
                if str(e[5]) != "None":
                    result_query.append('date("')
                    result_query.append(str(e[5]))
                    result_query.append('")')
                else:
                    result_query.append('date(null)')

                result_query.append(', cost: ')
                if str(e[6]) != "None":
                    result_query.append(str(e[6]))
                else:
                    result_query.append('"None"')

                result_query.append(', share: ')
                if str(e[7]) != "None":
                    result_query.append(str(e[7]))
                else:
                    result_query.append('"None"')
                result_query.append(', child_liquidated: "')
                result_query.append(str(e[8]))
                result_query.append('"}]->(child)')

                session.run("".join(result_query))
                # print("".join(result_query))

            elif parent_r.single() == None:         #Не существует parent, но существует child
                result_query = ['CREATE (parent: Object { face_id: ', str(e[1]), '}), ', '(parent)-[:PARENT]->(', child_r, ')']
                #session.run("".join(result_query))
            else:                                   #Не существует child, но существует parent
                print()

"""