import psycopg2
import json
import time
import os
from neo4j import GraphDatabase, basic_auth
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, dotenv_values
from django.core.serializers.json import DjangoJSONEncoder

time_obj = time
timer = 0.0

connection_Neo = GraphDatabase.driver(
        "bolt://localhost:5332",
        auth=basic_auth("neo4j", "12345678"))
"""
driver = connection_Neo


connection = psycopg2.connect(
            host="127.0.0.1", 
            port="5432",
            user='postgres',
            password='postgres',
            database='postgres'
            )

with connection.cursor(cursor_factory=RealDictCursor) as cursor:
    sql = "COPY (Select * from face_info) TO STDOUT WITH CSV HEADER"

    with open("/var/lib/neo4j/import/faces.csv", "w") as file:
        cursor.copy_expert(sql, file)

cursor.close()

with connection.cursor(cursor_factory=RealDictCursor) as cursor:
    sql = "COPY (Select * from links) TO STDOUT WITH CSV HEADER"

    with open("/var/lib/neo4j/import/links.csv", "w") as file:
        cursor.copy_expert(sql, file)

cursor.close()

"""
with connection_Neo.session(database="neo4j") as session:
    time_start = time_obj.perf_counter()

    #query_create_pk = 'CREATE CONSTRAINT exemplar_pk ON (n:Exemplar) ASSERT  n.pk IS UNIQUE;'
    #session.run(query_create_pk)

    #query_create_nodes = "USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///faces.csv' as line CREATE (:Exemplar {pk:line.face_id, face_type:line.fac>
    #session.run(query_create_nodes)

    #query_create_links = "USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///links2.csv' as line MATCH (p:Exemplar {pk: line.parent}), (c:Exemplar {pk:r {pk: line.child}) MERGE (p)-[:Properties{kind:line.kind, date_begin:line.date_begin, date_end:coalesce(line.date_end, '-'),cost:coalesce(line.cost, '-'),share:coare:coalesce(line.share, '-'), child_liquidated:coalesce(line.child_liquidated, '-')}]->(c);"

    query_create_link = 'CALL apoc.periodic.commit("MATCH(n)<-[:Properties]-(m) WITH n, COUNT(m) AS c WHERE c = ' + str(1) + ' RETURN n limit 25", {limit:1000});'
    res = session.run(query_create_link)
    print(res.data())
    time_end = time_obj.perf_counter()

    json = {
        "result":{
            "10901923": {
                "0": {
                    "pk": 123,
                    "face_type": 0,
                    "face_name": None,
                },
                "1": {
                    "pk": 123,
                    "face_type": 0,
                    "face_name": None,
                }
            },
            "10901924": {
                "0": {
                    "pk": 123,
                    "face_type": 0,
                    "face_name": None,
                },
                "1": {
                    "pk": 123,
                    "face_type": 0,
                    "face_name": None,
                },
                "2": {
                    "pk": 123,
                    "face_type": 0,
                    "face_name": None,
                }
            }
        },
        "time": 0
    }

"""
connection.close()
"""
connection_Neo.close()
#print(time_end - time_start)

