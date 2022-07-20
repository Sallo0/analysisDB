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

"""
connection = psycopg2.connect(
            host="46.48.3.74",
            #port="5432",
            user='postgres',
            password='postgres',
            database='postgres'
            )


with connection.cursor(cursor_factory=RealDictCursor) as cursor:
    sql = "COPY (Select * from face_info order by face_id LIMIT 20000) TO STDOUT WITH CSV HEADER"

    with open("C:/Users/Nikita/.Neo4jDesktop/relate-data/dbmss/dbms-fb0b047d-5797-4e55-a723-267203e15904/import/faces.csv", "w") as file:
        cursor.copy_expert(sql, file)

cursor.close()

with connection.cursor(cursor_factory=RealDictCursor) as cursor:
    sql = "COPY (Select * from links order by parent LIMIT 20000) TO STDOUT WITH CSV HEADER"

    with open("C:/Users/Nikita/.Neo4jDesktop/relate-data/dbmss/dbms-fb0b047d-5797-4e55-a723-267203e15904/import/links.csv", "w") as file:
        cursor.copy_expert(sql, file)

cursor.close()
connection.close()
"""

"""
connection_Neo = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "123"))
"""
connection_Neo = GraphDatabase.driver(
        "bolt://46.48.3.74:5332",
        auth=basic_auth("neo4j", "12345678"))

driver = connection_Neo

with connection_Neo.session(database="neo4j") as session:
    time_start = time_obj.perf_counter()

    #query_create_pk = 'create constraint exemplar_pk for (n:Exemplar) require n.pk is unique;'
    #session.run(query_create_pk)

    #query_create_nodes = "USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///faces.csv' as line CREATE (:Exemplar {pk:line.face_id, face_type:line.face_type, face_name:coalesce(line.face_name, '-')});"
    #session.run(query_create_nodes)

    #query_create_links = "USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///links.csv' as line MATCH (p:Exemplar {pk: line.parent}), (c:Exemplar {pk: line.child}) MERGE (p)-[:Properties{kind:line.kind, date_begin:line.date_begin, date_end:coalesce(line.date_end, '-'),cost:coalesce(line.cost, '-'),share:coalesce(line.share, '-'), child_liquidated:line.child_liquidated}]->(c);"
    #session.run(query_create_links)

    query_create_links = "MATCH (p{pk: '15314139'})-[l]->(c{pk: '10398095'}) RETURN l LIMIT 10"
    res = session.run(query_create_links)
    print(res.data())


    time_end = time_obj.perf_counter()
connection_Neo.close()
print(time_end - time_start)

