import psycopg2
import json
import time
import os
from neo4j import GraphDatabase, basic_auth
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, dotenv_values
from django.core.serializers.json import DjangoJSONEncoder

connection_Neo = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "123"))

driver = connection_Neo

connection = psycopg2.connect(
            host='46.48.3.74',
            user='postgres',
            password='postgres',
            database='postgres'
            )

with connection.cursor(cursor_factory=RealDictCursor) as cursor:
    sql = "COPY (Select * from face_info LIMIT 10000) TO STDOUT WITH CSV HEADER"

    with open("C:/Users/Nikita/.Neo4jDesktop/relate-data/dbmss/dbms-fb0b047d-5797-4e55-a723-267203e15904/import/faces.csv", "w") as file:
        cursor.copy_expert(sql, file)

cursor.close()

with connection.cursor(cursor_factory=RealDictCursor) as cursor:
    sql = "COPY (Select * from links LIMIT 10000) TO STDOUT WITH CSV HEADER"

    with open("C:/Users/Nikita/.Neo4jDesktop/relate-data/dbmss/dbms-fb0b047d-5797-4e55-a723-267203e15904/import/links.csv", "w") as file:
        cursor.copy_expert(sql, file)

cursor.close()

with connection_Neo.session(database="neo4j") as session:
    query_create_pk = 'create constraint exemplar_pk for (n:Exemplar) require n.pk is unique;'
    session.run(query_create_pk)

    query_create_nodes = ":auto USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///faces.csv' as line CREATE (:Exemplar {pk:line.face_id, face_type:line.face_type,face_name:line.face_name});"
    session.run(query_create_nodes)

    query_create_links = ":auto USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///links.csv' as line MATCH (p:Exemplar {pk: line.parent}), (c:Exemplar {pk: line.child}) MERGE (p)-[:Properties{kind:line.kind,child_liquidated:line.child_liquidated,date_begin:line.date_begin, date_end:coalesce(line.date_end, "-"),cost:coalesce(line.cost, "-"),share:coalesce(line.share, "-")}]->(c) "
    session.run(query_create_links)

connection.close()
connection_Neo.close()