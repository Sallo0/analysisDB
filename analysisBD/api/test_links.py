from dotenv import load_dotenv, dotenv_values
import os
import time
from neo4j import GraphDatabase, basic_auth
t = time

connection = GraphDatabase.driver(
        "bolt://localhost:5332",
        auth=basic_auth("neo4j", "12345678"))

with connection.session(database="neo4j") as session:
    cypher_query = "match (p)-[r:Properties{}]->(c{pk: '11721855'}) return PROPERTIES(r), p"
    time_start = t.perf_counter()
    results = session.run(cypher_query).data()
    time_end = t.perf_counter()
    timer = time_end - time_start
    result_json = {'result': results, "time": timer}

    print(result_json)


connection.close()
