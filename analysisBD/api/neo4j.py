import neo4j
import time
from neo4j import GraphDatabase, basic_auth

t = time
connection = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "123"))


def getDataNeo4j(request):
    driver = connection

    cypher_query = '''
    MATCH (ee:Person) WHERE ee.name = 'Emil' RETURN ee LIMIT 25;
    '''

    def get_node_tx(tx, name):
        time_start = t.perf_counter()
        result = tx.run(cypher_query, name=name)
        time_end = t.perf_counter()
        value = result.data()
        timer = time_end - time_start
        return value, timer

    with driver.session(database="neo4j") as session:
        results, timer = session.read_transaction(get_node_tx, "name")

    driver.close()

    results.append({'time': timer, 'name': "time"})

    return results #{1: "Neo4j", 2: "тест"}   Это обязательно JSON


def createTestDataNe04j(request):
    driver = connection

    def create_node_tx(tx, name):
        time_start = t.perf_counter()
        for i in range(int(10)):
            tx.run("CREATE (ee:Person {name: 'Emil', from: 'Sweden', kloutScore: " + str(i) + "})", name=name)
        time_end = t.perf_counter()
        return time_end - time_start

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(create_node_tx, "name")

    driver.close()

    return results
