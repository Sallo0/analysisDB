import neo4j
import time
from neo4j import GraphDatabase, basic_auth

t = time

def getDataNeo4j(request):
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "123"))

    cypher_query = '''
    MATCH (ee:Person) WHERE ee.name = 'Emil' RETURN ee LIMIT 25;
    '''

    def create_node_tx(tx, name):
        for i in range(1000):
            tx.run("CREATE (ee:Person {name: 'Emil', from: 'Sweden', kloutScore: " + str(i) + "})", name=name)

    def get_node_tx(tx, name):
        time_start = t.perf_counter()
        result = tx.run(cypher_query, name=name)
        time_end = t.perf_counter()
        value = result.data()
        timer = time_end - time_start
        return value, timer

    with driver.session(database="neo4j") as session:
        results, timer = session.read_transaction(get_node_tx, "name")

    #with driver.session(database="neo4j") as session:
        #results, timer = session.write_transaction(create_node_tx, "name")

    driver.close()

    results.append({'time': timer, 'name': "time"})

    return results #{1: "Neo4j", 2: "тест"}   Это обязательно JSON
