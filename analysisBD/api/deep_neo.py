import neo4j
import time

from neo4j import GraphDatabase, basic_auth

t = time
timer = 0

connection = GraphDatabase.driver(
        "bolt://localhost:5332",
        auth=basic_auth("neo4j", "12345678"))


def queryConstructor(data):
    query = []
    query.append("Match (n {face_id:'")
    query.append(data['mainfilter']['Child'])
    query.append("'})<-[r]-(b)-[t]-> (m) Return m t")
    return "".join(query)


def getGraphDataNeo4j(request):
    data = request.data
    cypher_query = queryConstructor(data)
    print(cypher_query)

    with connection.session(database="neo4j") as session:
        time_start = t.perf_counter()
        results = session.run(cypher_query).data()
        time_end = t.perf_counter()
        timer = time_end - time_start
        result_json = {'result': results, "time": timer}
        return result_json

data = {
    "record-1": {
        "child": {
            "pk": 1,
            "name": "xui",
            "type": "1",
        },
        "parent-1": {
            "pk": 1,
            "name": "xui",
            "type": "1",
        },
        "parent-2": {
            "pk": 1,
            "name": "xui",
            "type": "1",
        }
    },
    "record-2": {
        "child": {
            "pk": 1,
            "name": "xui",
            "type": "1",
        },
        "parent-1": {
            "pk": 1,
            "name": "xui",
            "type": "1",
        },
        "parent-2": {
            "pk": 1,
            "name": "xui",
            "type": "1",
        }
    }
}

connection.close()

#Match (n:Example {face_id:""})<-[r:Properties]-(b)-[t:Properties]-> (m) Return m,n,b,r,t