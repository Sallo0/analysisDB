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
    filters = []
    if data['kind'] != "-":
        filters.append(f'kind: "{data["kind"]}"')
    if data['date_begin'] != "":
        filters.append(f'date_begin: "{data["date_begin"]}"')
    if data['date_end'] != "":
        filters.append(f'date_end: "{data["date_end"]}"')
    if data['cost'] != "":
        filters.append(f'cost: "{data["cost"]}"')
    if data['share'] != "":
        filters.append(f'share: "{data["share"]}"')
    if data['child_liquidated'] == "true":
        filters.append(f'child_liquidated: "t"')
    elif data['child_liquidated'] == "false":
        filters.append(f'child_liquidated: "f"')

    if data['mainfilter']['Child'] != "" and data['mainfilter']['Parent'] != "":
        query.append("match (p{pk: '")
        query.append(data['mainfilter']['Parent'])
        query.append("'})-[r:Properties{")
        query.append(",".join(filters))
        query.append("}]->(c{pk: '")
        query.append(data['mainfilter']['Child'])
        query.append("'}) return PROPERTIES(r) ")
    elif data['mainfilter']['Child'] != "":
        query.append("match (p)-[r:Properties{")
        query.append(",".join(filters))
        query.append("}]->(c{pk: '")
        query.append(data['mainfilter']['Child'])
        query.append("'}) return PROPERTIES(r), p")
    elif data['mainfilter']['Parent'] != "":
        query.append("match (p{pk: '")
        query.append(data['mainfilter']['Parent'])
        query.append("'})-[r:Properties{")
        query.append(",".join(filters))
        query.append("}]->(c) return PROPERTIES(r), c")

    return "".join(query)


def f (data):
    res = {}
    el = []
    for i in data:
        x = i['m']['pk']
        if res.get(x) != None:
            el = []
            el = res.get(x)
            if i['b'] not in el:
                el.append(i['b'])
                res.update({x: el})
        else:
            el = []
            el.append(i['m'])
            el.append(i['b'])
            res.update({x: el})
    return res


def queryConstructorDeep(data):
    query = []
    query.append("Match (n:Example {face_id:'")
    query.append(data['mainfilter']['Child'])
    query.append("'})<-[r:Properties]-(b)-[t:Properties]-> (m) Return m, t")
    return "".join(query)


def getGraphDataNeo4j(request):
    data = request.data
    cypher_query = queryConstructorDeep(data)
    print(cypher_query)

    with connection.session(database="neo4j") as session:
        time_start = t.perf_counter()
        results = session.run(cypher_query).data()
        time_end = t.perf_counter()
        timer = time_end - time_start
        result_json = {'result': f(results), "time": timer}
        return result_json


def getDataNeo4j(request):
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
    connection.close()

    return results


connection.close()
