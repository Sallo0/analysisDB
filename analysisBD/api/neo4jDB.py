import neo4j
import time

from neo4j import GraphDatabase, basic_auth

t = time

connection = GraphDatabase.driver(
        "bolt://localhost:5332",
        auth=basic_auth("neo4j", "12345678"))


def queryConstructor(data):
    """Конструирует запрос на языке cypher для получения плоского списка вершин и связей в БД neo4j.

        Args:
            data (object): Объект, в котором хранится json словарь с данными из запроса.

        Keyword Args:
            query (list): Лист, который хранит cypher-запрос по частям
            filters (list): Лист, который хранит часть cypher-запроса, содержащую фильтры

        Returns:
            str: Возвращает cypher-запрос в формате строки

        """
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
        query.append("match (p:Exemplar{pk: '")
        query.append(data['mainfilter']['Parent'])
        query.append("'})-[r:Properties{")
        query.append(",".join(filters))
        query.append("}]->(c:Exemplar{pk: '")
        query.append(data['mainfilter']['Child'])
        query.append("'}) return PROPERTIES(r) ORDER BY r.date_end, r.kind")
    elif data['mainfilter']['Child'] != "":
        query.append("match (p:Exemplar)-[r:Properties{")
        query.append(",".join(filters))
        query.append("}]->(c:Exemplar{pk: '")
        query.append(data['mainfilter']['Child'])
        query.append("'}) return PROPERTIES(r), p ORDER BY r.date_end, r.kind, p.pk")
    elif data['mainfilter']['Parent'] != "":
        query.append("match (p:Exemplar{pk: '")
        query.append(data['mainfilter']['Parent'])
        query.append("'})-[r:Properties{")
        query.append(",".join(filters))
        query.append("}]->(c:Exemplar) return PROPERTIES(r), c ORDER BY r.date_end, r.kind, c.pk")

    return "".join(query)


def f(data):
    """Приводит данные, полученные из neo4j в json формат для отправки обратно на клиент.

            Args:
                data (object): Объект, в котором хранятся данные, полученные из neo4j

            Returns:
                res: Возвращает json-объект с данными из neo4j

    """
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
    """Конструирует запрос на языке cypher для получения вершин, которые связаны с текущей через parent (поиск через
        колено).

            Args:
                data (object): Объект, в котором хранится json словарь с данными из запроса.

            Keyword Args:
                query (list): Лист, который хранит cypher-запрос по частям

            Returns:
                str: Возвращает cypher-запрос в формате строки

    """
    query = ["Match (n:Exemplar {pk:'", data['mainfilter']['Child'],
             "'})<-[r]-(b:Exemplar)-[t]-> (m:Exemplar) WHERE m.pk<>'",
             data['mainfilter']['Child'], "' Return m, b ORDER BY r.date_end, toInteger(m.pk) ", "SKIP ",
             str((data['page'] - 1) * 25), " LIMIT 25"]
    return "".join(query)


def getGraphDataNeo4j(request):
    """Выполняет запрос на получение данных (поиск через колено) из neo4j .

            Args:
                request (object): Объект, в котором хранится json-словарь с данными, полученный в POST-запросе.

            Keyword Args:
                data (object): Непосредственно сам json-словарь с данными из запроса
                cypher_query (str): Строка запроса получения данных из neo4j (на языке cypher)
                timer (int): Время, за которое выполняется запрос
                results (list): Список с данными, полученными из neo4j по запросу cypher_query

            Returns:
                result_json: Возвращает Объект с данными из БД neo4j и временем, за которое они были получены

    """
    data = request.data
    cypher_query = queryConstructorDeep(data)

    with connection.session(database="neo4j") as session:
        time_start = t.perf_counter()
        results = session.run(cypher_query).data()
        time_end = t.perf_counter()
        timer = time_end - time_start
        result_json = {'result': f(results), "time": timer}

        return result_json


def getDataNeo4j(request):
    """Выполняет запрос на получение данных (плоского списка) из neo4j.

                Args:
                    request (object): Объект, в котором хранится json-словарь с данными, полученный в POST-запросе.

                Keyword Args:
                    data (object): Непосредственно сам json-словарь с данными из запроса
                    cypher_query (str): Строка запроса получения данных из neo4j (на языке cypher)
                    timer (int): Время, за которое выполняется запрос
                    results (list): Список с данными, полученными из neo4j по запросу cypher_query

                Returns:
                    result_json: Возвращает Объект с данными из БД neo4j и временем, за которое они были получены.
                        Данные содержат информацию о вершине и связи.

    """
    data = request.data
    cypher_query = queryConstructor(data)

    with connection.session(database="neo4j") as session:
        time_start = t.perf_counter()
        results = session.run(cypher_query).data()
        time_end = t.perf_counter()
        timer = time_end - time_start
        result_json = {'result': results, "time": timer}
        return result_json


connection.close()
