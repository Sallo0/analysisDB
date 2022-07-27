import pyorient as po
import json
import os
import time


t = time


class OrientDBRepository():

    def __init__(self):
        self.client = po.OrientDB(os.getenv('orientdb_ip'), 2424)
        self.client.set_session_token( True )
        self.client.connect(os.getenv('orientdb_login'), os.getenv('orientdb_pass'))
        self.client.db_open("analysis", os.getenv('orientdb_login'), os.getenv('orientdb_pass'))

    def query(self, query):
        a =self.client.query(query)
        return json.dumps(list(map(lambda x: x.oRecordData, a)))

    def over_knee(self, id):
        raw_resp = self.client.query(f"select " \
                                f"outV().id as parent, outV().out().id as parent_childs " \
                                f"from (traverse inE() from (select from Face where id = {id}))")
        childsDict = list(map(lambda x: x.oRecordData, raw_resp))
        parentsDict = {}
        for parent,childs in childsDict:
            for child in childs:
                if child not in parentsDict.keys():
                    parentsDict[child] = []
                parentsDict[child].append(parent)

        return parentsDict

    def flat_list(self, filter_data):
        query = queryConstructor(filter_data)
        time_start = t.perf_counter()
        result = list(map(lambda x: x.oRecordData, self.client.query(query)))
        time_end = t.perf_counter()
        timer = time_end - time_start
        return {"result": result, "time": timer}


def queryConstructor(data):
    """Создаёт sql запрос в виде строки по полученным фильтрам с сайта
    Args:
        data (dict): Json словарь полученый с фронта.
    Returns:
        str: Готовый SQL запрос.
    """
    sql_args = []

    is_par_set = False
    is_ch_set = False
    for k, v in data.items():
        if v == "" or v == "-":
            continue
        elif k == "date_begin":
            sql_args.append(f"parentEdge.{k}>=date('{v}', 'yyyy-MM-dd')")
        elif k == "date_end":
            sql_args.append(f"parentEdge.{k}<=date('{v}', 'yyyy-MM-dd')")
        elif k == "mainfilter":
            if v['Parent'] != "":
                sql_args.append(f"parentEdge.out.id={v['Parent']}")
                is_par_set = True
            if v['Child'] != "":
                sql_args.append(f"parentEdge.in.id={v['Child']}")
                is_ch_set = True
        else:
            sql_args.append(f"parentEdge.{k}={v}")

    if len(data) == 0:
        sql_args.append(f'pk=0')

    pattern = "parentEdge.{0}.name as name,parentEdge.{0}.id as id,parentEdge.{0}.face_type as type,"
    projection = "" if is_par_set and is_ch_set else pattern.format("in") if is_par_set else pattern.format("out")
    query = [f"select ",
             projection,
             f"parentEdge.cost as cost,",
             f"parentEdge.share as share,",
             f"parentEdge.kind as kind,",
             f"parentEdge.date_begin as date_begin,",
             f"parentEdge.date_end as date_end,",
             f"parentEdge.child_liquidated as child_liquidated "
             f"from (select {'inE()' if is_ch_set else 'outE()'} as parentEdge",
             f"      from Face ",
             f"      where id = {data['mainfilter']['Child'] if is_ch_set else data['mainfilter']['Parent']} ",
             f"      unwind parentEdge)",
             f" where ",
             " and ".join(sql_args)]

    return "".join(query)


def getDataOrientDB(request):
    data = request.data
    data.pop("dbtype")
    return OrientDBRepository().flat_list(request.data)