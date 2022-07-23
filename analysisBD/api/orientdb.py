import pyorient as po
import json
import analysisBD.settings as config

class OrientDBRepository():

    client = None

    def __init__(self):
        self.client = po.OrientDB(config.ORIENTDB_IP, 2424)
        self.client.set_session_token( True )
        self.client.connect(config.ORIENTDB_LOGIN, config.ORIENTDB_PASS)
        self.client.db_open("dbforanal", config.ORIENTDB_LOGIN, config.ORIENTDB_PASS)

    def query(self, query):
        a =self.client.query(query)
        return json.dumps(list(map(lambda x: x.oRecordData, a)))

def getDataOrientDB(request):

    return OrientDBRepository().query("select name, out('Owning').name from Organization where name != 'EvilOrg' limit 25")