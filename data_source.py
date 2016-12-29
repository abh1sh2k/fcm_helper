import MySQLdb as mysql
import json, collections

class dataSource():

    """
    Initializer method
    """
    def __init__(self, logger, properties):
        self._logger = logger
        self._properties = properties


    def connect(self):
        host = self._properties.get('auroraHost')
        port = int(self._properties.get('auroraPort'))
        db = self._properties.get('auroraDB')
        username = self._properties.get('auroraUser')
        password = self._properties.get('auroraPassword')

        conn = mysql.connect(host=host, user=username, passwd=password, db=db, port=port, charset='utf8', use_unicode=True)

        return conn


    def getJobsToExecute(self):
        conn = self.connect()
        cursor = conn.cursor()

        stmt = """
         select campaignmethodid ,segmentid , ribbonid , payload from ribbon_payload
        """
        self._logger.debug(stmt)

        cursor.execute(stmt)
        rows = cursor.fetchall()
        conn.close()

        jobs = []
        for row in rows:
            job = collections.defaultdict()
            job['campaignmethodid'] = row[0]
            job['segmentid'] =  row[1]
            job['ribbonid'] = row[2]
            job['payload'] = json.dumps(json.loads(row[3]))

            jobs.append(job)

        return jobs




