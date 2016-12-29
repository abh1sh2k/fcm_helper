import os,copy, json,ast,shutil,collections
from datetime import datetime, timedelta

from elasticsearch import Elasticsearch, helpers

from logHelper import logHelper




class elasticSearchHelper():

    """
    Initializer method
    """
    def __init__(self, logger, properties):
        self._logger = logger
        self._properties = properties

    def executeQueryAndFetchResults(self, job):

        hosts = ast.literal_eval(self._properties.get('elasticSearchHost'))
        port = self._properties.get('elasticSearchPort')
        timeout = int(self._properties.get('elasticSearchTimeout'))
        sizePerShard = int(self._properties.get('docsPerShard'))
        indexName = self._properties.get('indexName')
        docType = self._properties.get('masterAttributeDocType')
        fieldsWithTimezone = self._properties.get('gcmWithTimezone')
        fieldsWithoutTimezone = self._properties.get('gcmOnly')
        fileSize = int(self._properties.get('infoLogFileSize'))

        h = []
        for host in hosts:
            h.append({'host':host,'port':port, 'timeout': timeout})

        es = Elasticsearch(h)

        self._logger.debug('Created connection to ElasticSearch')

        campaignMethodId = job.get('campaignMethodId')
        es_query = job.get('es_query')
        limit = job.get('limit')
        type = job.get('type')
        timezone = job.get('timezone')

        fields = fieldsWithoutTimezone
        if timezone == 'local':
            fields = fieldsWithTimezone

        dataPath = self._properties.get('infoLogFilePath') + self._properties.get('dataPath') + str(campaignMethodId) + '/'

        self._logger.debug('dataPath: %s' % dataPath)
        self._logger.debug('es_queries: %s'%es_query)
        self._logger.debug('type: %s'%type)
        self._logger.debug('limit: %s'%limit)

        newQuery = self.getEffectiveElasticSearchQuery(es_query)

        self._logger.debug('Query after conversion: %s'%newQuery)
        self._logger.debug('campaignMethodId: %s'%campaignMethodId)
        self._logger.debug('indexName: %s'%indexName)
        self._logger.debug('docType: %s'%docType)
        self._logger.debug('sizePerShard: %s'%sizePerShard)

        scan_response = helpers.scan(client=es, index=indexName,
                                 doc_type=docType, query=newQuery,
                                 fields=fields, scroll='1m', search_type='scan',
                                 size=sizePerShard)

        cnt = 0
        self._logger.debug('Start fetching results from ElasticSearch \n campaignMethodId :[%s]'%(campaignMethodId))

        if os.path.exists(dataPath):
            shutil.rmtree(dataPath, ignore_errors=True)

        _logger = logHelper()

        _infoLogger = _logger.getInfoLogger(inFolder=dataPath, inFileName=str(campaignMethodId) + '.log', inFileSize=fileSize)

        for response in scan_response:
            try:
                cnt += 1
                gcmid, userid, androidid, tmz = '','','','+0000'

                fieldData = response.get('fields')
                if not fieldData:
                    continue

                gcmid = fieldData.get('gcmid')
                if not gcmid:
                    continue
                else:
                    gcmid = gcmid[0]

                userid = fieldData.get('userid',[''])
                if userid:
                    userid = userid[0]

                androidid = fieldData.get('androidid',[''])
                if androidid:
                    androidid = androidid[0]

                timezone1 = fieldData.get('timezone')
                if timezone1:
                    tmz = timezone1[0]

                _infoLogger.info('%s,%s,%s,%s'%(userid ,androidid, gcmid, tmz))

                if limit and limit <> 0 and cnt >= limit:
                    break

            except Exception, e:
                self._logger.debug(e)
                self._logger.debug(response)

        self._logger.debug('Finished fetching [%s] results from ElasticSearch for campaignMethodId [%s]'%(cnt,campaignMethodId))



    def getEffectiveElasticSearchQuery(self , inQuery):
        query = self.trimSpaces(inQuery)
        newQuery = '{"query": %s}' % (json.dumps(json.loads(query)))
        return newQuery


    def trimSpaces(self,inQuery):
        query = copy.deepcopy(inQuery)
        replace_text_pairs = {r'\r': '', r'\n': '', r'\t': ''}

        for old in replace_text_pairs:
            query = query.replace(old, replace_text_pairs[old])

        return query



