from elasticSearchHelper import  elasticSearchHelper
from logHelper import  logHelper
from configManager import  configManager
from datetime import datetime
from fcmNotification import fcmNotification
import collections


if __name__ == '__main__':
    es_query = '{"query":{"filtered":{"filter":{"bool":{"must":{"bool":{"must":{"terms":{"userid":[508397978,856247344 ,860300180,863548610]}}}}}}}}}'
    #es_query= '{"query": {"filtered": {"filter": {"bool": {"must": {"bool": {"must": [{"query": {"wildcard": {"device_country_full": "india"}}}, {"range": {"latest_appversion": {"from": "9.4.3.5"}}}]}}}}}}}'
    _logger = logHelper()
    config = configManager()
    _properties = config._properties
    payload ={ "source": "fcm" , "title": "Video Songs, Trailers, Funny Videos", "image": "https://i.ytimg.com/vi/CvPdtf8Ijj4/mqdefault.jpg", "message": "Catchup with latest trailers, funny videos and video songs!", "type": "url", "url":"peel://home?dest=streaming"}
    print payload

    job = collections.defaultdict()
    job['campaignMethodId'] = 100000 # temp for ribbon
    job['payload'] = payload
    job['limit'] = 50000 #total notification to send
    job['es_query'] = es_query
    timezone = 'utc'
    #elasticSearchHelper(_logger, _properties).executeQueryAndFetchResults(job)
    fcmNotification(_logger, _properties).initializePushProcess(payload)

