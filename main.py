from datetime import datetime
import gzip , os
from configManager import  configManager
from logHelper import logHelper
import collections
import concurrent.futures
from time import sleep
from pyfcm import FCMNotification
import traceback
from time import sleep
from botoHelper import botoHelper


def uncompressGzipFilesinDir(dir):
    for path, dir, files in os.walk(dir):
        for file in files:
            filepath = os.path.join(path, file)
            _debugLogger.debug('uncompressing file : %s' % filepath)
            if '.gz' in filepath:
                uncompressed_file_path = filepath[0:-3]
                f_out = open(uncompressed_file_path , 'wb')
                f_in = gzip.open(filepath , 'rb')
                f_out.writelines(f_in)
                f_out.close()
                f_in.close()
                os.unlink(filepath )

def uncompressGzipFile(filepath):
    f_out = open(filepath , 'wb')
    f_in = gzip.open(filepath +'.gz', 'rb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    os.unlink(filepath+'.gz' )

def sendNotification( filepath , payload , campaignmethodid , segmentid , _debugLogger , limit ):
    job_id = '%s-%s-%s' % (campaignmethodid,segmentid ,  datetime.now().strftime('%Y%m%d'))
    payload['jobid'] = job_id
    payload['source'] = 'fcm'
    gcmDict = collections.OrderedDict()
    _debugLogger.debug('processing file : %s' % filepath)
    _infoLogger = _logger.getInfoLogger(inFolder='/mnt/ebs/fcm/ribbon_result/', inFileName=job_id + '.log',inFileSize=fileSize)
    with open(filepath, 'r') as fin:
        for line in fin:
            t = line.strip().split(',')
            di = {}
            di['userid'] = t[0]
            di['androidid'] = t[1]
            #di['timezone'] = d
            gcmDict[t[2]] = di

        startPushNotificationThreads(gcmDict, payload , campaignmethodid, job_id , _debugLogger , limit , _infoLogger)



def startPushNotificationThreads(gcmDict, payload , campaignMethodId, jobId , _debugLogger , limit , _infoLogger):
    threadId = 0
    newGCMDict = collections.defaultdict(dict)
    with concurrent.futures.ThreadPoolExecutor(max_workers=_maxThreadCount) as executor:
        cnt = 0
        for key in gcmDict:
            cnt += 1
            newGCMDict[key] = gcmDict[key]
            if cnt % _gcmMaxMessages == 0:
                threadId += 1
                _debugLogger.debug('Submitting thread %s for campaign method %s' % (threadId, campaignMethodId))

                executor.submit(sendMessage, threadId, campaignMethodId, newGCMDict, payload, jobId , _debugLogger, _infoLogger)
                newGCMDict = collections.defaultdict(dict)

                sleep(1)

                if threadId % 100 == 0:
                    sleep(10)
            if cnt > limit:
                break

        if len(newGCMDict) > 0:
            threadId += 1
            executor.submit(sendMessage, threadId, campaignMethodId, newGCMDict, payload, jobId , _debugLogger , _infoLogger)


def sendMessage( threadId, campaignMethodId, gcmDict, data, jobId , _debugLogger , _infoLogger):
    _debugLogger.debug('Sending message for thread %s.' % threadId)
    key = '%s' % (campaignMethodId)
    cid = int(campaignMethodId)
    jid = jobId

    reg_ids = [y for y in gcmDict]

    fcm_result = _fcm.notify_multiple_devices(registration_ids=reg_ids , data_message=data , collapse_key=key , delay_while_idle=False , dry_run=False , time_to_live=86400)

    parseFCMResponse(jid, cid, fcm_result, reg_ids, gcmDict , _infoLogger)
    _debugLogger.debug('Sent message for thread campaignMethodId %s %s.' % (campaignMethodId,threadId))


def parseFCMResponse( jobId, campaignMethodId, fcm_result, reg_ids, gcmDict , _infoLogger):
    thread_now_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    thread_date = datetime.now().strftime('%Y-%m-%d')

    if 'results' in fcm_result:
        results = fcm_result['results']
        i = 0
        for result in results:
            reg_id = reg_ids[i]
            userid = gcmDict[reg_id].get('userid')
            androidid = gcmDict[reg_id].get('androidid')
            if 'message_id' in results[i]:
                _infoLogger.info('%s,%s,%s,%s,%s,%s,%s,1,%s' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid, reg_id, result['message_id']))
            if 'registration_id' in results[i]:
                _infoLogger.info('%s,%s,%s,%s,%s,%s,%s,2,%s' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid, reg_id , result['registration_id']))
            if 'error' in results[i]:
                if 'NotRegistered' == result['error']:
                    _infoLogger.info('%s,%s,%s,%s,%s,%s,%s,3' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid, reg_id))
                else:
                    _infoLogger.info('%s,%s,%s,%s,%s,%s,%s,4,%s' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid, reg_id, result['error']))
            i += 1


if __name__ == '__main__':
    config = configManager()
    _properties = config._properties
    _logger = logHelper()
    _debugLogger = _logger.getDebugLogger(debugLogFileName='ribbon.log')
    _botoHelper = botoHelper(_properties)
    try:
        _maxThreadCount = int(_properties.get('maxThreadCount'))
        _gcmMaxMessages = int(_properties.get('gcmMaxMessages'))
        _logger = logHelper()
        _fcm = FCMNotification(api_key=_properties.get('gcmApiKey'))
        fileSize = int(_properties.get('infoLogFileSize'))
        infoLogFilePath = _properties.get('infoLogFilePath')
        dataPath = _properties.get('dataPath')
        resultPath = _properties.get('resultPath')


        local_path = 'ribbon/'

        campaignmethodids = [10000 , 10001 , 10002 , 10003 , 10004 , 10005 , 10006 , 10007 , 10008 , 10009 , 10010 , 10011 , 10012 , 10013 , 10014 , 10015 , 10016 , 10017 , 10018 , 10019]
        segmentids = [0,1,2,3,4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19]

        import json
        from pprint import pprint

        with open('ribbon_payload.json') as data_file:
            payload_data = json.load(data_file)

        #pprint(payload_data)

        for id in segmentids:
            local_file_name = infoLogFilePath + local_path + 'segment' + str(id) + '.txt.gz'
            key = 'recent_active_users/segments/' + datetime.now().strftime('%Y/%m/%d') + '/segment' + str(id) + '_000.gz'
            _debugLogger.debug('downlaoding  key : %s' % key)
            _botoHelper.downloadFileFromS3(key, local_file_name)
            file_to_un_compress = infoLogFilePath + local_path + 'segment' + str(id) + '.txt'
            _debugLogger.debug('uncompressing file to : %s' % file_to_un_compress)
            uncompressGzipFile(file_to_un_compress)

        limit = 100000
        for campaignmethodid, segmentid in zip(campaignmethodids, segmentids):
            try :
                payload = payload_data[str(campaignmethodid)]['payload']
                filepath = infoLogFilePath + local_path + 'segment'+ str(segmentid)+'.txt'
                print payload , filepath , campaignmethodid , segmentid
                sendNotification(filepath, payload, campaignmethodid , segmentid , _debugLogger , limit)
                sleep(5)
            except Exception, e:
                _debugLogger.debug(traceback.print_exc())


        _botoHelper.upload_result_files(infoLogFilePath + 'ribbon_result/' , 'recent_active_users/results/'+datetime.now().strftime('%Y/%m/%d/') , _debugLogger)
    except Exception, e:
        _debugLogger.debug(traceback.print_exc())
