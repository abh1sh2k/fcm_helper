from botoHelper import botoHelper
import json
from datetime import datetime
import gzip , os
from configManager import  configManager
from logHelper import logHelper
import collections
import concurrent.futures
from time import sleep
from pyfcm import FCMNotification
import traceback

config = configManager()
_properties = config._properties
_maxThreadCount = int(_properties.get('maxThreadCount'))
_gcmMaxMessages = int(_properties.get('gcmMaxMessages'))
_logger = logHelper()
_fcm = FCMNotification(api_key=_properties.get('gcmApiKey'))
_outputPath = _properties.get('infoLogFilePath')
fileSize = int(_properties.get('infoLogFileSize'))
resultPath = _outputPath + _properties.get('resultPath')
_debugLogger =  _logger.getDebugLogger(debugLogFileName='ribbon.log')

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

def sendNotification( filepath , payload , campaignMethodId):
    jobId = '%s-%s' % (campaignMethodId, datetime.now().strftime('%Y%m%d'))
    payload['jobid'] = jobId
    payload['source'] = 'fcm'
    gcmDict = collections.OrderedDict()
    _debugLogger.debug('processing file : %s' % filepath)
    with open(filepath, 'r') as fin:
        for line in fin:
            a, b, c, d = line.strip().split(',')
            di = {}
            di['userid'] = a
            di['androidid'] = b
            di['timezone'] = d
            gcmDict[c] = di

        startPushNotificationThreads(gcmDict, payload , campaignMethodId, jobId)



def startPushNotificationThreads(gcmDict, payload , campaignMethodId, jobId):
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

                executor.submit(sendMessage, threadId, campaignMethodId, newGCMDict, payload, jobId)
                newGCMDict = collections.defaultdict(dict)

                if threadId % 100 == 0:
                    sleep(10)

    if len(newGCMDict) > 0:
        threadId += 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=_maxThreadCount) as executor:
            executor.submit(sendMessage, threadId, campaignMethodId, newGCMDict, payload, jobId)


def sendMessage( threadId, campaignMethodId, gcmDict, data, jobId):
    _debugLogger.debug('Sending message for thread %s.' % threadId)
    key = '%s' % (campaignMethodId)
    cid = int(campaignMethodId)
    jid = jobId

    reg_ids = [y for y in gcmDict]

    fcm_result = _fcm.notify_multiple_devices(registration_ids=reg_ids, data_message=data, collapse_key=key,delay_while_idle=False, dry_run=True,time_to_live=86400)

    parseFCMResponse(jid, cid, fcm_result, reg_ids, gcmDict)
    _debugLogger.debug('Sent message for thread campaignMethodId %s %s.' % (campaignMethodId,threadId))


def parseFCMResponse( jobId, campaignMethodId, fcm_result, reg_ids, gcmDict):
    thread_now_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    thread_date = datetime.now().strftime('%Y-%m-%d')

    _infoLogger = _logger.getInfoLogger(inFolder='/mnt/ebs/fcm/ribbon_result/',
                                        inFileName=str(campaignMethodId) +'.log',
                                        inFileSize=fileSize)

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
    try:
        s3_key_suffix = datetime.now().strftime('%Y/%m/%d/%H/')
        local_file_name_prefix = datetime.now().strftime('%Y_%m_%d_%H')
        local_dir = '/mnt/ebs/fcm/activation/'
        '''_botoHelper = botoHelper(_properties)
        _botoHelper.downloadFileFromS3('notification/activation_users/newmovietrailers/000.gz' , local_dir  + '100000001.gz')
        _botoHelper.downloadFileFromS3('notification/activation_users/TheRichest/000.gz', local_dir + '100000002.gz')
        _botoHelper.downloadFileFromS3('notification/activation_users/GagReel/000.gz', local_dir + '100000003.gz')
        _botoHelper.downloadFileFromS3('notification/activation_users/JustReleasedMusicVideos/000.gz', local_dir + '100000004.gz')
        _botoHelper.downloadFileFromS3('notification/activation_users/peeltv_US/000.gz', local_dir + '100000005.gz')
        uncompressGzipFilesinDir(local_dir)'''
        campaign_method_id = 100000001
        payloads = [{"title": "Latest Movie Trailers", "url": "peel://contents/?type=streaming&id=newmovietrailers&title=Latest Movie Trailers&display_type=current&aspect_ratio=16x9", "strategic": "true", "priority": "high", "message": "Latest movie trailers from hollywood!", "type": "URL", "image":"https://i.ytimg.com/vi/iSGUAlTDyO8/mqdefault.jpg"},
    {"title": "The Richest - Latest Videos", "url": "peel://contents/?type=streaming&id=TheRichest&title=The Richest &display_type=current&aspect_ratio=16x9&autoplay=true", "strategic": "true", "priority": "high", "message": "Shocking, controversial, mysterious, funny, weird and mind-blowing facts", "type": "URL", "image": "https://i.ytimg.com/vi/iYdjxvnNF_s/mqdefault.jpg"},
    {"title": "Latest Videos - Horror Parasites! Beware!", "url": "peel://contents/?type=streaming&id=GagReel&title=Aww's Fails Gags&display_type=current&aspect_ratio=16x9", "strategic": "true", "priority": "high", "message": "The Most Gruesome Parasites - Neglected Tropical Diseases - NTDs", "type": "URL", "image":"https://i.ytimg.com/vi/qNWWrDBRBqk/mqdefault.jpg"},
    {"title": "Just Released Music Videos", "url": "peel://contents/?type=streaming&id=JustReleasedMusicVideos&title=Just Released Music Videos&display_type=current&aspect_ratio=16x9&autoplay=true", "strategic": "true", "priority": "high", "message": "Latest Music Videos and Albums", "type": "URL", "image": "https://i.ytimg.com/vi/Xn599R0ZBwg/mqdefault.jpg"},
    {"title": "News highlights from around the world!","url": "peel://contents/?type=streaming&id=peeltv_US&title=New Highlights&display_type=current&aspect_ratio=16x9","strategic": "true", "priority": "high", "message": "Breaking News, Latest Updates etc", "type": "URL","image": "https://i.ytimg.com/vi/KmY5LmaZImk/mqdefault.jpg"}
                    ]
        ids = ['newmovietrailers','TheRichest','GagReel','JustReleasedMusicVideos','peeltv_US']
        while campaign_method_id <= 100000005 :
            payload = payloads[campaign_method_id-100000001]
            #payload['display_time'] = '2016-12-06T17:00:00' wrong one
            payload['display_time'] = '2016-12-07T04:50:00.0+00:00'
            print campaign_method_id , json.dumps(payload) , ids[campaign_method_id-100000001]
            sendNotification( local_dir +str(campaign_method_id) , payload , campaign_method_id )
            campaign_method_id+=1
    except Exception, e:
        _debugLogger.debug(traceback.print_exc())