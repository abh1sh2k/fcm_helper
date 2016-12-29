from botoHelper import botoHelper
from datetime import datetime
import gzip , os
from configManager import  configManager
from logHelper import logHelper
import collections
import concurrent.futures
from time import sleep
from pyfcm import FCMNotification

config = configManager()
_properties = config._properties
_maxThreadCount = int(_properties.get('maxThreadCount'))
_gcmMaxMessages = int(_properties.get('gcmMaxMessages'))
_gcmMaxMessages = 500
_logger = logHelper()


s3_key_suffix = datetime.now().strftime('%Y/%m/%d/%H/')
local_file_name_prefix = datetime.now().strftime('%Y_%m_%d_%H')
in_File_Name = str(100000000)+'_'+local_file_name_prefix


_fcm = FCMNotification(api_key=_properties.get('gcmApiKey'))


fileSize = int(_properties.get('infoLogFileSize'))

resultPath = '/mnt/ebs/fcm/newly_active_result/'
_infoLogger = _logger.getInfoLogger(inFolder=resultPath, inFileName=in_File_Name + '.log',inFileSize=fileSize)
_debugLogger =  _logger.getDebugLogger(debugLogFileName='activation.log')

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

def sendNotification( dir , payload , campaignMethodId):
    jobId = '%s-%s' % (campaignMethodId, datetime.now().strftime('%Y%m%d'))
    payload['jobid'] = jobId
    print "finally payload ", payload
    gcmDict = collections.OrderedDict()
    for path, dir, files in os.walk(dir):
        for file in files:
            filepath = os.path.join(path, file)
            _debugLogger.debug('processing file : %s' % filepath)
            with open(filepath, 'r') as fin:
                for line in fin:
                    a, b, c = line.strip().split(',')
                    di = {}
                    di['userid'] = a
                    di['androidid'] = b
                    gcmDict[c] = di

                startPushNotificationThreads(gcmDict, payload , campaignMethodId, jobId)
            #_debugLogger.debug('deleting file : %s' % filepath)
            #os.unlink(filepath)


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
                sleep(2)
                _debugLogger.debug('Sleeping thread %s for campaign method %s' % (threadId, campaignMethodId))
                if threadId % 50 == 0:
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
    _debugLogger.debug('Sent message for thread %s.' % threadId)


def parseFCMResponse( jobId, campaignMethodId, fcm_result, reg_ids, gcmDict):
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
    local_dir = '/mnt/ebs/fcm/newly_active_data/'
    _botoHelper = botoHelper(_properties)
    _botoHelper.downloadFilesFromS3('notification/activation_users/adhoc/2016/12/19/06/' , local_dir  , local_file_name_prefix )
    uncompressGzipFilesinDir(local_dir)
    payload = {"title": "Catch up with Late Night", "url": "peel://contents/?type=streaming&id=LateNIght_CatchUp&title=Late Night&display_type=current&autoplay=true", "strategic": "true", "priority": "high", "message": "Watch the best of Conan, Kimmel, and more", "type": "URL","source":"fcm", "logo_image": "https://d3s0fgls9ltf2x.cloudfront.net/Debate/Video-Production-Icon.png"}
    campaign_method_id = 100000000

    sendNotification( local_dir  , payload , campaign_method_id)

    sleep(30)
    #uploading result files
    _debugLogger.debug('uploading ' + resultPath + in_File_Name + '.log to notification/activation_users/newly_activated_users_result/' + s3_key_suffix + in_File_Name + '.log')
    _botoHelper.uploadFileToS3('notification/activation_users/newly_activated_users_result/' + s3_key_suffix + in_File_Name + '.log',
        resultPath + in_File_Name + '.log')