import sys, os, shutil
import concurrent.futures
from pyfcm import FCMNotification
from datetime import datetime, timedelta
import collections
from time import sleep


class fcmNotification():

    """
    Initializer method
    """
    def __init__(self, logger, properties):
        self._logger = logger
        self._properties = properties
        self._fcm = None
        self._infoLogger = None


    def initializePushProcess(self, job):

        campaignMethodId = job.get('campaignMethodId')
        payload = job.get('payload')
        timezone = job.get('timezone')
        '''isSilent = int(job.get('isSilent') or 0)
        isWifi = int(job.get('isWifi') or 0)
        displayAt = job.get('displayAt')
        expiresIn = int(job.get('expiresIn') or 0)
        batchSize = int(job.get('batchSize') or 0)
        batchInterval = int(job.get('batchInterval') or 0)'''

        self._fcm = FCMNotification(api_key=self._properties.get('gcmApiKey'))
        self._maxThreadCount = int(self._properties.get('maxThreadCount'))
        self._gcmMaxMessages = int(self._properties.get('gcmMaxMessages'))
        _outputPath = self._properties.get('infoLogFilePath')
        fileSize = int(self._properties.get('infoLogFileSize'))
        resultPath = _outputPath + self._properties.get('resultPath')
        dataPath = _outputPath + self._properties.get('dataPath') + str(campaignMethodId)

        jobId = '%s-%s'%(campaignMethodId, datetime.now().strftime('%Y%m%d'))

        self._infoLogger = self._logger.getInfoLogger(inFolder=resultPath, inFileName=str(campaignMethodId) + '.log', inFileSize=fileSize)

        self._logger.debug('campaignMethodId: %s'%campaignMethodId)
        self._logger.debug('jobId: %s'%jobId)
        '''self._logger.debug('isSilent: %s'%isSilent)
        self._logger.debug('isWifi: %s'%isWifi)
        self._logger.debug('timezone: %s'%timezone)
        self._logger.debug('displayAt: %s'%displayAt)
        self._logger.debug('expiresIn: %s'%expiresIn)
        self._logger.debug('batchSize: %s'%batchSize)
        self._logger.debug('batchInterval: %s'%batchInterval)'''

        for path,dir,files in os.walk(dataPath):
            for file in files:
                filepath = os.path.join(path,file)

                self._logger.debug('processing file : %s'%filepath)

                gcmDict = collections.OrderedDict()
                with open(filepath,'r') as fin:
                    for line in fin:
                        a, b, c, d = line.strip().split(',')
                        di = {}
                        di['userid'] = a
                        di['androidid'] = b
                        di['timezone'] = d
                        gcmDict[c] = di

                    self.startPushNotificationThreads(gcmDict, payload, campaignMethodId, jobId)

                os.unlink(filepath)

        shutil.rmtree(dataPath, ignore_errors=True)



    def startPushNotificationThreads(self, gcmDict, payload, campaignMethodId, jobId):
        threadId = 0
        newGCMDict = collections.defaultdict(dict)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._maxThreadCount) as executor:
            cnt = 0
            for key in gcmDict:
                cnt += 1
                newGCMDict[key] = gcmDict[key]
                if cnt % self._gcmMaxMessages == 0:
                    threadId += 1
                    self._logger.debug('Submitting thread %s for campaign method %s'%(threadId, campaignMethodId))

                    executor.submit(self.sendMessage, threadId, campaignMethodId, newGCMDict, payload, jobId)
                    newGCMDict = collections.defaultdict(dict)

                    if threadId % 100 == 0:
                        sleep(10)

        if len(newGCMDict) > 0:
            threadId += 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=self._maxThreadCount) as executor:
                executor.submit(self.sendMessage, threadId, campaignMethodId, newGCMDict, payload, jobId)


    def sendMessage(self, threadId, campaignMethodId, gcmDict, data, jobId):
        self._logger.debug('Sending message for thread %s.' % threadId)
        key = '%s' % (campaignMethodId)
        cid = int(campaignMethodId)
        jid = jobId

        reg_ids = [y for y in gcmDict]

        fcm_result = self._fcm.notify_multiple_devices(registration_ids=reg_ids, data_message=data,collapse_key=key, delay_while_idle=False, dry_run=False,
                                                       time_to_live=86400)

        self.parseFCMResponse(jid, cid, fcm_result,reg_ids, gcmDict)
        self._logger.debug('Sent message for thread %s.' % threadId)



    def parseFCMResponse(self, jobId, campaignMethodId, fcm_result,reg_ids, gcmDict):
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
                    self._infoLogger.info('%s,%s,%s,%s,%s,%s,%s,1,%s' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid,reg_id, result['message_id']))
                if 'registration_id' in results[i]:
                    self._infoLogger.info('%s,%s,%s,%s,%s,%s,%s,2,%s' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid, reg_id, result['registration_id']))
                if 'error' in results[i]:
                    if 'NotRegistered' == result['error']:
                        self._infoLogger.info('%s,%s,%s,%s,%s,%s,%s,3' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid, reg_id))
                    else :
                        self._infoLogger.info('%s,%s,%s,%s,%s,%s,%s,4,%s' % (thread_date, thread_now_date, jobId, campaignMethodId, userid, androidid, reg_id, result['error']))
                i+=1