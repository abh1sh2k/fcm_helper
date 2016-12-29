import os, sys
import logging
import logging.handlers
from configManager import configManager
import random

class logHelper():

    """
    Initializer method
    """
    def __init__(self):
        self._debugLogger = None
        self._infoLogger = None
        self._properties = None

        self.getProperties()

    """
    get properties from configManager
    """
    def getProperties(self):
        config = configManager()
        self._properties = config._properties

    """
    get debug logger
    """
    def getDebugLogger(self,debugLogFileName=None):

        debugLogFilePath = self._properties.get('debugLogFilePath')
        if not debugLogFileName:
            debugLogFileName = self._properties.get('debugLogFileName')
        debugLogFileSize = int(self._properties.get('debugLogFileSize'))
        debugLogFileBackupCount = int(self._properties.get('debugLogFileBackupCount'))

        if not debugLogFilePath or not debugLogFileName:
            sys.exit(0)

        if not debugLogFileSize or debugLogFileSize == 0:
            debugLogFileSize = 10485760

        if not debugLogFileBackupCount or debugLogFileBackupCount == 0:
            debugLogFileBackupCount = 10

        if not os.path.exists(debugLogFilePath):
            os.makedirs(debugLogFilePath)

        debugLogFile =  debugLogFilePath + debugLogFileName
        l_logger = random.random()
        self._debugLogger = logging.getLogger('%s'%l_logger)
        self._debugLogger.setLevel(logging.DEBUG)
        fh = logging.handlers.RotatingFileHandler(debugLogFile, mode='a', maxBytes=debugLogFileSize,
            backupCount=debugLogFileBackupCount, encoding=None, delay=0)

        formatter = logging.Formatter('%(asctime)s  - %(filename)s - %(funcName)s:%(lineno)d %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        self._debugLogger.addHandler(fh)

        return self._debugLogger


    """
    get info logger
    """
    def getInfoLogger(self, inFolder=None, inFileName=None, inFileSize=None):

        if not inFolder:
            inFolder = self._properties.get('infoLogFilePath')

        if not inFileName:
            inFileName = self._properties.get('infoLogFileName')

        if not inFileSize:
            inFileSize = int(self._properties.get('infoLogFileSize'))

        if not inFolder or not inFileName:
            sys.exit(0)

        if not inFileSize:
            inFileSize = 10485760

        infoLogFileBackupCount = int(self._properties.get('infoLogFileBackupCount'))
        if not infoLogFileBackupCount or infoLogFileBackupCount == 0:
            infoLogFileBackupCount = 100

        if not os.path.exists(inFolder):
            os.makedirs(inFolder)

        infoLogFile =  inFolder + inFileName
        l_logger = random.random()
        self._infoLogger = logging.getLogger('%s'%l_logger)
        self._infoLogger.setLevel(logging.INFO)
        fh = logging.handlers.RotatingFileHandler(infoLogFile, mode='a', maxBytes=inFileSize,
            backupCount=infoLogFileBackupCount, encoding=None, delay=0)

        formatter = logging.Formatter('%(message)s')
        fh.setFormatter(formatter)
        self._infoLogger.addHandler(fh)

        return self._infoLogger

    """
    logger to write debug logs
    """
    def debug(self, message):
        if not self._debugLogger:
            self.getDebugLogger()

        self._debugLogger.debug(message)


    """
    logger to write info logs
    """
    def info(self, message):
        self._infoLogger.info(message)

