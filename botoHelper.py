import boto
import os, gzip
from configManager import configManager
from time import sleep
from datetime import datetime

class botoHelper():

    """
    Initializer method
    """
    def __init__(self, properties):
        self._properties = properties
        self.bucket = self._properties.get('s3bucket')


    def uploadFileToS3(self, key, filename):
        s3 = boto.connect_s3()
        bucketId = s3.get_bucket(self.bucket)

        k = bucketId.get_key(key)
        if k:
            bucketId.delete_key(k)

        k = bucketId.new_key(key)
        k.set_contents_from_filename(filename, headers=None, replace=False, cb=None)

    def downloadFilesFromS3(self, prefix, dir , local_file_name_prefix):
        s3 = boto.connect_s3()
        bucketId = s3.get_bucket(self.bucket)

        s3_keys = bucketId.list(prefix)
        print prefix, dir , local_file_name_prefix
        for s3_key in s3_keys :
            local_file = dir + local_file_name_prefix + s3_key.name.encode('utf-8').rsplit('/',1)[-1]
            print local_file
            s3_key.get_contents_to_filename(local_file, headers=None)

    def downloadFileFromS3(self, key,  local_file_name):
        s3 = boto.connect_s3()
        bucketId = s3.get_bucket(self.bucket)
        s3_key = bucketId.get_key(key)
        s3_key.get_contents_to_filename(local_file_name, headers=None)

    def upload_result_files(self , local_dir , s3_base_folder  , _debugLogger):

        s3 = boto.connect_s3()
        bucketId = s3.get_bucket(self.bucket)

        for path, dir, files in os.walk(local_dir):
            for file in files:
                filepath = os.path.join(path, file)
                if '.gz' not in filepath:
                    f_in = open(filepath, 'rb')
                    f_out = gzip.open(filepath + '.gz', 'wb')
                    f_out.writelines(f_in)
                    f_out.close()
                    f_in.close()
                    os.unlink(filepath)
                    filepath = filepath + '.gz'
                    file = file+'.gz'
                s3_file = s3_base_folder + file
                _debugLogger.debug('uploading  file : %s to s3 folder %s' % (filepath,s3_file))
                key = bucketId.get_key(s3_file)
                if key:
                    bucketId.delete_key(key)
                key = bucketId.new_key(s3_file)
                key.set_contents_from_filename(filepath, headers=None, replace=False, cb=None)
                os.unlink(filepath)

if __name__=='__main__':
    config = configManager()
    _properties = config._properties
    _botoHelper = botoHelper(_properties)
    s3_dir = datetime.now().strftime('%Y/%m/%d/%H/')
