import sys
sys.path.append('/home/ec2-user/scripts')

import os
import logging
import argparse
from datetime import datetime, timedelta
import email_helper
import boto_s3_helper
from redshiftHelper import redshiftHelper
import common_utility
import ConfigParser
import ast
import psycopg2
from itertools import groupby
from time import sleep
import traceback
import logging

## properties file
Config = ConfigParser.ConfigParser()
Config.read("/home/ec2-user/.env.ini")

## email properties
email_notify_addr = ['abhishek@peel.com']
email_notify_subject = 'notification adhoc'
## s3
REDSHIFT_LOAD_BUCKET = Config.get('s3','REDSHIFT_LOAD_BUCKET').strip()
access_key = Config.get('s3','ACCESS_KEY')
secret_key = Config.get('s3','SECRET_KEY')
query_filepath  = '/home/ec2-user/scripts/daily_adhoc_notification_pull_query.sql'
logfilename = '/home/ec2-user/netflix_hulu/daily_adhoc_notification_pull_query.log'
logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)s: %(message)s', filename=logfilename, filemode='w')
logger = logging.getLogger('notification_adhoc')


def writetologs(msg):
    print msg
    logger.info(msg)


def main( ):
    try:
        rds = redshiftHelper()
        rds.connect()
        rds.markAutoCommit()
        stmt = ''

        with open(query_filepath) as FileObj:
            for line in FileObj:
                if not line.strip() or line.startswith('--'):
                    continue
                stmt = stmt + ' ' + line
                
        writetologs( stmt)
        id = 0
        now_time = datetime.now()
        previous_day_time = now_time - timedelta(days=1)
        next_day_time = now_time + timedelta(days=1)
        current_hour = int (now_time.strftime('%H') )
        upload_date_dir = None
        previous_day = None
        if current_hour  <=3 :
            upload_date_dir = now_time.strftime('%Y/%m/%d')
            previous_day = previous_day_time.strftime('%Y-%m-%d')
            job_id_date = previous_day_time.strftime('%Y%m%d')
        else :
            upload_date_dir = next_day_time.strftime('%Y/%m/%d')
            previous_day = now_time.strftime('%Y-%m-%d')
            job_id_date = now_time.strftime('%Y%m%d')

        while id < 20 :
            clicked_jobid = str(10000 + id) + '-'+ str(id ) + '-' + job_id_date
            if id > 0 :
                not_clicked_id = id -1
            else:
                not_clicked_id = 19
            not_clicked_jobid = str(10000 + not_clicked_id ) + '-'+ str(not_clicked_id ) + '-' + job_id_date
            writetologs('upload_date_dir ' +upload_date_dir)
            stmt1 = stmt.replace('<ACCESS_KEY>', access_key).replace('<SECRET_KEY>', secret_key).replace('<id>',str(id)).replace('<upload_date_dir>',upload_date_dir)\
                .replace('<previous_day>',previous_day).replace('<clicked_jobid>', clicked_jobid).replace('<not_clicked_jobid>',not_clicked_jobid)
            writetologs('uploading to redshift stmt = '+stmt1)
            id +=1
            try:
                rds.executeStatement(stmt1)
            except Exception, e:
                writetologs(traceback.print_exc())
                email_helper.emailwithmessage(email_notify_addr, [], 'FAILED ' + email_notify_subject, 'Notification upload failed : '+str(traceback.print_exc()))
    except Exception, e:
        writetologs(traceback.print_exc())
        email_helper.emailwithmessage(email_notify_addr, [], 'FAILED ' + email_notify_subject, 'Notification upload failed : '+str(traceback.print_exc()))

if __name__ == "__main__":
    main()
