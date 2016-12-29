import sys
sys.path.append('/home/ec2-user/scripts')
from datetime import datetime , timedelta
import smtplib , logging
import traceback
from email.MIMEMultipart import MIMEMultipart
from email.mime.text import MIMEText
from redshiftHelper import redshiftHelper
import ConfigParser


## properties file
Config = ConfigParser.ConfigParser()
Config.read("/home/ec2-user/.env.ini")

REDSHIFT_LOAD_BUCKET = Config.get('s3','REDSHIFT_LOAD_BUCKET').strip()
access_key = Config.get('s3','ACCESS_KEY')
secret_key = Config.get('s3','SECRET_KEY')



sender = 'analytics@peel.com'
emailNotifyAddress = ['abhishek@peel.com']
emailNotifySubject = 'Activation User Process'

LOG_PATH = '/mnt/ebs/activation/'
logfilename = 'activation_user.log'
logging.basicConfig(level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)s: %(message)s', filename=LOG_PATH + logfilename, filemode='a')
logger = logging.getLogger('tunein')
sqlfilename = '/home/ec2-user/scripts/upload_new_activation.sql'

def email_with_message(to_recipients, subject, message):
	msg = MIMEMultipart()
	msg['From'] = sender
	msg['To'] = ", ".join(to_recipients)

	msg['Subject'] = subject
	msg.attach( MIMEText(message) )
	server = smtplib.SMTP('localhost')
	sendmail_recipients = to_recipients
	server.sendmail(sender, sendmail_recipients, msg.as_string())

def loaddata(filepath,  start_time , end_time , hour_dir):
    stmt = ""
    rds = redshiftHelper()
    rds.connect()

    with open(filepath) as FileObj:
        for line in FileObj:
            if not line.strip() or line.startswith('--'):
                continue

            stmt = stmt + ' ' + line
            if line.strip().endswith(';'):
                logger.debug(line)
                stmt = stmt.replace('<ACCESS_KEY>', access_key).replace('<SECRET_KEY>', secret_key).replace(
                    '<hour_dir>', hour_dir).replace('<start_time>',start_time).replace('<end_time>',end_time)
                print stmt
                #rds.executeStatement(stmt)
                stmt = ""

    rds.commit()
    rds.disconnect()

def main():
    now = datetime.now()
    start_time = (now - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M')+':00'
    end_time = (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')+':00'
    hour_dir = now.strftime('%Y/%m/%d/%H/')
    logger.debug('processing activation user for hour = %s.'%(hour_dir))
    loaddata(sqlfilename, start_time , end_time , hour_dir)

if __name__=='__main__':
    try:
        main()
    except Exception, e:
        logger.debug(traceback.print_exc())
        #email_with_message(emailNotifyAddress,'FAILED ' + emailNotifySubject, 'Wifi calculation failed. Check log file %s for more detail.'%(LOG_PATH+logfilename))


