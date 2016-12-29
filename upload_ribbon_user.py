import sys
sys.path.append('/home/ec2-user/scripts')
from datetime import datetime , timedelta
import traceback
from redshiftHelper import redshiftHelper
import ConfigParser


## properties file
Config = ConfigParser.ConfigParser()
Config.read("/home/ec2-user/.env.ini")

REDSHIFT_LOAD_BUCKET = Config.get('s3','REDSHIFT_LOAD_BUCKET').strip()
access_key = Config.get('s3','ACCESS_KEY')
secret_key = Config.get('s3','SECRET_KEY')

sqlfilename = '/home/ec2-user/scripts/upload_ribbon_user.sql'


def loaddata(filepath,  start_date , end_date ):
    stmt = ""
    rds = redshiftHelper()
    rds.connect()

    with open(filepath) as FileObj:
        for line in FileObj:
            if not line.strip() or line.startswith('--'):
                continue

            stmt = stmt + ' ' + line
            if line.strip().endswith(';'):
                print line
                stmt = stmt.replace('<ACCESS_KEY>', access_key).replace('<SECRET_KEY>', secret_key).\
                    replace('<start_date>',start_date).replace('<end_date>',end_date)
                print stmt
                rds.executeStatement(stmt)
                stmt = ""

    rds.commit()
    rds.disconnect()

def main():
    now = datetime.now()
    start_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
    end_date =   (now - timedelta(days=1)).strftime('%Y-%m-%d')
    loaddata(sqlfilename, start_date , end_date )

if __name__=='__main__':
    try:
        main()
    except Exception, e:
        traceback.print_exc()
