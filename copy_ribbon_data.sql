copy prod.campaign_ribbon_results (sent_date, sent_time, job_id, campaign_method_id, userid, androidid, gcmid, status, message)
from 's3://redshift_load/recent_active_users/results/2017/01/07/'
CREDENTIALS 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>'
delimiter ',' escape maxerror 1000 IGNOREBLANKLINES TRUNCATECOLUMNS FILLRECORD DATEFORMAT 'auto'
TRIMBLANKS EMPTYASNULL BLANKSASNULL ACCEPTANYDATE NULL AS 'NULL' compupdate off ROUNDEC gzip ;


copy prod.campaign_master
from 's3://redshift_load/recent_active_users/campaign_details/campaign_details_s3.txt'
CREDENTIALS 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>'
format as json 'auto';