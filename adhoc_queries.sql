--- 20 th decemeber query sent
unload ('select DISTINCT D.userid , androidid , gcmid from device_master D , ( SELECT  userid FROM prod.event WHERE eventid = 751 AND
utcdate  >= \'2016-12-15\' AND jobid LIKE \'2133-%\' ) E where D.userid = E.userid ') to
's3://redshift_load/notification/activation_users/adhoc/2016/12/19/06/'
credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>'
DELIMITER  ',' parallel off gzip;

-- 28 th december

unload ('select userid , androidid , gcmid , segmentid from analytics.recent_active_users') to
's3://redshift_load/recent_active_users/segments/'
 credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>'
 DELIMITER  ',' parallel off gzip;
