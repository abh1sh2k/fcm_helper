unload ('select userid , androidid , gcmid from bg_daily where created_datetime
 between \'<start_time>\' and \'<end_time>\' ') to
's3://redshift_load/notification/activation_users/<hour_dir>'
 credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>' DELIMITER  ',' parallel off gzip;