unload ('select distinct D.userid , androidid , gcmid , timezone from device_master D , (select userid ,  count (*) from event where carouselid =
\'JustReleasedMusicVideos\' and eventid=\'371\' and action=\'start\' and utcdate between \'<start_date>\' and \'<end_date>\'
 group by userid   having  COUNT(*) > 1) E where D.userid = E.userid and D.epg_country=\'US\' ') to
's3://redshift_load/notification/activation_users/JustReleasedMusicVideos/'
 credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>' DELIMITER  ',' parallel off gzip;

unload ('select distinct D.userid , androidid , gcmid , timezone from device_master D , (select userid ,  count (*) from event where carouselid =
\'newmovietrailers\' and eventid=\'371\' and action=\'start\' and utcdate between \'<start_date>\' and \'<end_date>\'
 group by userid   having  COUNT(*) > 1) E where D.userid = E.userid and D.epg_country=\'US\' ') to
's3://redshift_load/notification/activation_users/newmovietrailers/'
 credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>' DELIMITER  ',' parallel off gzip;


unload ('select distinct D.userid , androidid , gcmid , timezone from device_master D , (select userid ,  count (*) from event where carouselid =
\'TheRichest\' and eventid=\'371\' and action=\'start\' and utcdate between \'<start_date>\' and \'<end_date>\'
 group by userid   having  COUNT(*) > 1) E where D.userid = E.userid and D.epg_country=\'US\' ') to
's3://redshift_load/notification/activation_users/TheRichest/'
 credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>' DELIMITER  ',' parallel off gzip;


unload ('select distinct D.userid , androidid , gcmid , timezone from device_master D , (select userid ,  count (*) from event where carouselid =
\'GagReel\' and eventid=\'371\' and action=\'start\' and utcdate between \'<start_date>\' and \'<end_date>\'
 group by userid   having  COUNT(*) > 1) E where D.userid = E.userid and D.epg_country=\'US\' ') to
's3://redshift_load/notification/activation_users/GagReel/'
 credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>' DELIMITER  ',' parallel off gzip;

unload ('select distinct D.userid , androidid , gcmid , timezone from device_master D , (select userid ,  count (*) from event where carouselid =
\'peeltv_US\' and eventid=\'371\' and action=\'start\' and utcdate between \'<start_date>\' and \'<end_date>\'
 group by userid   having  COUNT(*) > 1) E where D.userid = E.userid and D.epg_country=\'US\' ') to
's3://redshift_load/notification/activation_users/peeltv_US/'
 credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>' DELIMITER  ',' parallel off gzip;

