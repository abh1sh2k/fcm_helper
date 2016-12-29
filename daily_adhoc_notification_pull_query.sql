--unload ('select userid , androidid , gcmid , timezone  from analytics.recent_active_users where segmentid=<id>') to
--'s3://redshift_load/recent_active_users/segments/<date>/segment<id>.txt'
--credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>'
-- DELIMITER  ',' parallel off gzip;

  unload ('
      select   userid , androidid , gcmid , timezone from analytics.recent_active_users  where userid in (
        select distinct userid from (
           select A.userid from prod.campaign_ribbon_results A , event E where A.sent_date =\'<previous_day>\' and A.job_id=\'<clicked_jobid>\' and E.utcdate = \'<previous_day>\'
           and A.job_id = E.jobid and E.eventid = 753 and A.userid = E.userid
           UNION ALL
           select userid from prod.campaign_ribbon_results where job_id = \'<not_clicked_jobid>\' and sent_date =\'<previous_day>\' and userid not in
           ( select userid  from event where jobid=\'<not_clicked_jobid>\' and eventid = 753 and utcdate = \'<previous_day>\' ) )
   ') to 's3://redshift_load/recent_active_users/segments/<upload_date_dir>/segment<id>_'
   credentials 'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>' DELIMITER  ',' parallel off gzip ;