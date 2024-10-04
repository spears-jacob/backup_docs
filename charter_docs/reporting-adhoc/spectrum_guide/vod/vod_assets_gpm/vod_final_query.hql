CREATE TABLE TEST.vod_view_times AS

SELECT account__number_aes256,
        SUM(total_amc_viewing_in_s) AS total_viewing_in_s,
        SUM(total_walking_dead_viewing_in_s) AS total_walking_dead_viewing_in_s
 FROM test_tmp.vod_watch_times_monthly
 GROUP BY account__number_aes256
