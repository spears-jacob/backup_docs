DROP TABLE TEST.amc_watch_times;
CREATE TABLE TEST.amc_watch_times AS
SELECT account__number_aes256,
    SUM(total_amc_viewing_in_s) AS total_amc_viewing_in_s,
    SUM(total_walking_dead_viewing_in_s) AS total_walking_dead_viewing_in_s
FROM TEST_TMP.amc_watch_times_monthly
GROUP BY account__number_aes256;
