CREATE TABLE TEST.walking_dead_accounts AS
SELECT account__number_aes256,
        SIZE(COLLECT_SET(episode)) AS number_of_episodes,
        SUM(total_view_time_in_s) AS total_view_time
FROM TEST_TMP.walking_dead_episodes
GROUP BY account__number_aes256;
