SELECT partition_date_time,
        session__equipment_mac_id_aes256,
        series,
        title__content_provider_name
FROM
(
SELECT partition_date_time,
        session__equipment_mac_id_aes256,
        title__content_provider_name,
    CASE WHEN lower(title__name) LIKE 'pawn stars%'
                THEN 'Pawn Stars'
        WHEN lower(title__name) LIKE 'walking dead%'
                THEN 'Walking Dead'
        WHEN lower(title__name) LIKE 'fear the walking dead%'
                THEN 'Fear the Walking Dead'
        WHEN lower(title__name) LIKE "it's always sunny in philadelphia%"
                THEN "It's Always Sunny in Philadelphia"
        WHEN lower(title__name) LIKE 'the people v. o.j. simpson: american crime story%'
                THEN 'The People v. O.J. Simpson: American Crime Story'
        WHEN lower(title__name) REGEXP 'westworld s.:.*'
                THEN 'Westworld'
        WHEN lower(title__name) LIKE 'big little lies.*'
                THEN 'Big Little Lies'
        WHEN lower(title__name) LIKE 'the big bang theory%'
                THEN 'The Big Bang Theory'
        WHEN lower(title__name) LIKE 'this is us%'
                THEN 'This Is Us'
        WHEN lower(title__name) LIKE 'pawn stars%'
                THEN 'Pawn Stars'
        WHEN lower(title__name) LIKE 'the bachelor %'
                THEN 'The Bachelor'
        WHEN lower(title__name) LIKE 'bad girls' AND title__content_provider_name = 'Oxygen'
                THEN 'Bad Girls'
        WHEN lower(title__name) LIKE 'shameless%'
                THEN 'Shameless'
        WHEN lower(title__name) LIKE 'gold rush%:%'
            THEN 'Gold Rush'
        WHEN lower(title__name) LIKE 'say yes%dress%'
            THEN 'Say Yes to the Dress'
        WHEN lower(title__name) LIKE 'american horror story%'
            THEN 'American Horror Story'
    ELSE 'other' END AS series
FROM prod.vod_concurrent_stream_session_event
WHERE asset__class_code = 'MOVIE'
AND session__is_error = FALSE
AND title__service_category_name != 'ADS'
AND partition_date_time BETWEEN '2016-01-01' AND '2017-05-01'
) d
WHERE series != 'other'
GROUP BY partition_date_time,
        session__equipment_mac_id_aes256,
        series,
        title__content_provider_name
