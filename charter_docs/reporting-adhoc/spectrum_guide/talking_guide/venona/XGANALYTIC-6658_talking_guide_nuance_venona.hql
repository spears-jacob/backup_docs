set mapreduce.input.fileinputformat.split.minsize=5368709120;

set mapreduce.input.fileinputformat.split.maxsize=5368709120;

SELECT  SIZE(COLLECT_SET(CASE WHEN day_count > 4 THEN account__number_aes256 ELSE NULL END)) AS hh_count
FROM
    (SELECT account__number_aes256,
            SIZE(COLLECT_SET(partition_date)) AS day_count
    FROM
        (SELECT account__number_aes256,
               partition_date
        FROM
            (SELECT visit__device__uuid,
                    prod.epoch_converter(received__timestamp, 'America/Denver') as partition_date
            FROM prod.venona_events
            WHERE partition_date_utc BETWEEN '2017-07-01' AND '2017-10-01'
            AND visit__talking_guide = true
            AND visit__application_details__application_name = 'Spectrum Guide'
            GROUP BY visit__device__uuid,
                     prod.epoch_converter(received__timestamp, 'America/Denver')) events
            JOIN
                (SELECT run_date,
                        account__mac_id_aes256,
                        account__number_aes256,
                        system__kma_desc,
                        account__category,
                        equipment__model,
                        sg_deployed_type
                  FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
                  WHERE  run_date BETWEEN '2017-07-01' AND '2017-09-31') deployed
            ON events.visit__device__uuid = prod.aes_encrypt(prod.aes_decrypt256(deployed.account__mac_id_aes256))
        AND events.partition_date = deployed.run_date) q
    GROUP BY account__number_aes256) d
