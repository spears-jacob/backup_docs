USE ${env:ENVIRONMENT};

SET CUTOVER_DATE='2018-02-07';
sET SOURCE_TABLE=asp_v_net_events;

SELECT "\n\nThe CUTOVER_DATE variable is used to note when the new equipment flow was implemented,\n which is now set to:\n  ${hiveconf:CUTOVER_DATE}\n\n";

SELECT "\n\nThe SOURCE_TABLE variable is used to note the source event view/table,\n which is now set to:\n  ${hiveconf:SOURCE_TABLE}\n\n";

SELECT "\n\nNow Preparing a dictionary based on the following dates:\n start: ${env:START_DATE}\n   end: ${env:END_DATE} \n\n\n\n";

DROP TABLE IF EXISTS dictionary;
CREATE TEMPORARY TABLE dictionary as
SELECT message__category as message_category,
       message__name AS message_name,
       message__timestamp as message_timestamp,
       state__view__current_page__name as page_name,
       operation__type as operation_type,
       visit__visit_id AS visit_id,
       partition_date as date_denver
from ${hiveconf:SOURCE_TABLE}
WHERE ( partition_date >= '${env:START_DATE}'
    AND partition_date <  '${env:END_DATE}'  )
AND (LOWER(message__name)
     in ( 'my-internet-services',
          'reset-equipment-internet',
          'progress-bar-modal-internet',
          'reset-equipment-internet-failure',
          'reset-equipment-internet-success',
          'my-internet-services.manually-reset-equipment',
          'my-internet-services.contact-us',
          'equipment.internet.troubleshoot',
          'equip.internet.troubleshoot.reset-equip.start',
          'equip.internet.troubleshoot.reset-equip.cancel',
          'equip.internet.troubleshoot.reset-equip.reset-failure-modal-continue',
          'equip.internet.troubleshoot.reset-equip.success-modal-continue',
          'equip.internet.troubleshoot.reset-equip.success-modal-issue-resolved',
          'my-voice-services',
          'reset-equipment-voice',
          'progress-bar-modal-voice',
          'reset-equipment-voice-failure',
          'reset-equipment-voice-success',
          'my-voice-services.manually-reset-equipment',
          'my-voice-services.contact-us',
          'equipment.voice.troubleshoot',
          'equip.voice.troubleshoot.reset-equip.start',
          'equip.voice.troubleshoot.reset-equip.cancel',
          'equip.voice.troubleshoot.reset-equip.reset-failure-modal-continue',
          'equip.voice.troubleshoot.reset-equip.success-modal-continue',
          'equip.voice.troubleshoot.reset-equip.success-modal-issue-resolved',
          'my-voice-services',
          'reset-equipment-voice',
          'progress-bar-modal-voice',
          'reset-equipment-voice-failure',
          'reset-equipment-voice-success',
          'my-voice-services.manually-reset-equipment',
          'my-voice-services.contact-us',
          'equipment.voice.troubleshoot',
          'equip.voice.troubleshoot.reset-equip.start',
          'equip.voice.troubleshoot.reset-equip.cancel',
          'equip.voice.troubleshoot.reset-equip.reset-failure-modal-continue',
          'equip.voice.troubleshoot.reset-equip.success-modal-continue',
          'equip.voice.troubleshoot.reset-equip.success-modal-issue-resolved',
          'my-tv-services',
          'reset-equipment-tv',
          'progress-bar-modal-tv',
          'reset-equipment-tv-failure',
          'reset-equipment-tv-success',
          'my-tv-services.manually-reset-equipment',
          'my-tv-services.contact-us',
          'equipment.tv.troubleshoot',
          'equip.tv.troubleshoot.reset-equip.start',
          'equip.tv.troubleshoot.reset-equip.cancel',
          'equip.tv.troubleshoot.reset-equip.reset-failure-modal-continue',
          'equip.tv.troubleshoot.reset-equip.success-modal-continue',
          'equip.tv.troubleshoot.reset-equip.success-modal-issue-resolved',
          'refresh',
          'refresh-status')
  OR LOWER(state__view__current_page__name)
       IN('reset-equipment-wifi',
          'reset-modem-internetvoice',
          'reset-modem-internet',
          'test-connection-wifi',
          'test-connection-internetvoice',
          'test-connection-internet',
          'unable-reset-error-modal-internet',
          'unable-reset-error-modal-internetvoice',
          'unable-reset-router-error-wifi',
          'unable-reset-modem-error-wifi',
          'reset-modem-voice',
          'test-connection-voice',
          'unable-reset-error-modal-voice',
          'unable-reset-error-modal-internetvoice')
  OR LOWER(operation__type) = 'success'
);

--------------------------------- INTERNET ------------------------------------------
-------------------------------------------------------------------------------------

SELECT "\n\nNow inserting internet with success and failure duplicates.\n\n";
-----------------> Insert OVERWRITE for after cutoff <-----------------
INSERT OVERWRITE TABLE asp_equipment_flow PARTITION(date_denver,source_table)
select  message_category,
        message_name,
        'Internet_with_dups' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver >= ${hiveconf:CUTOVER_DATE} )
AND message_name in ('my-internet-services',
                      'Reset-Equipment-Internet',
                      'Progress-Bar-Modal-Internet',
                      'Reset-Equipment-Internet-Failure',
                      'Reset-Equipment-Internet-Success',
                      'my-internet-services.manually-reset-equipment',
                      'my-internet-services.contact-us',
                      'equipment.internet.troubleshoot',
                      'equip.internet.troubleshoot.reset-equip.start',
                      'equip.internet.troubleshoot.reset-equip.cancel',
                      'equip.internet.troubleshoot.reset-equip.reset-failure-modal-continue',
                      'equip.internet.troubleshoot.reset-equip.success-modal-continue',
                      'equip.internet.troubleshoot.reset-equip.success-modal-issue-resolved'
                    )
group by  message_category,
          message_name,
          date_denver;


--------------------------------------- insert internet de duplicated failures and successes

SELECT "\n\nNow inserting internet de duplicated failures and successes.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)

select  b.message_category,
        b.message_name,
        'Internet' AS domain,
        count(*) as instances,
        count (distinct b.visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        b.date_denver
FROM (select  a.date_denver,
              a.message_timestamp,
              a.message_category,
              a.visit_id,
              a.message_name,
              a.next_page,
              case when a.message_name = 'Reset-Equipment-Internet-Failure'
                    AND a.next_page = 'Reset-Equipment-Internet-Failure' then 'double_fail'
                   when a.message_name = 'Reset-Equipment-Internet-Success'
                    AND a.next_page = 'Reset-Equipment-Internet-Success' then 'double_success'
                   else a.message_name
              END AS test
      from (select date_denver,
                   message_timestamp,
                   message_category,
                   visit_id,
                   message_name,
                   lead(message_name)
                    OVER (PARTITION BY visit_id
                          ORDER BY message_timestamp)
                    AS next_page
            from dictionary
            WHERE ( date_denver >= ${hiveconf:CUTOVER_DATE} )
            AND message_name in ('Reset-Equipment-Internet-Failure',
                                  'equip.internet.troubleshoot.reset-equip.start',
                                  'Reset-Equipment-Internet-Success')
            group by  date_denver,
                      message_timestamp,
                      message_category,
                      visit_id,
                      message_name
            order by visit_id,
                     message_timestamp
            ) a
     ) b
where  b.test <> 'double_fail'
   AND b.test <> 'double_success'
GROUP BY  b.message_category,
          b.message_name,
          b.date_denver;

--------------------------------------- insert internet data WITHOUT starts, successes, and failures
SELECT "\n\nNow inserting internet data WITHOUT starts, successes, and failures.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)

select  message_category,
        message_name,
        'Internet' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver >= ${hiveconf:CUTOVER_DATE} )
AND message_name in ('my-internet-services',
                      'Reset-Equipment-Internet',
                      'Progress-Bar-Modal-Internet',
                      --'Reset-Equipment-Internet-Failure',
                      --'Reset-Equipment-Internet-Success',
                      'my-internet-services.manually-reset-equipment',
                      'my-internet-services.contact-us',
                      'equipment.internet.troubleshoot',
                      --'equip.internet.troubleshoot.reset-equip.start',
                      'equip.internet.troubleshoot.reset-equip.cancel',
                      'equip.internet.troubleshoot.reset-equip.reset-failure-modal-continue',
                      'equip.internet.troubleshoot.reset-equip.success-modal-continue',
                      'equip.internet.troubleshoot.reset-equip.success-modal-issue-resolved' )
group by message_category,
         message_name,
         date_denver;


--------------------------------------- insert INTERNET Pre Launch Start
-----------------> Insert OVERWRITE for before cutoff <-----------------

SELECT "\n\nNow inserting INTERNET Pre Launch Start.\n\n";

INSERT OVERWRITE TABLE asp_equipment_flow PARTITION(date_denver,source_table)

Select  message_category,
        'Reset_Internet_start_pre' AS message_name,
        'Internet' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
AND message_category = 'Page View'
AND LOWER(page_name)
        IN ('reset-equipment-wifi',
            'reset-modem-internetvoice',
            'reset-modem-internet')
GROUP BY  message_category,
          date_denver;

--------------------------------------- insert INTERNET Pre Launch Start
SELECT "\n\nNow inserting INTERNET Pre Launch Start.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
Select  message_category,
        'Reset_Internet_success_pre' AS message_name,
        'Internet' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
AND message_category = 'Page View'
AND LOWER(page_name)
        IN ('test-connection-wifi',
            'test-connection-internetvoice',
            'test-connection-internet')
GROUP BY  message_category,
          date_denver;

--------------------------------------- insert Internet Pre Launch failure
SELECT "\n\nNow inserting INTERNET Pre Launch failure.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
Select  message_category,
        'Reset_Internet_fail_pre' AS message_name,
        'Internet' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
AND message_category = 'Page View'
AND LOWER(page_name)
        IN ('unable-reset-error-modal-internet',
            'unable-reset-error-modal-internetvoice',
            'unable-reset-router-error-wifi',
            'unable-reset-modem-error-wifi')
GROUP BY  message_category,
          date_denver;

------------------------------------ VOICE ------------------------------------------
-------------------------------------------------------------------------------------

--------------------------------------- insert VOICE with duplicated successes and failures
SELECT "\n\nNow inserting VOICE with success and failure duplicates.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
select  message_category,
        message_name,
        'Voice_with_dups' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver >= ${hiveconf:CUTOVER_DATE} )
  AND message_name in ('my-voice-services',
                        'Reset-Equipment-Voice',
                        'Progress-Bar-Modal-Voice',
                        'Reset-Equipment-Voice-Failure',
                        'Reset-Equipment-Voice-Success',
                        'my-voice-services.manually-reset-equipment',
                        'my-voice-services.contact-us',
                        'equipment.voice.troubleshoot',
                        'equip.voice.troubleshoot.reset-equip.start',
                        'equip.voice.troubleshoot.reset-equip.cancel',
                        'equip.voice.troubleshoot.reset-equip.reset-failure-modal-continue',
                        'equip.voice.troubleshoot.reset-equip.success-modal-continue',
                        'equip.voice.troubleshoot.reset-equip.success-modal-issue-resolved'
                       )
group by  message_category,
          message_name,
          date_denver;

--------------------------------------- insert VOICE de duplicated failures and successes
SELECT "\n\nNow inserting VOICE de duplicated failures and successes.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
select  b.message_category,
        b.message_name,
        'Voice' AS domain,
        count(*) as instances,
        count (distinct b.visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        b.date_denver
FROM (select  a.date_denver,
              a.message_timestamp,
              a.message_category,
              a.visit_id,
              a.message_name,
              a.next_page,
              case  when a.message_name = 'Reset-Equipment-Voice-Failure'
                     AND a.next_page = 'Reset-Equipment-Voice-Failure'
                     then 'double_fail'
                    when a.message_name = 'Reset-Equipment-Voice-Success'
                     AND a.next_page = 'Reset-Equipment-Voice-Success'
                     then 'double_success'
                    else a.message_name
              END AS test
      from (select  date_denver,
                    message_timestamp,
                    message_category,
                    visit_id,
                    message_name,
                    lead(message_name)
                      OVER (PARTITION BY visit_id
                            ORDER BY message_timestamp)
                      AS next_page
            from dictionary
            WHERE ( date_denver >= ${hiveconf:CUTOVER_DATE} )
            AND message_name in ('Reset-Equipment-Voice-Failure',
                                  'equip.voice.troubleshoot.reset-equip.start',
                                  'Reset-Equipment-Voice-Success')
            group by  date_denver,
                      message_timestamp,
                      message_category,
                      visit_id,
                      message_name
            order by  visit_id,
                      message_timestamp
           ) a
     ) b
where b.test <> 'double_fail'
  AND b.test <> 'double_success'
GROUP BY  b.message_category,
          b.message_name,
          b.date_denver;

--------------------------------------- insert voice WITHOUT starts, successes, failures
SELECT "\n\nNow inserting VOICE WITHOUT starts, successes, failures.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)

select  message_category,
        message_name,
        'Voice' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver >= ${hiveconf:CUTOVER_DATE} )
  AND message_name in ('my-voice-services',
                        'Reset-Equipment-Voice',
                        'Progress-Bar-Modal-Voice',
                        --'Reset-Equipment-Voice-Failure',
                        --'Reset-Equipment-Voice-Success',
                        'my-voice-services.manually-reset-equipment',
                        'my-voice-services.contact-us',
                        'equipment.voice.troubleshoot',
                        --'equip.voice.troubleshoot.reset-equip.start',
                        'equip.voice.troubleshoot.reset-equip.cancel',
                        'equip.voice.troubleshoot.reset-equip.reset-failure-modal-continue',
                        'equip.voice.troubleshoot.reset-equip.success-modal-continue',
                        'equip.voice.troubleshoot.reset-equip.success-modal-issue-resolved'
                       )
group by  message_category,
          message_name,
          date_denver;

--------------------------------------- insert VOICE Pre Launch START
SELECT "\n\nNow inserting VOICE Pre Launch START.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
Select  message_category,
        'Reset_Voice_start_pre' AS message_name,
        'Voice' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
AND message_category = 'Page View'
AND LOWER(page_name)
        IN ('reset-modem-voice',
            'reset-modem-internetvoice')
GROUP BY  message_category,
          date_denver;


--------------------------------------- insert VOICE Pre Launch success
SELECT "\n\nNow inserting VOICE Pre Launch success.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
Select  message_category,
        'Reset_Voice_success_pre' AS message_name,
        'Voice' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
AND message_category = 'Page View'
AND LOWER(page_name)
        IN ('test-connection-voice',
            'test-connection-internetvoice')
GROUP BY  message_category,
          date_denver;

--------------------------------------- insert VOICE Pre Launch failure
SELECT "\n\nNow inserting VOICE Pre Launch failure.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
Select  message_category,
        'Reset_Voice_fail_pre' AS message_name,
        'Voice' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
AND message_category = 'Page View'
AND LOWER(page_name)
        IN ('unable-reset-error-modal-voice',
            'unable-reset-error-modal-internetvoice')
GROUP BY  message_category,
          date_denver;


--------------------------------------- TV ------------------------------------------
-------------------------------------------------------------------------------------
--------------------------------------- insert TV
SELECT "\n\nNow inserting TV.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
select  message_category,
        message_name,
        'TV' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver >= ${hiveconf:CUTOVER_DATE} )
  AND lower(message_name)
          in ('my-tv-services',
              'reset-equipment-tv',
              'progress-bar-modal-tv',
              'reset-equipment-tv-failure',
              'reset-equipment-tv-success',
              'my-tv-services.manually-reset-equipment',
              'my-tv-services.contact-us',
              'equipment.tv.troubleshoot',
              'equip.tv.troubleshoot.reset-equip.start',
              'equip.tv.troubleshoot.reset-equip.cancel',
              'equip.tv.troubleshoot.reset-equip.reset-failure-modal-continue',
              'equip.tv.troubleshoot.reset-equip.success-modal-continue',
              'equip.tv.troubleshoot.reset-equip.success-modal-issue-resolved' )
group by  date_denver,
          message_category,
          message_name;

--------------------------------------- insert TV Pre Launch Start
SELECT "\n\nNow inserting TV Pre Launch Start.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
select  message_category,
        'Reset_TV_start_pre' AS message_name,
        'TV' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
  AND message_category <> 'Page View'
  AND LOWER(message_name) = 'refresh'
GROUP BY  message_category,
          date_denver;


--------------------------------------- insert TV Pre Launch successes
SELECT "\n\nNow inserting TV Pre Launch successes.\n\n";

INSERT INTO TABLE asp_equipment_flow PARTITION(date_denver,source_table)
select  message_category,
        'Reset_TV_success_pre' AS message_name,
        'TV' AS domain,
        count(*) as instances,
        count (distinct visit_id) as visits,
        '${hiveconf:SOURCE_TABLE}' AS source_table,
        date_denver
from dictionary
WHERE ( date_denver < ${hiveconf:CUTOVER_DATE} )
  AND message_category = 'Custom Link'
  AND LOWER(message_name) = 'refresh-status'
  AND LOWER(operation_type) = 'success'
GROUP BY  message_category,
          date_denver;
