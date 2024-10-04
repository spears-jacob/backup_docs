set job.priority HIGH;
set output.compression.enabled true;
set mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
set pig.exec.mapPartAgg true; -- for optimizing the group by statements
set pig.cachedbag.memusage 0.35; -- increased the memory usage of the pig script by 15%. Previously it was 20% and now it is 35%

REGISTER 'hdfs:///udf/hadoop-libs-pig-1.0.4.jar';
DEFINE enumerate com.spectrum.pig.udfs.Enumerate('1');

raw_bhn_bill_pay = LOAD '$TMP_db.bhn_bill_pay_denorm' USING org.apache.hive.hcatalog.pig.HCatLoader();

map_custom_event = LOAD '$LKP_db.bhn_bill_pay_event' USING org.apache.hive.hcatalog.pig.HCatLoader();
map_custom_event = DISTINCT map_custom_event;

-- relation created for calculation ----------------

raw_bhn_bill_pay_short = FOREACH raw_bhn_bill_pay GENERATE
    unique_id as unique_id:chararray,
    (hit_source!=5?(hit_source!=7?(hit_source!=8?(hit_source!=9?CONCAT(CONCAT((chararray)post_visid_high,(chararray)post_visid_low),(chararray)visit_num):''):''):''):'') AS visit_id:chararray,
    (hit_source!=5?(hit_source!=7?(hit_source!=8?(hit_source!=9?CONCAT(CONCAT((chararray)post_visid_high,(chararray)post_visid_low),(chararray)(visit_num-1)):''):''):''):'') AS previous_visit_id:chararray,
    rank_id as rank_id:long,
    (long)post_cust_hit_time_gmt as date_time:long,
    post_page_event_var2 as post_page_event_var2:chararray,
    null as page_sub_section,
    (chararray)(CASE post_page_event
      WHEN 0 THEN post_pagename
      WHEN 100 THEN post_page_event_var2
      WHEN 101 THEN post_page_event_var1
      WHEN 102 THEN post_page_event_var1
      ELSE (chararray)post_page_event
     END) AS message_name:chararray, -- using for more exhaustive message_name generation
    post_event_list as post_event_list:chararray;

raw_bhn_bill_pay_short_dur = raw_bhn_bill_pay_short;

-- message.sequence_number calculation
visit_id_complete_group = GROUP raw_bhn_bill_pay_short BY visit_id;

visit_id_complete = FOREACH visit_id_complete_group
{
    sorted_visit = DISTINCT raw_bhn_bill_pay_short;
    sorted_visit = ORDER sorted_visit BY date_time ASC, rank_id ASC;
    enu_visit = enumerate(sorted_visit);
    GENERATE flatten(enu_visit);
};

raw_bhn_bill_pay_short = JOIN raw_bhn_bill_pay_short BY unique_id, visit_id_complete BY unique_id;

raw_bhn_bill_pay_calc = FOREACH raw_bhn_bill_pay_short GENERATE
    raw_bhn_bill_pay_short::unique_id AS unique_id_1:chararray,
    raw_bhn_bill_pay_short::visit_id AS visit_id_1:chararray,
    raw_bhn_bill_pay_short::previous_visit_id as previous_visit_id_1:chararray,
    raw_bhn_bill_pay_short::page_sub_section as page_sub_section_1:chararray,
    (int)i AS sequence_number_1:int,
    NULL AS visit_failed_attempts_1:int,  -- setting this to NULL temporarily
    NULL AS visit_login_duration_1:double,  -- setting this to NULL temporarily
    raw_bhn_bill_pay_short::message_name AS message_name_1:chararray;

raw_bhn_bill_pay_calc = DISTINCT raw_bhn_bill_pay_calc;

-- calculations for visit_start_timestamp
raw_bhn_bill_pay_short_g_f = FOREACH visit_id_complete_group
                {
                    GENERATE group AS visit_id_t:chararray,
                    MIN(raw_bhn_bill_pay_short.date_time) AS visit_start_timestamp_1 : long;
                 };

raw_bhn_bill_pay_calc = Join raw_bhn_bill_pay_calc BY visit_id_1 LEFT OUTER, raw_bhn_bill_pay_short_g_f BY visit_id_t;


-- Logic for login_duration start ----------------------------------------------------------------

raw_bhn_bill_pay_t1 = filter raw_bhn_bill_pay_short_dur by post_page_event_var2 == 'Sign In' and message_name matches '*success*';
raw_bhn_bill_pay_t2 = group raw_bhn_bill_pay_t1 by (visit_id, date_time, unique_id);
raw_bhn_bill_pay_t3 = JOIN raw_bhn_bill_pay_t2 BY group.$0, raw_bhn_bill_pay_t1 by visit_id;
raw_bhn_bill_pay_t3 = DISTINCT raw_bhn_bill_pay_t3;

raw_bhn_bill_pay_t4 = FOREACH
                raw_bhn_bill_pay_t3 GENERATE
                            raw_bhn_bill_pay_t2::group.visit_id AS visit_id_actual:chararray,
                            (long)raw_bhn_bill_pay_t2::group.date_time AS date_time_actual:long,
                            raw_bhn_bill_pay_t2::group.unique_id AS unique_id_actual:chararray,
                            flatten(raw_bhn_bill_pay_t1);


raw_bhn_bill_pay_t4 = DISTINCT raw_bhn_bill_pay_t4;
raw_bhn_bill_pay_t4 = FILTER raw_bhn_bill_pay_t4 BY date_time_actual > raw_bhn_bill_pay_t1::date_time;
raw_bhn_bill_pay_t5 = GROUP raw_bhn_bill_pay_t4 BY (visit_id_actual, date_time_actual, unique_id_actual);
raw_bhn_bill_pay_t6 = FOREACH raw_bhn_bill_pay_t5
{
    success_sort = ORDER raw_bhn_bill_pay_t4 BY raw_bhn_bill_pay_t1::date_time DESC;
    success_sort_prev = LIMIT success_sort 1;

    GENERATE group.visit_id_actual, group.date_time_actual, group.unique_id_actual, flatten(success_sort_prev);
}

raw_bhn_bill_pay_t6 = DISTINCT raw_bhn_bill_pay_t6;
raw_bhn_bill_pay_t1_success = DISTINCT raw_bhn_bill_pay_t1;
raw_bhn_bill_pay_t7 = JOIN raw_bhn_bill_pay_t1_success BY unique_id LEFT OUTER, raw_bhn_bill_pay_t6 BY unique_id_actual;
raw_bhn_bill_pay_fail = filter raw_bhn_bill_pay_short_dur by post_page_event_var2 == 'Sign In' and message_name matches '*failure*';

raw_bhn_bill_pay_fail = FOREACH raw_bhn_bill_pay_fail GENERATE
                visit_id as visit_id_fail:chararray,
                (long)date_time as date_time_fail:long;

raw_bhn_bill_pay_t8 = JOIN raw_bhn_bill_pay_t7 BY raw_bhn_bill_pay_t1_success::visit_id LEFT OUTER, raw_bhn_bill_pay_fail BY visit_id_fail;


raw_bhn_bill_pay_t9 = FILTER raw_bhn_bill_pay_t8 BY raw_bhn_bill_pay_t7::raw_bhn_bill_pay_t1_success::visit_id == raw_bhn_bill_pay_fail::visit_id_fail AND ((raw_bhn_bill_pay_t6::success_sort_prev::raw_bhn_bill_pay_t2::raw_bhn_bill_pay_t1::date_time IS NULL AND raw_bhn_bill_pay_t7::raw_bhn_bill_pay_t1_success::date_time >= raw_bhn_bill_pay_fail::date_time_fail) OR (raw_bhn_bill_pay_t6::success_sort_prev::raw_bhn_bill_pay_t2::raw_bhn_bill_pay_t1::date_time IS NOT NULL AND raw_bhn_bill_pay_t6::success_sort_prev::raw_bhn_bill_pay_t2::raw_bhn_bill_pay_t1::date_time <= raw_bhn_bill_pay_fail::date_time_fail AND raw_bhn_bill_pay_t7::raw_bhn_bill_pay_t1_success::date_time >= raw_bhn_bill_pay_fail::date_time_fail));

raw_bhn_bill_pay_t10 = group raw_bhn_bill_pay_t9 BY (raw_bhn_bill_pay_t7::raw_bhn_bill_pay_t1_success::visit_id, raw_bhn_bill_pay_t7::raw_bhn_bill_pay_t1_success::unique_id, raw_bhn_bill_pay_t7::raw_bhn_bill_pay_t1_success::date_time);

raw_login_duration_calc = FOREACH raw_bhn_bill_pay_t10 GENERATE
    group.visit_id as visit_id_f:chararray,
    group.unique_id as unique_id_f:chararray,
    (double)(group.date_time - MIN(raw_bhn_bill_pay_t9.date_time_fail)) as login_duration_sec:double;

-- Logic for login_duration end ----------------------------------------------------------------

raw_bhn_bill_pay_calc = JOIN raw_bhn_bill_pay_calc BY unique_id_1 LEFT OUTER, raw_login_duration_calc BY unique_id_f;


-- message__feature__name

raw_bhn_bill_pay_message_data = FOREACH raw_bhn_bill_pay_short_dur GENERATE
    unique_id AS unique_id:chararray,
    FLATTEN(TOKENIZE(post_event_list,',')) AS post_event_list_mod:chararray;

raw_bhn_bill_pay_message_data_join = JOIN raw_bhn_bill_pay_message_data BY (chararray)post_event_list_mod LEFT OUTER, map_custom_event BY (chararray)event_id USING 'replicated';
raw_bhn_bill_pay_message_data_join = FILTER raw_bhn_bill_pay_message_data_join BY map_custom_event::event_id IS NOT NULL;

raw_bhn_bill_pay_message_names_final = FOREACH raw_bhn_bill_pay_message_data_join GENERATE
    raw_bhn_bill_pay_message_data::unique_id AS unique_id:chararray,
    map_custom_event::detail AS message_feature_name:chararray;

raw_bhn_bill_pay_final_join = JOIN raw_bhn_bill_pay_calc BY raw_bhn_bill_pay_calc::unique_id_1 FULL OUTER, raw_bhn_bill_pay_message_names_final BY unique_id;
raw_bhn_bill_pay_final = FOREACH raw_bhn_bill_pay_final_join GENERATE
    (chararray)raw_bhn_bill_pay_calc::unique_id_1 AS unique_id:chararray,
    (chararray)raw_bhn_bill_pay_calc::visit_id_1 AS visit_id:chararray,
    (chararray)raw_bhn_bill_pay_calc::previous_visit_id_1 AS previous_visit_id:chararray,
    (chararray)raw_bhn_bill_pay_calc::page_sub_section_1 AS page_sub_section:chararray,
    (int)raw_bhn_bill_pay_calc::sequence_number_1 AS sequence_number:int,
    (int)raw_bhn_bill_pay_calc::visit_failed_attempts_1 AS visit_failed_attempts:int,  -- setting this to NULL temporarily
    (double)raw_login_duration_calc::login_duration_sec AS visit_login_duration:double,  -- setting this to NULL temporarily
    (long)raw_bhn_bill_pay_short_g_f::visit_start_timestamp_1 AS visit_start_timestamp:long,
    (chararray)raw_bhn_bill_pay_calc::message_name_1 AS message_name:chararray,
    (chararray)raw_bhn_bill_pay_message_names_final::message_feature_name AS message_feature_name:chararray;


STORE raw_bhn_bill_pay_final INTO '$TMP_db.bhn_bill_pay_calc' USING org.apache.hive.hcatalog.pig.HCatStorer();
