set job.priority HIGH;
set output.compression.enabled true;
set mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
set pig.exec.mapPartAgg true; -- for optimizing the group by statements
set pig.cachedbag.memusage 0.35; -- increased the memory usage of the pig script by 15%. Previously it was 20% and now it is 35%

REGISTER 'hdfs:///udf/hadoop-libs-pig-1.0.4.jar';
DEFINE enumerate com.spectrum.pig.udfs.Enumerate('1');

raw_data = LOAD '$TMP_db.$dbprefix_denorm' USING org.apache.hive.hcatalog.pig.HCatLoader();

map_custom_event = LOAD '$LKP_db.$dbprefix_event_lkp' USING org.apache.hive.hcatalog.pig.HCatLoader();
map_custom_event = DISTINCT map_custom_event;

-- relation created for calculation ----------------

raw_data_short = FOREACH raw_data GENERATE
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

raw_data_short_dur = raw_data_short;

-- message.sequence_number calculation
visit_id_complete_group = GROUP raw_data_short BY visit_id;

visit_id_complete = FOREACH visit_id_complete_group
{
    sorted_visit = DISTINCT raw_data_short;
    sorted_visit = ORDER sorted_visit BY date_time ASC, rank_id ASC;
    enu_visit = enumerate(sorted_visit);
    GENERATE flatten(enu_visit);
};

raw_data_short = JOIN raw_data_short BY unique_id, visit_id_complete BY unique_id;

raw_data_calc = FOREACH raw_data_short GENERATE
    raw_data_short::unique_id AS unique_id_1:chararray,
    raw_data_short::visit_id AS visit_id_1:chararray,
    raw_data_short::previous_visit_id as previous_visit_id_1:chararray,
    raw_data_short::page_sub_section as page_sub_section_1:chararray,
    (int)i AS sequence_number_1:int,
    NULL AS visit_failed_attempts_1:int,  -- setting this to NULL temporarily
    NULL AS visit_login_duration_1:double,  -- setting this to NULL temporarily
    raw_data_short::message_name AS message_name_1:chararray;

raw_data_calc = DISTINCT raw_data_calc;

-- calculations for visit_start_timestamp
raw_data_short_g_f = FOREACH visit_id_complete_group
                {
                    GENERATE group AS visit_id_t:chararray,
                    MIN(raw_data_short.date_time) AS visit_start_timestamp_1 : long;
                 };

raw_data_calc = Join raw_data_calc BY visit_id_1 LEFT OUTER, raw_data_short_g_f BY visit_id_t;


-- Logic for login_duration start ----------------------------------------------------------------

raw_data_t1 = filter raw_data_short_dur by post_page_event_var2 == 'Sign In' and message_name matches '*success*';
raw_data_t2 = group raw_data_t1 by (visit_id, date_time, unique_id);
raw_data_t3 = JOIN raw_data_t2 BY group.$0, raw_data_t1 by visit_id;
raw_data_t3 = DISTINCT raw_data_t3;

raw_data_t4 = FOREACH
                raw_data_t3 GENERATE
                            raw_data_t2::group.visit_id AS visit_id_actual:chararray,
                            (long)raw_data_t2::group.date_time AS date_time_actual:long,
                            raw_data_t2::group.unique_id AS unique_id_actual:chararray,
                            flatten(raw_data_t1);


raw_data_t4 = DISTINCT raw_data_t4;
raw_data_t4 = FILTER raw_data_t4 BY date_time_actual > raw_data_t1::date_time;
raw_data_t5 = GROUP raw_data_t4 BY (visit_id_actual, date_time_actual, unique_id_actual);
raw_data_t6 = FOREACH raw_data_t5
{
    success_sort = ORDER raw_data_t4 BY raw_data_t1::date_time DESC;
    success_sort_prev = LIMIT success_sort 1;

    GENERATE group.visit_id_actual, group.date_time_actual, group.unique_id_actual, flatten(success_sort_prev);
}

raw_data_t6 = DISTINCT raw_data_t6;
raw_data_t1_success = DISTINCT raw_data_t1;
raw_data_t7 = JOIN raw_data_t1_success BY unique_id LEFT OUTER, raw_data_t6 BY unique_id_actual;
raw_data_fail = filter raw_data_short_dur by post_page_event_var2 == 'Sign In' and message_name matches '*failure*';

raw_data_fail = FOREACH raw_data_fail GENERATE
                visit_id as visit_id_fail:chararray,
                (long)date_time as date_time_fail:long;

raw_data_t8 = JOIN raw_data_t7 BY raw_data_t1_success::visit_id LEFT OUTER, raw_data_fail BY visit_id_fail;


raw_data_t9 = FILTER raw_data_t8 BY raw_data_t7::raw_data_t1_success::visit_id == raw_data_fail::visit_id_fail AND ((raw_data_t6::success_sort_prev::raw_data_t2::raw_data_t1::date_time IS NULL AND raw_data_t7::raw_data_t1_success::date_time >= raw_data_fail::date_time_fail) OR (raw_data_t6::success_sort_prev::raw_data_t2::raw_data_t1::date_time IS NOT NULL AND raw_data_t6::success_sort_prev::raw_data_t2::raw_data_t1::date_time <= raw_data_fail::date_time_fail AND raw_data_t7::raw_data_t1_success::date_time >= raw_data_fail::date_time_fail));

raw_data_t10 = group raw_data_t9 BY (raw_data_t7::raw_data_t1_success::visit_id, raw_data_t7::raw_data_t1_success::unique_id, raw_data_t7::raw_data_t1_success::date_time);

raw_login_duration_calc = FOREACH raw_data_t10 GENERATE
    group.visit_id as visit_id_f:chararray,
    group.unique_id as unique_id_f:chararray,
    (double)(group.date_time - MIN(raw_data_t9.date_time_fail)) as login_duration_sec:double;

-- Logic for login_duration end ----------------------------------------------------------------

raw_data_calc = JOIN raw_data_calc BY unique_id_1 LEFT OUTER, raw_login_duration_calc BY unique_id_f;


-- message__feature__name

raw_data_message_data = FOREACH raw_data_short_dur GENERATE
    unique_id AS unique_id:chararray,
    FLATTEN(TOKENIZE(post_event_list,',')) AS post_event_list_mod:chararray;

raw_data_message_data_join = JOIN raw_data_message_data BY (chararray)post_event_list_mod LEFT OUTER, map_custom_event BY (chararray)event_id USING 'replicated';
raw_data_message_data_join = FILTER raw_data_message_data_join BY map_custom_event::event_id IS NOT NULL;

raw_data_message_names_final = FOREACH raw_data_message_data_join GENERATE
    raw_data_message_data::unique_id AS unique_id:chararray,
    map_custom_event::detail AS message_feature_name:chararray;

raw_data_final_join = JOIN raw_data_calc BY raw_data_calc::unique_id_1 FULL OUTER, raw_data_message_names_final BY unique_id;
raw_data_final = FOREACH raw_data_final_join GENERATE
    (chararray)raw_data_calc::unique_id_1 AS unique_id:chararray,
    (chararray)raw_data_calc::visit_id_1 AS visit_id:chararray,
    (chararray)raw_data_calc::previous_visit_id_1 AS previous_visit_id:chararray,
    (chararray)raw_data_calc::page_sub_section_1 AS page_sub_section:chararray,
    (int)raw_data_calc::sequence_number_1 AS sequence_number:int,
    (int)raw_data_calc::visit_failed_attempts_1 AS visit_failed_attempts:int,  -- setting this to NULL temporarily
    (double)raw_login_duration_calc::login_duration_sec AS visit_login_duration:double,  -- setting this to NULL temporarily
    (long)raw_data_short_g_f::visit_start_timestamp_1 AS visit_start_timestamp:long,
    (chararray)raw_data_calc::message_name_1 AS message_name:chararray,
    (chararray)raw_data_message_names_final::message_feature_name AS message_feature_name:chararray;


STORE raw_data_final INTO '$TMP_db.$dbprefix_calc' USING org.apache.hive.hcatalog.pig.HCatStorer();
