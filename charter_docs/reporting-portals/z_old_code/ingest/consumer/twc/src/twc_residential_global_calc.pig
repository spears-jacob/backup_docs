set job.priority HIGH;
set output.compression.enabled true;
set mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
set pig.exec.mapPartAgg true; -- for optimizing the group by statements
set pig.cachedbag.memusage 0.35; -- increased the memory usage of the pig script by 15%. Previously it was 20% and now it is 35%
set default_parallel 400;

REGISTER 'hdfs:///udf/hadoop-libs-pig-1.0.4.jar';
DEFINE enumerate com.spectrum.pig.udfs.Enumerate('1');

raw_twc_residential_global = LOAD '$TMP_db.twc_residential_global_denorm_all_files' USING org.apache.hive.hcatalog.pig.HCatLoader();

map_custom_event = LOAD '$LKP_db.twc_residential_global_event' USING org.apache.hive.hcatalog.pig.HCatLoader();
map_custom_event = DISTINCT map_custom_event;

-- relation created for calculation ----------------

raw_twc_residential_global_short = FOREACH raw_twc_residential_global GENERATE
    unique_id as unique_id:chararray,
    (hit_source!=5?(hit_source!=7?(hit_source!=8?(hit_source!=9?CONCAT(CONCAT((chararray)post_visid_high,(chararray)post_visid_low),(chararray)visit_num):''):''):''):'') AS visit_id:chararray,
    (hit_source!=5?(hit_source!=7?(hit_source!=8?(hit_source!=9?CONCAT(CONCAT((chararray)post_visid_high,(chararray)post_visid_low),(chararray)(visit_num-1)):''):''):''):'') AS previous_visit_id:chararray,
    rank_id as rank_id:long,
    (long)post_cust_hit_time_gmt as date_time:long,
    post_page_event_var2 as post_page_event_var2:chararray,
    post_prop63 as page_sub_section,
    (chararray)(CASE post_page_event
      WHEN 0 THEN post_pagename
      WHEN 100 THEN post_page_event_var2
      WHEN 101 THEN post_page_event_var1
      WHEN 102 THEN post_page_event_var1
      ELSE (chararray)post_page_event
     END) AS message_name:chararray, -- using for more exhaustive message_name generation
    post_event_list as post_event_list:chararray;

raw_twc_residential_global_short_dur = raw_twc_residential_global_short;

-- message.sequence_number calculation
visit_id_complete_group = GROUP raw_twc_residential_global_short BY visit_id;

visit_id_complete = FOREACH visit_id_complete_group
{
    sorted_visit = DISTINCT raw_twc_residential_global_short;
    sorted_visit = ORDER sorted_visit BY date_time ASC, rank_id ASC;
    enu_visit = enumerate(sorted_visit);
    GENERATE flatten(enu_visit);
};

raw_twc_residential_global_short = JOIN raw_twc_residential_global_short BY unique_id, visit_id_complete BY unique_id;

raw_twc_residential_global_calc = FOREACH raw_twc_residential_global_short GENERATE
    raw_twc_residential_global_short::unique_id AS unique_id_1:chararray,
    raw_twc_residential_global_short::visit_id AS visit_id_1:chararray,
    raw_twc_residential_global_short::previous_visit_id as previous_visit_id_1:chararray,
    raw_twc_residential_global_short::page_sub_section as page_sub_section_1:chararray,
    (int)i AS sequence_number_1:int,
    NULL AS visit_failed_attempts_1:int,  -- setting this to NULL temporarily
    NULL AS visit_login_duration_1:double,  -- setting this to NULL temporarily
    raw_twc_residential_global_short::message_name AS message_name_1:chararray;

raw_twc_residential_global_calc = DISTINCT raw_twc_residential_global_calc;

-- calculations for visit_start_timestamp
raw_twc_residential_global_short_g_f = FOREACH visit_id_complete_group
                {
                    GENERATE group AS visit_id_t:chararray,
                    MIN(raw_twc_residential_global_short.date_time) AS visit_start_timestamp_1 : long;
                 };

raw_twc_residential_global_calc = Join raw_twc_residential_global_calc BY visit_id_1 LEFT OUTER, raw_twc_residential_global_short_g_f BY visit_id_t;


-- Logic for login_duration start ----------------------------------------------------------------

raw_twc_residential_global_t1 = filter raw_twc_residential_global_short_dur by post_page_event_var2 == 'Sign In' and message_name matches '*success*';
raw_twc_residential_global_t2 = group raw_twc_residential_global_t1 by (visit_id, date_time, unique_id);
raw_twc_residential_global_t3 = JOIN raw_twc_residential_global_t2 BY group.$0, raw_twc_residential_global_t1 by visit_id;
raw_twc_residential_global_t3 = DISTINCT raw_twc_residential_global_t3;

raw_twc_residential_global_t4 = FOREACH
                raw_twc_residential_global_t3 GENERATE
                            raw_twc_residential_global_t2::group.visit_id AS visit_id_actual:chararray,
                            (long)raw_twc_residential_global_t2::group.date_time AS date_time_actual:long,
                            raw_twc_residential_global_t2::group.unique_id AS unique_id_actual:chararray,
                            flatten(raw_twc_residential_global_t1);


raw_twc_residential_global_t4 = DISTINCT raw_twc_residential_global_t4;
raw_twc_residential_global_t4 = FILTER raw_twc_residential_global_t4 BY date_time_actual > raw_twc_residential_global_t1::date_time;
raw_twc_residential_global_t5 = GROUP raw_twc_residential_global_t4 BY (visit_id_actual, date_time_actual, unique_id_actual);
raw_twc_residential_global_t6 = FOREACH raw_twc_residential_global_t5
{
    success_sort = ORDER raw_twc_residential_global_t4 BY raw_twc_residential_global_t1::date_time DESC;
    success_sort_prev = LIMIT success_sort 1;

    GENERATE group.visit_id_actual, group.date_time_actual, group.unique_id_actual, flatten(success_sort_prev);
}

raw_twc_residential_global_t6 = DISTINCT raw_twc_residential_global_t6;
raw_twc_residential_global_t1_success = DISTINCT raw_twc_residential_global_t1;
raw_twc_residential_global_t7 = JOIN raw_twc_residential_global_t1_success BY unique_id LEFT OUTER, raw_twc_residential_global_t6 BY unique_id_actual;
raw_twc_residential_global_fail = filter raw_twc_residential_global_short_dur by post_page_event_var2 == 'Sign In' and message_name matches '*failure*';

raw_twc_residential_global_fail = FOREACH raw_twc_residential_global_fail GENERATE
                visit_id as visit_id_fail:chararray,
                (long)date_time as date_time_fail:long;

raw_twc_residential_global_t8 = JOIN raw_twc_residential_global_t7 BY raw_twc_residential_global_t1_success::visit_id LEFT OUTER, raw_twc_residential_global_fail BY visit_id_fail;


raw_twc_residential_global_t9 = FILTER raw_twc_residential_global_t8 BY raw_twc_residential_global_t7::raw_twc_residential_global_t1_success::visit_id == raw_twc_residential_global_fail::visit_id_fail AND ((raw_twc_residential_global_t6::success_sort_prev::raw_twc_residential_global_t2::raw_twc_residential_global_t1::date_time IS NULL AND raw_twc_residential_global_t7::raw_twc_residential_global_t1_success::date_time >= raw_twc_residential_global_fail::date_time_fail) OR (raw_twc_residential_global_t6::success_sort_prev::raw_twc_residential_global_t2::raw_twc_residential_global_t1::date_time IS NOT NULL AND raw_twc_residential_global_t6::success_sort_prev::raw_twc_residential_global_t2::raw_twc_residential_global_t1::date_time <= raw_twc_residential_global_fail::date_time_fail AND raw_twc_residential_global_t7::raw_twc_residential_global_t1_success::date_time >= raw_twc_residential_global_fail::date_time_fail));

raw_twc_residential_global_t10 = group raw_twc_residential_global_t9 BY (raw_twc_residential_global_t7::raw_twc_residential_global_t1_success::visit_id, raw_twc_residential_global_t7::raw_twc_residential_global_t1_success::unique_id, raw_twc_residential_global_t7::raw_twc_residential_global_t1_success::date_time);

raw_login_duration_calc = FOREACH raw_twc_residential_global_t10 GENERATE
    group.visit_id as visit_id_f:chararray,
    group.unique_id as unique_id_f:chararray,
    (double)(group.date_time - MIN(raw_twc_residential_global_t9.date_time_fail)) as login_duration_sec:double;

-- Logic for login_duration end ----------------------------------------------------------------

raw_twc_residential_global_calc = JOIN raw_twc_residential_global_calc BY unique_id_1 LEFT OUTER, raw_login_duration_calc BY unique_id_f;

raw_twc_residential_global_calc_f = FOREACH raw_twc_residential_global_calc GENERATE
    (chararray)raw_twc_residential_global_calc::unique_id_1 AS unique_id:chararray,
    (chararray)raw_twc_residential_global_calc::visit_id_1 AS visit_id:chararray,
    (chararray)raw_twc_residential_global_calc::previous_visit_id_1 AS previous_visit_id:chararray,
    (chararray)raw_twc_residential_global_calc::page_sub_section_1 AS page_sub_section:chararray,
    (int)raw_twc_residential_global_calc::sequence_number_1 AS sequence_number:int,
    (int)raw_twc_residential_global_calc::visit_failed_attempts_1 AS visit_failed_attempts:int,  -- setting this to NULL temporarily
    (double)raw_login_duration_calc::login_duration_sec AS visit_login_duration:double,  -- setting this to NULL temporarily
    (long)raw_twc_residential_global_short_g_f::visit_start_timestamp_1 AS visit_start_timestamp:long,
    (chararray)raw_twc_residential_global_calc::message_name_1 AS message_name:chararray;

-- message__feature__name
raw_twc_residential_global_message_data = FOREACH raw_twc_residential_global_short_dur GENERATE
    unique_id AS unique_id:chararray,
    FLATTEN(TOKENIZE(post_event_list,',')) AS post_event_list_mod:chararray;

raw_twc_residential_global_message_data_join = JOIN raw_twc_residential_global_message_data BY (chararray)post_event_list_mod LEFT OUTER, map_custom_event BY (chararray)event_id USING 'replicated';
raw_twc_residential_global_message_data_join = FILTER raw_twc_residential_global_message_data_join BY map_custom_event::event_id IS NOT NULL;

raw_twc_residential_global_message_names_final = FOREACH raw_twc_residential_global_message_data_join GENERATE
    raw_twc_residential_global_message_data::unique_id AS unique_id:chararray,
    map_custom_event::detail AS message_feature_name:chararray;

raw_twc_residential_global_final_join = JOIN raw_twc_residential_global_calc BY raw_twc_residential_global_calc::unique_id_1 FULL OUTER, raw_twc_residential_global_message_names_final BY unique_id;
raw_twc_residential_global_calc_final = FOREACH raw_twc_residential_global_final_join GENERATE
    (chararray)raw_twc_residential_global_calc::unique_id_1 AS unique_id:chararray,
    (chararray)raw_twc_residential_global_calc::visit_id_1 AS visit_id:chararray,
    (chararray)raw_twc_residential_global_calc::previous_visit_id_1 AS previous_visit_id:chararray,
    (chararray)raw_twc_residential_global_calc::page_sub_section_1 AS page_sub_section:chararray,
    (int)raw_twc_residential_global_calc::sequence_number_1 AS sequence_number:int,
    (int)raw_twc_residential_global_calc::visit_failed_attempts_1 AS visit_failed_attempts:int,  -- setting this to NULL temporarily
    (double)raw_login_duration_calc::login_duration_sec AS visit_login_duration:double,  -- setting this to NULL temporarily
    (long)raw_twc_residential_global_short_g_f::visit_start_timestamp_1 AS visit_start_timestamp:long,
    (chararray)raw_twc_residential_global_calc::message_name_1 AS message_name:chararray,
    (chararray)raw_twc_residential_global_message_names_final::message_feature_name AS message_feature_name:chararray;

-- DUMP raw_twc_residential_global_calc_final;

STORE raw_twc_residential_global_calc_final INTO '$TMP_db.twc_residential_global_calc' USING org.apache.hive.hcatalog.pig.HCatStorer();
