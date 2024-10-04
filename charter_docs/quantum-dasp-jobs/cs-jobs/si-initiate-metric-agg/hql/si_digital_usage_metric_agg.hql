USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

MSCK REPAIR TABLE ${env:ENVIRONMENT}.atom_cs_call_care_data_3;

-- ..............................................
-- Updating cs_initiate_si_and_pro_install_accounts
-- ..............................................
DROP TABLE IF EXISTS ${env:TMP_db}.accounts_initiate_si_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.accounts_initiate_si_${env:CLUSTER} AS
    select
        agg.account_number as account_id
        ,agg.mso as mso
        ,agg.application_name as si_channel
        -- new si agg table does not have user_package field as old summary_agg_master did
        ,'' as user_package
        ,case when upper(aas.extract_type) = 'RESIDENTIAL' then 'RESIDENTIAL'
        WHEN UPPER(aas.extract_type) = 'COMMERCIAL_BUSINESS' THEN 'SMB'
        ELSE NULL END as subscriber_type
        ,visit_id as interaction_identifier
        ,sum(if((agg.key_internet_initiated + agg.key_wifi_initiated + agg.key_receiver_initiated > 0) AND
                CASE WHEN key_internet_initiated > 0 THEN NOT visit_internet_funnel_invalid
                WHEN key_wifi_initiated > 0 THEN NOT visit_wifi_funnel_invalid
                WHEN key_receiver_initiated > 0 THEN NOT visit_receiver_funnel_invalid
                END, 1, 0)) as initiated_self_install
        ,sum(if((agg.key_internet_activationsuccess + agg.key_wifi_activationsuccess + agg.key_receiver_activationsuccess > 0)
                AND CASE WHEN key_internet_activationsuccess > 0 THEN NOT visit_internet_funnel_invalid
                WHEN key_wifi_activationsuccess > 0 THEN NOT visit_wifi_funnel_invalid
                WHEN key_receiver_activationsuccess > 0 THEN NOT visit_receiver_funnel_invalid
                END, 1, 0)) as any_service_activated
        ,'si_page_agg' as table_source
        ,agg.denver_date as si_date
    from `${env:SI_AGG}` as agg
    LEFT JOIN `${env:AAS}` as aas
        ON agg.account_number = aas.encrypted_padded_account_number_256
        AND aas.partition_date_denver = agg.denver_date
    where agg.application_name in ('MySpectrum', 'SelfInstall')
        and (agg.denver_date >= '${env:STARTDATE}'
        and agg.denver_date < '${env:ENDDATE}')
    group by
        agg.account_number
        ,mso
        ,application_name
        ,''
        ,case when upper(aas.extract_type) = 'RESIDENTIAL' then 'RESIDENTIAL'
        WHEN UPPER(aas.extract_type) = 'COMMERCIAL_BUSINESS' THEN 'SMB'
        ELSE NULL END
        ,visit_id
        ,'si_page_agg'
        ,agg.denver_date
    ;

    ------- pro_install addition -------
DROP TABLE IF EXISTS ${env:TMP_db}.pro_installs_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.pro_installs_${env:CLUSTER} AS
    select
        distinct
        aas.encrypted_legacy_account_number_256
        ,case
            WHEN awo.legacy_company = 'BHN' then 'L-BHN'
            WHEN awo.legacy_company = 'CHR' then 'L-CHTR'
            when awo.legacy_company = 'TWC' THEN 'L-TWC'
            else awo.legacy_company
            end as mso
        ,'Pro-Install' as si_channel
        ,cast(null as varchar(8)) as user_package
        ,case when upper(aas.extract_type) = 'RESIDENTIAL' then 'RESIDENTIAL'
        WHEN UPPER(aas.extract_type) = 'COMMERCIAL_BUSINESS' THEN 'SMB'
        ELSE NULL END as subscriber_type
        ,awo.order_number as interaction_identifier
        ,cast(null as bigint) as initiated_self_install
        ,cast(null as bigint) as any_service_activated
        ,'Atom Work Orders' as table_source
        ,awo.partition_date_denver as pro_install_date
    FROM `${env:AWO}` as awo
    join`${env:AAS}` aas
        on awo.encrypted_account_key_256 = aas.encrypted_account_key_256
        and awo.legacy_company = aas.legacy_company
        and aas.partition_date_denver = awo.partition_date_denver
    where
        (awo.partition_date_denver >= '${env:STARTDATE}'
        and awo.partition_date_denver < '${env:ENDDATE}')
        AND aas.partition_date_denver = awo.partition_date_denver
        AND is_home_ship <> true
        and is_store_pickup <> true
        and is_self_install_move_transfer_enriched <> true
        and job_category_code = 'W'
        and service_code RLIKE 'LA720|LA721|LA723|LA727|LA824|LA825|LA850|LA855|LA860|LK705|LK747|LA775'
        and job_status_code IN ('C', 'CP', 'D')
        and includes_truck_roll
    ;

DROP TABLE IF EXISTS ${env:TMP_db}.acquisition_so_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.acquisition_so_${env:CLUSTER} AS
    select
        encrypted_account_number_256 as encrypted_account_number
        ,case
            WHEN legacy_company = 'BHN' then 'L-BHN'
            WHEN legacy_company = 'CHR' then 'L-CHTR'
            when legacy_company = 'TWC' THEN 'L-TWC'
            else legacy_company
            end as mso
        ,sale_type as sales_type
        ,sum(if(self_install_fulfilment_type = 'Home Shipment', 1, 0)) as home_shipment_flag
        ,sum(if(self_install_fulfilment_type = 'Store Pickup', 1, 0)) as store_pickup_flag
    from `${env:ASOI}`as prod_so
    where sale_type = 'Acquisition'
        and self_install_fulfilment_type in ('Home Shipment', 'Store Pickup')
        and sale_enter_date >= '${env:STARTDATE_15}'
        and sale_enter_date < '${env:ENDDATE_15}'
    group by
    encrypted_account_number_256,
    case
        WHEN legacy_company = 'BHN' then 'L-BHN'
        WHEN legacy_company = 'CHR' then 'L-CHTR'
        when legacy_company = 'TWC' THEN 'L-TWC'
        else legacy_company
        end,
    sale_type
    ;

DROP TABLE IF EXISTS ${env:TMP_db}.initiate_si_and_pro_install_accounts_temp_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.initiate_si_and_pro_install_accounts_temp_${env:CLUSTER} AS
    select
        acquisition_so.sales_type
        ,if(acquisition_so.home_shipment_flag > 0, 'Y', 'N') as home_shipment_flag
        ,if(acquisition_so.store_pickup_flag > 0, 'Y', 'N') as store_pickup_flag
        ,accounts_initiate_si.*
    from ${env:TMP_db}.accounts_initiate_si_${env:CLUSTER} as accounts_initiate_si
    left join ${env:TMP_db}.acquisition_so_${env:CLUSTER} as acquisition_so
        on accounts_initiate_si.account_id = acquisition_so.encrypted_account_number
        and accounts_initiate_si.mso = acquisition_so.mso
    WHERE initiated_self_install > 0
    ;
INSERT INTO TABLE ${env:TMP_db}.initiate_si_and_pro_install_accounts_temp_${env:CLUSTER}
    select
        acquisition_so.sales_type
        ,if(acquisition_so.home_shipment_flag > 0, 'Y', 'N') as home_shipment_flag
        ,if(acquisition_so.store_pickup_flag > 0, 'Y', 'N') as store_pickup_flag
        ,pro_installs.*
    from ${env:TMP_db}.pro_installs_${env:CLUSTER} as pro_installs
        left join ${env:TMP_db}.acquisition_so_${env:CLUSTER} as acquisition_so
        on pro_installs.encrypted_legacy_account_number_256 = acquisition_so.encrypted_account_number
        and pro_installs.mso = acquisition_so.mso
    ;
INSERT OVERWRITE TABLE ${env:DASP_db}.cs_initiate_si_and_pro_install_accounts
partition (si_date)
select * from ${env:TMP_db}.initiate_si_and_pro_install_accounts_temp_${env:CLUSTER}
;

DROP TABLE IF EXISTS ${env:TMP_db}.accounts_initiate_si_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.acquisition_so_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.pro_installs_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.initiate_si_and_pro_install_accounts_temp_${env:CLUSTER} PURGE;

-- ..............................................
-- Updating cs_initiate_si_metric_agg
-- ..............................................


DROP TABLE IF EXISTS ${env:TMP_db}.portals_ma_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.portals_ma_${env:CLUSTER} AS

SELECT
    ma.application_name
    ,ma.portals_unique_acct_key
    ,ma.mso
    ,ma.denver_date
    ,ma.visit_id
    ,SUM(portals_manage_auto_payment_starts) as portals_manage_auto_payment_starts
    ,SUM(portals_manage_auto_payment_successes) as portals_manage_auto_payment_successes
    ,SUM(portals_set_up_auto_payment_starts) as portals_set_up_auto_payment_starts
    ,SUM(portals_set_up_auto_payment_submits) as portals_set_up_auto_payment_submits
    ,SUM(portals_set_up_auto_payment_successes) as portals_set_up_auto_payment_successes
    ,SUM(portals_one_time_payment_starts) as portals_one_time_payment_starts
    ,SUM(portals_one_time_payment_submits) as portals_one_time_payment_submits
    ,SUM(portals_one_time_payment_successes) as portals_one_time_payment_successes
    ,sum(portals_view_online_statments) as portals_view_online_statments
    ,SUM(portals_equipment_confirm_edit_ssid_select_action) as portals_equipment_confirm_edit_ssid_select_action
    ,SUM(portals_equipment_edit_ssid_select_action) as portals_equipment_edit_ssid_select_action
    ,SUM(portals_scp_click_pause_device) as portals_scp_click_pause_device
    ,SUM(portals_scp_click_unpause_device) as portals_scp_click_unpause_device
    ,SUM(portals_scp_click_cancel_pause_device) as portals_scp_click_cancel_pause_device
    ,SUM(portals_support_page_views) as portals_support_page_views
    ,sum(portals_all_equipment_reset_flow_starts) as portals_all_equipment_reset_flow_starts
    ,SUM(portals_all_equipment_reset_flow_successes) as portals_all_equipment_reset_flow_successes
    FROM `${env:MA}` as ma
    /*to only include metric_agg records where account is in self-install table, ie trims row_count. Query would not run during development without this.*/
    join (select distinct account_id, mso from `${env:ISI_and_PI}`) as acct_inclu
    on ma.portals_unique_acct_key = acct_inclu.account_id
    and ma.mso = acct_inclu.mso
    /*list of visit_ids to exclude*/
    left join (select distinct interaction_identifier from `${env:ISI_and_PI}`) as visit_exclu
    on ma.visit_id = visit_exclu.interaction_identifier
    WHERE denver_date >= '${env:LAST_MONTH_START_DATE}'
    AND denver_date <= '${env:END_DATE}'
    and application_name in ('specnet', 'myspectrum', 'smb')
    /*to exclude portals_ma visits where user did self-install. (i.e. so we do not count digital adoption where visit itself was the install set-up)*/
    and visit_exclu.interaction_identifier is null
    GROUP BY
    ma.application_name
    ,ma.portals_unique_acct_key
    ,ma.mso
    ,ma.denver_date
    ,ma.visit_id
;
DROP TABLE IF EXISTS ${env:TMP_db}.dist_calls_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.dist_calls_${env:CLUSTER} AS

SELECT distinct
encrypted_account_number_256
,CASE
WHEN account_agent_mso = 'TWC' then 'L-TWC'
WHEN account_agent_mso = 'BHN' then 'L-BHN'
WHEN account_agent_mso = 'CHR' THEN 'L-CHTR'
ELSE 'L-'||account_agent_mso end as mso
,call_end_date_utc
/*to be joined on to avoid instances where si_date > call_date, which can arise as result of multiple si_dates*/
,si_pi_accts.si_date as si_date_joined
from `${env:CC}` as cc
join `${env:ISI_and_PI}` as si_pi_accts
on cc.encrypted_account_number_256 = si_pi_accts.account_id
and CASE
WHEN cc.account_agent_mso = 'TWC' then 'L-TWC'
WHEN cc.account_agent_mso = 'BHN' then 'L-BHN'
WHEN cc.account_agent_mso = 'CHR' THEN 'L-CHTR'
ELSE 'L-'||cc.account_agent_mso end = si_pi_accts.mso
where
call_end_date_utc >= '${env:LAST_MONTH_START_DATE}'
AND call_end_date_utc <= '${env:END_DATE}'
and cc.segment_handled_flag = 1
;
DROP TABLE IF EXISTS ${env:TMP_db}.agg_initiate_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.agg_initiate_${env:CLUSTER} AS
    select
    table_source
    ,account_id
    ,mso
    ,sales_type
    ,home_shipment_flag
    ,store_pickup_flag
    ,si_channel
    ,user_package
    ,subscriber_type
    ,sum(initiated_self_install) as initiated_self_install
    ,sum(any_service_activated) as any_service_activated
    ,min(si_date) as si_date
    from `${env:ISI_and_PI}`
    where si_date >= '${env:LAST_MONTH_START_DATE}'and
    si_date < '${env:ENDDATE}'
    group by
    table_source
    ,account_id
    ,mso
    ,sales_type
    ,home_shipment_flag
    ,store_pickup_flag
    ,si_channel
    ,user_package
    ,subscriber_type
;
DROP TABLE IF EXISTS ${env:TMP_db}.cs_initiate_si_metric_agg_final_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.cs_initiate_si_metric_agg_final_${env:CLUSTER} AS
/*call enrich portion*/
    select
    agg_initiate.account_id
    ,agg_initiate.mso
    ,agg_initiate.sales_type
    ,agg_initiate.home_shipment_flag
    ,agg_initiate.store_pickup_flag
    ,agg_initiate.si_channel
    ,agg_initiate.user_package
    ,agg_initiate.subscriber_type
    ,dist_calls.call_end_date_utc as metric_date
    ,datediff(to_date(call_end_date_utc), to_date(si_date)) as days_since_initiate
    ,if(agg_initiate.initiated_self_install > 0, 'TRUE', 'FALSE') as initiated_self_install
    ,if(agg_initiate.any_service_activated > 0, 'TRUE', 'FALSE') as any_service_activated
    ,cast(0 as bigint) as portals_manage_auto_payment_starts
    ,cast(0 as bigint) as portals_manage_auto_payment_successes
    ,cast(0 as bigint) as portals_set_up_auto_payment_starts
    ,cast(0 as bigint) as portals_set_up_auto_payment_submits
    ,cast(0 as bigint) as portals_set_up_auto_payment_successes
    ,cast(0 as bigint) as portals_one_time_payment_starts
    ,cast(0 as bigint) as portals_one_time_payment_submits
    ,cast(0 as bigint) as portals_one_time_payment_successes
    ,cast(0 as bigint) as portals_view_online_statments
    ,cast(0 as bigint) as portals_equipment_confirm_edit_ssid_select_action
    ,cast(0 as bigint) as portals_equipment_edit_ssid_select_action
    ,cast(0 as bigint) as portals_scp_click_pause_device
    ,cast(0 as bigint) as portals_scp_click_unpause_device
    ,cast(0 as bigint) as portals_scp_click_cancel_pause_device
    ,cast(0 as bigint) as portals_support_page_views
    ,cast(0 as bigint) as portals_all_equipment_reset_flow_starts
    ,cast(0 as bigint) as portals_all_equipment_reset_flow_successes
    ,cast(0 as bigint) as visit_count
    ,count(distinct call_end_date_utc) as days_w_call
    ,agg_initiate.table_source
    ,agg_initiate.si_date
    from ${env:TMP_db}.agg_initiate_${env:CLUSTER} as agg_initiate
    left join ${env:TMP_db}.dist_calls_${env:CLUSTER} as dist_calls
    on agg_initiate.account_id = dist_calls.encrypted_account_number_256
    and agg_initiate.mso = dist_calls.mso
    and agg_initiate.si_date = dist_calls.si_date_joined
    group by
    agg_initiate.account_id
    ,agg_initiate.mso
    ,agg_initiate.sales_type
    ,agg_initiate.home_shipment_flag
    ,agg_initiate.store_pickup_flag
    ,agg_initiate.si_channel
    ,agg_initiate.user_package
    ,agg_initiate.subscriber_type
    ,dist_calls.call_end_date_utc
    ,datediff(to_date(call_end_date_utc), to_date(si_date))
    ,agg_initiate.initiated_self_install
    ,agg_initiate.any_service_activated
    ,agg_initiate.table_source
    ,agg_initiate.si_date
;
INSERT INTO TABLE ${env:TMP_db}.cs_initiate_si_metric_agg_final_${env:CLUSTER}
select
agg_initiate.account_id
,agg_initiate.mso
,agg_initiate.sales_type
,agg_initiate.home_shipment_flag
,agg_initiate.store_pickup_flag
,agg_initiate.si_channel
,agg_initiate.user_package
,agg_initiate.subscriber_type
,portals_ma.denver_date as metric_date
,datediff(to_date(denver_date), to_date(si_date)) as days_since_initiate
,if(agg_initiate.initiated_self_install > 0, 'TRUE', 'FALSE') as initiated_self_install
,if(agg_initiate.any_service_activated > 0, 'TRUE', 'FALSE') as any_service_activated
,sum(portals_ma.portals_manage_auto_payment_starts) as portals_manage_auto_payment_starts
,sum(portals_ma.portals_manage_auto_payment_successes) as portals_manage_auto_payment_successes
,sum(portals_ma.portals_set_up_auto_payment_starts) as portals_set_up_auto_payment_starts
,sum(portals_ma.portals_set_up_auto_payment_submits) as portals_set_up_auto_payment_submits
,sum(portals_ma.portals_set_up_auto_payment_successes) as portals_set_up_auto_payment_successes
,sum(portals_ma.portals_one_time_payment_starts) as portals_one_time_payment_starts
,sum(portals_ma.portals_one_time_payment_submits) as portals_one_time_payment_submits
,sum(portals_ma.portals_one_time_payment_successes) as portals_one_time_payment_successes
,sum(portals_ma.portals_view_online_statments) as portals_view_online_statments
,sum(portals_ma.portals_equipment_confirm_edit_ssid_select_action) as portals_equipment_confirm_edit_ssid_select_action
,sum(portals_ma.portals_equipment_edit_ssid_select_action) as portals_equipment_edit_ssid_select_action
,sum(portals_ma.portals_scp_click_pause_device) as portals_scp_click_pause_device
,sum(portals_ma.portals_scp_click_unpause_device) as portals_scp_click_unpause_device
,sum(portals_ma.portals_scp_click_cancel_pause_device) as portals_scp_click_cancel_pause_device
,sum(portals_ma.portals_support_page_views) as portals_support_page_views
,sum(portals_ma.portals_all_equipment_reset_flow_starts) as portals_all_equipment_reset_flow_starts
,sum(portals_ma.portals_all_equipment_reset_flow_successes) as portals_all_equipment_reset_flow_successes
,count(portals_ma.visit_id) as visit_count
,cast(null as int) as days_w_call
,agg_initiate.table_source
,agg_initiate.si_date
from ${env:TMP_db}.agg_initiate_${env:CLUSTER} as agg_initiate
left join ${env:TMP_db}.portals_ma_${env:CLUSTER} as portals_ma
on agg_initiate.account_id = portals_ma.portals_unique_acct_key
and agg_initiate.mso = portals_ma.mso
and portals_ma.denver_date >= agg_initiate.si_date
group by
agg_initiate.account_id
,agg_initiate.mso
,agg_initiate.sales_type
,agg_initiate.home_shipment_flag
,agg_initiate.store_pickup_flag
,agg_initiate.si_channel
,agg_initiate.user_package
,agg_initiate.subscriber_type
,portals_ma.denver_date
,datediff(to_date(denver_date), to_date(si_date))
,if(agg_initiate.initiated_self_install > 0, 'TRUE', 'FALSE')
,if(agg_initiate.any_service_activated > 0, 'TRUE', 'FALSE')
,agg_initiate.table_source
,agg_initiate.si_date
;

INSERT OVERWRITE TABLE ${env:DASP_db}.cs_initiate_si_metric_agg
partition (si_date)
select
    account_id
    ,mso
    ,sales_type
    ,home_shipment_flag
    ,store_pickup_flag
    ,si_channel
    ,user_package
    ,subscriber_type
    ,metric_date
    ,days_since_initiate
    ,initiated_self_install
    ,any_service_activated
    ,sum(portals_manage_auto_payment_starts) as portals_manage_auto_payment_starts
    ,sum(portals_manage_auto_payment_successes) as portals_manage_auto_payment_successes
    ,sum(portals_set_up_auto_payment_starts) as portals_set_up_auto_payment_starts
    ,sum(portals_set_up_auto_payment_submits) as portals_set_up_auto_payment_submits
    ,sum(portals_set_up_auto_payment_successes) as portals_set_up_auto_payment_successes
    ,sum(portals_one_time_payment_starts) as portals_one_time_payment_starts
    ,sum(portals_one_time_payment_submits) as portals_one_time_payment_submits
    ,sum(portals_one_time_payment_successes) as portals_one_time_payment_successes
    ,sum(portals_view_online_statments) as portals_view_online_statments
    ,sum(portals_equipment_confirm_edit_ssid_select_action) as portals_equipment_confirm_edit_ssid_select_action
    ,sum(portals_equipment_edit_ssid_select_action) as portals_equipment_edit_ssid_select_action
    ,sum(portals_scp_click_pause_device) as portals_scp_click_pause_device
    ,sum(portals_scp_click_unpause_device) as portals_scp_click_unpause_device
    ,sum(portals_scp_click_cancel_pause_device) as portals_scp_click_cancel_pause_device
    ,sum(portals_support_page_views) as portals_support_page_views
    ,sum(portals_all_equipment_reset_flow_starts) as portals_all_equipment_reset_flow_starts
    ,sum(portals_all_equipment_reset_flow_successes) as portals_all_equipment_reset_flow_successes
    ,sum(visit_count) as visit_count
    ,sum(days_w_call) as days_w_call
    ,table_source
    ,si_date
    from ${env:TMP_db}.cs_initiate_si_metric_agg_final_${env:CLUSTER}
    WHERE days_since_initiate <= 120 or metric_date is null
    group by
    account_id
    ,mso
    ,sales_type
    ,home_shipment_flag
    ,store_pickup_flag
    ,si_channel
    ,user_package
    ,subscriber_type
    ,metric_date
    ,days_since_initiate
    ,initiated_self_install
    ,any_service_activated
    ,table_source
    ,si_date
;
