SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

SELECT
old.segment_id
--,if(old.call_inbound_key = new.call_inbound_key ,1,0) as call_inbound_key_flag
--,if(old.call_id              =new.call_id,1,0) as call_id_flag
--,if(old.call_start_date_utc  =new.call_start_date_utc,1,0) as call_start_date_utc_flag 
--,if(old.call_start_time_utc  =new.call_start_time_utc,1,0) as call_start_time_utc_flag 
--,if(old.call_end_time_utc    =new.call_end_time_utc,1,0) as call_end_time_utc_flag 
--,if(old.call_start_datetime_utc =new.call_start_datetime_utc,1,0) as call_start_datetime_utc_flag 
--,if(old.call_end_datetime_utc =new.call_end_datetime_utc,1,0) as call_end_datetime_utc_flag 
--,if(old.call_start_timestamp_utc =new.call_start_timestamp_utc,1,0) as call_start_timestamp_utc_flag 
--,if(old.call_end_timestamp_utc =new.call_end_timestamp_utc,1,0) as call_end_timestamp_utc_flag 
--wrong--,if(old.previous_call_time_utc =new.previous_call_time_utc,1,0) as previous_call_time_utc_flag 
--,if(old.segment_number       =new.segment_number,1,0) as segment_number_flag 
--,if(old.segment_status_disposition =new.segment_status_disposition,1,0) as segment_status_disposition_flag 
--,if(old.segment_start_time_utc =new.segment_start_time_utc,1,0) as segment_start_time_utc_flag 
--wrong --,if(old.segment_end_time_utc =new.segment_end_time_utc,1,0) as segment_end_time_utc_flag 
--,if(old.segment_start_datetime_utc =new.segment_start_datetime_utc,1,0) as segment_start_datetime_utc_flag 
--wrong --,if(old.segment_end_datetime_utc =new.segment_end_datetime_utc,1,0) as segment_end_datetime_utc_flag 
--,if(old.segment_start_timestamp_utc =new.segment_start_timestamp_utc,1,0) as segment_start_timestamp_utc_flag 
--wrong --,if(old.segment_end_timestamp_utc =new.segment_end_timestamp_utc,1,0) as segment_end_timestamp_utc_flag 
--wrong --,if(old.segment_duration_seconds =new.segment_duration_seconds,1,0) as segment_duration_seconds_flag 
--wrong --,if(old.segment_duration_minutes =new.segment_duration_minutes,1,0) as segment_duration_minutes_flag 
--,if(old.segment_handled_flag=new.segment_handled_flag,1,0) as segment_handled_flag_flag 
--wrong --,if(old.customer_call_count_indicator =new.customer_call_count_indicator,1,0) as customer_call_count_indicator_flag 
--,if(old.call_handled_flag   =new.call_handled_flag,1,0) as call_handled_flag_flag 
--,if(old.call_owner           =new.call_owner,1,0) as call_owner_flag
--,if(old.product              =new.product,1,0) as product_flag
--wrong --,if(old.account_number       =new.account_number,1,0) as account_number_flag 
--,if(old.customer_account_number =new.customer_account_number,1,0) as customer_account_number_flag 
--wrong --,if(old.customer_type        =new.customer_type,1,0) as customer_type_flag 
--wrong --,if(old.customer_subtype     =new.customer_subtype,1,0) as customer_subtype_flag 
--,if(old.truck_roll_flag     =new.truck_roll_flag,1,0) as truck_roll_flag_flag 
--wrong --,if(old.notes_txt            =new.notes_txt,1,0) as notes_txt_flag
--,if(old.resolution_description =new.resolution_description,1,0) as resolution_description_flag 
--,if(old.cause_description    =new.cause_description,1,0) as cause_description_flag 
--,if(old.issue_description    =new.issue_description,1,0) as issue_description_flag 
--,if(old.company_code         =new.company_code,1,0) as company_code_flag
--,if(old.service_call_tracker_id =new.service_call_tracker_id,1,0) as service_call_tracker_id_flag 
--,if(old.created_on           =new.created_on,1,0) as created_on_flag
--,if(old.created_by           =new.created_by,1,0) as created_by_flag
--,if(old.phone_number_from_tracker =new.phone_number_from_tracker,1,0) as phone_number_from_tracker_flag 
--,if(old.call_type            =new.call_type,1,0) as call_type_flag
--,if(old.split_sum_desc       =new.split_sum_desc,1,0) as split_sum_desc_flag 
--,if(old.location_name        =new.location_name,1,0) as location_name_flag 
--,if(old.care_center_management_name =new.care_center_management_name,1,0) as care_center_management_name_flag 
--,if(old.agent_job_role_name  =new.agent_job_role_name,1,0) as agent_job_role_name_flag 
--,if(old.agent_effective_hire_date =new.agent_effective_hire_date,1,0) as agent_effective_hire_date_flag 
--,if(old.agent_mso            =new.agent_mso,1,0) as agent_mso_flag
--,if(old.eduid                =new.eduid,1,0) as eduid_flag
--,if(old.last_handled_segment_flag=new.last_handled_segment_flag,1,0) as last_handled_segment_flag_flag 
--,if(old.record_update_timestamp =new.record_update_timestamp,1,0) as record_update_timestamp_flag 
--,if(old.source               =new.source,1,0) as source_flag
--,if(old.enhanced_account_number=new.enhanced_account_number,1,0) as enhanced_account_number_flag 
--,if(old.call_end_date_utc =new.call_end_date_utc ,1,0) as call_end_date_utc_flag   
FROM
prod.cs_call_data old
INNER JOIN
test.cs_call_care_data_amo new
on old.segment_id = new.segment_id
WHERE
old.call_end_date_utc>='2019-01-23'
 AND new.call_end_date_utc>='2019-01-23'  
AND 
(old.call_inbound_key <> new.call_inbound_key
-- OR old.call_id <>new.call_id
-- OR old.call_start_date_utc  <>new.call_start_date_utc 
-- OR old.call_start_time_utc  <>new.call_start_time_utc 
-- OR old.call_end_time_utc    <>new.call_end_time_utc 
-- OR old.call_start_datetime_utc <>new.call_start_datetime_utc 
-- OR old.call_end_datetime_utc <>new.call_end_datetime_utc 
-- OR old.call_start_timestamp_utc <>new.call_start_timestamp_utc 
-- OR old.call_end_timestamp_utc <>new.call_end_timestamp_utc 
-- OR old.previous_call_time_utc <>new.previous_call_time_utc 
-- OR old.segment_number       <>new.segment_number 
-- OR old.segment_status_disposition <>new.segment_status_disposition 
--OR old.segment_start_time_utc <>new.segment_start_time_utc 
--OR old.segment_end_time_utc <>new.segment_end_time_utc 
-- OR old.segment_start_datetime_utc <>new.segment_start_datetime_utc 
-- OR old.segment_end_datetime_utc <>new.segment_end_datetime_utc 
-- OR old.segment_start_timestamp_utc <>new.segment_start_timestamp_utc 
--OR old.segment_end_timestamp_utc <>new.segment_end_timestamp_utc 
-- OR old.segment_duration_seconds <>new.segment_duration_seconds 
-- OR old.segment_duration_minutes <>new.segment_duration_minutes 
-- OR old.segment_handled_flag<>new.segment_handled_flag 
-- OR old.customer_call_count_indicator <>new.customer_call_count_indicator 
-- OR old.call_handled_flag   <>new.call_handled_flag 
-- OR old.call_owner           <>new.call_owner
-- OR old.product              <>new.product
-- OR old.account_number       <>new.account_number 
-- OR old.customer_account_number <>new.customer_account_number 
-- OR old.customer_type        <>new.customer_type 
-- OR old.customer_subtype     <>new.customer_subtype 
-- OR old.truck_roll_flag     <>new.truck_roll_flag 
-- OR old.notes_txt            <>new.notes_txt
-- OR old.resolution_description <>new.resolution_description 
-- OR old.cause_description    <>new.cause_description 
-- OR old.issue_description    <>new.issue_description 
-- OR old.company_code         <>new.company_code
-- OR old.service_call_tracker_id <>new.service_call_tracker_id 
-- OR old.created_on           <>new.created_on
-- OR old.created_by           <>new.created_by
-- OR old.phone_number_from_tracker <>new.phone_number_from_tracker 
-- OR old.call_type            <>new.call_type
-- OR old.split_sum_desc       <>new.split_sum_desc 
-- OR old.location_name        <>new.location_name 
-- OR old.care_center_management_name <>new.care_center_management_name 
-- OR old.agent_job_role_name  <>new.agent_job_role_name 
-- OR old.agent_effective_hire_date <>new.agent_effective_hire_date 
-- OR old.agent_mso            <>new.agent_mso
-- OR old.eduid                <>new.eduid
-- OR old.last_handled_segment_flag<>new.last_handled_segment_flag 
-- OR old.record_update_timestamp <>new.record_update_timestamp 
-- OR old.source               <>new.source
-- OR old.enhanced_account_number<>new.enhanced_account_number 
)
 limit 10
 ;	 	
