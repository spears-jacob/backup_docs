USE [msdb]
GO

/****** Object:  Job [ops_analytics.etl.ImgP_jobs]    Script Date: 7/22/2024 1:45:10 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 7/22/2024 1:45:23 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'ops_analytics.etl.ImgP_jobs', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'[ops_analytics].[dbo].[etl.ImgP_WFHist]
ops_analytics.dbo.[etl.ImgP_WFHist_Queue_Order]
ops_analytics.dbo.[etl.ImgP_WFStats_Queue_Order]
ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined]
ops_analytics.dbo.[etl.WFHist_Bill_Queues]
etl.WFHist_Bill_Queues_Count
ops_analytics.dbo.[etl.ImgP_Before_Billing] (final table); etl.ImgP_IR_Percentage_by_Location', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'E-470\kbasse', 
		@notify_email_operator_name=N'OpsAnalyst', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Daily add to etl.ImgP_WFHist]    Script Date: 7/22/2024 1:45:32 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Daily add to etl.ImgP_WFHist', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'insert into [ops_analytics].[dbo].[etl.ImgP_WFHist] ----------> this is the correct section
select
	--cast(h.biWorkflowHistID as bigint) biWorkflowHistID
	h.biWorkflowID
	,h.vcQueue
	,h.dtUpdTime
	,GETDATE() Updated_Date
--into [ops_analytics].[dbo].[etl.ImgP_WFHist]
from [TCSReports].[dbo].[tbWorkflowHist] h (nolock)
where 
	h.vcQueue in (''qAuditRev'', ''qManDMVRev'', ''qOSMReview'', ''qSupCitRev'', ''qSupReview'', ''qV1Review''
		, ''qV1SenRev'', ''qV1Special'', ''qV2CMCRW'', ''qV2CReview'', ''qV2MCRW'', ''qV2Review'')--relevant IR queues in 2023
	and cast(h.dtUpdTime as date) >= dateadd(day,-1,cast(getdate() as date))', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate and load etl.ImgP_WFHist_Queue_Order]    Script Date: 7/22/2024 1:45:34 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate and load etl.ImgP_WFHist_Queue_Order', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'truncate table ops_analytics.dbo.[etl.ImgP_WFHist_Queue_Order];

insert into ops_analytics.dbo.[etl.ImgP_WFHist_Queue_Order]
select *
,ROW_NUMBER() over(partition by h.biworkflowid, h.vcQueue order by h.dtUpdTime) IR_Queue_Counter
--into ops_analytics.dbo.[etl.ImgP_WFHist_Queue_Order]
from [ops_analytics].[dbo].[etl.ImgP_WFHist] h (nolock)
where  1=1 ', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate and load etl.ImgP_WFStats_Queue_Order]    Script Date: 7/22/2024 1:45:38 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate and load etl.ImgP_WFStats_Queue_Order', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'truncate table ops_analytics.dbo.[etl.ImgP_WFStats_Queue_Order];

insert into ops_analytics.dbo.[etl.ImgP_WFStats_Queue_Order]

select w.biWorkFlowID, w.vcQueue, w.vcUserName, w.iSeconds, w.dtUpdTime, w.bPlateChanged, w.bSelImgChanged, w.bStateChanged
,ROW_NUMBER() over(partition by w.biworkflowid, w.vcQueue order by w.dtUpdTime) IR_Queue_Counter
,GETDATE() Updated_Date
--into ops_analytics.dbo.[etl.ImgP_WFStats_Queue_Order]
from TCSReports.dbo.tbWorkFlowStats w (nolock)
where 1=1 and w.vcusername not in (''SYSTEM'') --and biWorkflowID in (1780795285,1819285076)
and w.dtUpdTime>=''2023-01-01''', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate and load etl.ImgP_WFHistStats_Combined]    Script Date: 7/22/2024 1:45:38 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate and load etl.ImgP_WFHistStats_Combined', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'truncate table ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined];
insert into ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined]
select 
h.biWorkflowID, h.vcQueue, q.vcQueueDesc , h.dtUpdTime Queue_Load_UpdTime, h.IR_Queue_Counter, w.vcUserName, w.iSeconds, w.dtUpdTime Image_Review_UpdTime, w.bPlateChanged, w.bSelImgChanged, w.bStateChanged
,row_number() over(partition by h.biworkflowid order by h.dtupdtime) IR_Queue_Counter_Overall
--into ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined]
from ops_analytics.dbo.[etl.ImgP_WFHist_Queue_Order] h (nolock)
join TCSReports.dbo.stbWorkflowQueue q (nolock) on h.vcQueue=q.vcQueue
left join  ops_analytics.dbo.[etl.ImgP_WFStats_Queue_Order] w (nolock) on h.biworkflowid=w.biworkflowid and h.vcqueue=w.vcqueue and h.IR_Queue_Counter=w.IR_Queue_Counter
where  1=1', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Daily add to etl.WFHist_Bill_Queues]    Script Date: 7/22/2024 1:45:39 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Daily add to etl.WFHist_Bill_Queues', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'insert into ops_analytics.dbo.[etl.WFHist_Bill_Queues]
select biWorkflowID
,vcQueue
,dtUpdtime 
,getdate() Updated_Date
--into ops_analytics.dbo.[etl.WFHist_Bill_Queues]
from [TCSReports].[dbo].[tbWorkflowHist] (nolock) 
where 1=1 
	and cast(dtUpdTime as date) >= dateadd(day,-1,cast(getdate() as date))
	--and cast(dtUpdTime as date) between ''2023-09-13'' and ''2023-09-17''
	and vcQueue in (''qBillableV'', ''qBilledV'', ''qvtoll'',''qRjctViols'',''qStNotProc'',''qCompVioln'')', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate and load etl.WFHist_Bill_Queues_Count]    Script Date: 7/22/2024 1:45:39 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate and load etl.WFHist_Bill_Queues_Count', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'truncate table ops_analytics.dbo.[etl.WFHist_Bill_Queues_Count];

insert into ops_analytics.dbo.[etl.WFHist_Bill_Queues_Count]
select 
biWorkflowID
,vcQueue
,dtUpdtime 
,ROW_NUMBER() over(partition by biworkflowid, vcqueue order by dtUpdtime) Bill_Queue_Order 
,Updated_Date
from ops_analytics.dbo.[etl.WFHist_Bill_Queues] (nolock)', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate and load etl.ImgP_Before_Billing]    Script Date: 7/22/2024 1:45:40 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate and load etl.ImgP_Before_Billing', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'truncate table ops_analytics.dbo.[etl.ImgP_Before_Billing];

insert into ops_analytics.dbo.[etl.ImgP_Before_Billing]
SELECT
t.[biWorkflowID] WorkflowID
,t.dtMsgDate Transaction_Date

--location details
,loc.vcRevAgency Rev_Agency
,loc.vcRoadway Roadway
,loc.vcLocationName Location_Name
,la.cLaneDirection Lane_Direction
,case when t.vcOrigin=''O'' then ''E470'' when t.vcOrigin=''E'' then ''ETC'' end Lane_System

--Bring in from image hdr table
,i.tiPlateConfidence Plate_Confidence
,i.tiStateConfidence State_Confidence
,i.tiETCCOverallImageConf ETCC_Overall_Image_Conf
,coalesce(i.vcFrontPlateNum,i.vcPlateNum) Plate_Num
,coalesce(i.vcFrontPlateState,i.vcPlateState) Plate_State

--image 1-4
,im.vcQueue First_Image_Queue
,im.vcQueueDesc First_Image_queue_desc
,im.vcUserName First_Image_Username
,im.image_review_updtime First_Image_Review_Date
,case when im.bPlatechanged=1 or im.bstatechanged=1 then 1 else 0 end First_Image_Changed_Flag

,im2.vcQueue Second_Image_Queue
,im2.vcQueueDesc Second_Image_Queue_Desc
,im2.vcUserName Second_Image_Username
,im2.image_review_updtime Second_Image_Review_Date
,case when im2.bPlatechanged=1 or im2.bstatechanged=1 then 1 else 0 end Second_Image_Changed_Flag

,im3.vcQueue Third_Image_Queue
,im3.vcQueueDesc Third_Image_Queue_Desc
,im3.vcUserName Third_Image_Username
,im3.image_review_updtime Third_Image_Review_Date
,case when im3.bPlatechanged=1 or im3.bstatechanged=1 then 1 else 0 end Third_Image_Changed_Flag

,im4.vcQueue Fourth_Image_Queue
,im4.vcQueueDesc Fourth_Image_Queue_Desc
,im4.vcUserName Fourth_Image_Username
,im4.image_review_updtime Fourth_Image_Review_Date
,case when im4.bPlatechanged=1 or im4.bstatechanged=1 then 1 else 0 end Fourth_Image_Changed_Flag

,iq.Total_IR_Queues --if this exceeds the # of image reviews I''m including (right now 4), its because there were additional reviews after the billed date

,sr.image_review_updtime Sensitive_Review_Date
,case when sr.biWorkflowID is not null then 1 else 0 end Sensitive_Review_Flag
,sp.image_review_updtime Special_Review_Date
,case when sp.biWorkflowID is not null then 1 else 0 end Special_Review_Flag

,case 
	when cc.Type_of_First_Bill <>''Vtoll'' then ''No VToll''
	when im.image_review_updtime is null and cc.Type_of_First_Bill=''Vtoll'' then ''No Image Review, VTolled''
	when im.image_review_updtime< cc.First_Vtoll_Date and cc.Type_of_First_Bill=''Vtoll''  then ''First Image Review Before VToll''
	when im.image_review_updtime> cc.First_Vtoll_Date and cc.Type_of_First_Bill=''Vtoll'' then ''First Image Review After VToll''
	end VToll_Timing_Review
--,case 
--		when cc.First_Vtoll_Date is null then ''No VToll''
--		when im.image_review_updtime is null and cc.First_Vtoll_Date is not null then ''No Image Review, VTolled''
--		when im.image_review_updtime< cc.First_Vtoll_Date then ''First Image Review Before VToll''
--		when im.image_review_updtime> cc.First_Vtoll_Date then ''First Image Review After VToll''
--	end VToll_Timing_Review1
,cc.First_Vtoll_Date
,cc.First_Billable_Date
,cc.First_Billed_Date
,case when cc.First_Rejected_Date is not null then cc.First_Rejected_Date 
	when cc.First_LES_No_Process_Date is not null then  cc.First_LES_No_Process_Date 
	when cc.First_Comp_LPT_Trxn_Date<cc.First_Billable_Date then cc.First_Comp_LPT_Trxn_Date
	when cc.First_Comp_LPT_Trxn_Date<cc.First_Billed_Date then cc.First_Comp_LPT_Trxn_Date
	end First_Non_Billed
,cc.First_Comp_LPT_Trxn_Date
,cc.Type_of_First_Bill --NEW 10/10
,cc.Date_of_First_Bill --NEW 10/10
, case when cc.First_Billable_Date is null and cc.First_Billed_Date is null and cc.First_Vtoll_Date is null 
		and cc.First_Rejected_Date is null and cc.First_LES_No_Process_Date is null and cc.First_Comp_LPT_Trxn_Date is null then 1 else 0
end In_Initial_Image_Review_Flag
, case 
	when cc.First_Vtoll_Date is not null and (cc.First_Billable_Date is not null or cc.First_Billed_Date is not null) then 1 
	when cc.First_Rejected_Date>cc.First_Billable_Date then 1
	when cc.First_Rejected_Date>cc.First_Billed_Date then 1
	when cc.First_Rejected_Date>cc.First_Vtoll_Date then 1
	else 0 
end Post_IR_Accuracy_Issue_Flag
,getdate() Updated_Date
--into ops_analytics.dbo.[etl.ImgP_Before_Billing]
FROM TCSReports.dbo.tbTollTrxnHdr t (nolock) 
JOIN [TCSReports].[dbo].[stbLocation] loc (nolock) ON t.siLocationID=loc.siLocationID
JOIN [TCSReports].[dbo].[stbLane] la (nolock) on t.tiLaneID = la.tiLaneID
--Bring in original plate info and confidence
left join TCSReports.dbo.tbImageHdr i (nolock) on t.biWorkFlowID = i.biWorkFlowID and i.dtUpdTime>=''2023-01-01''

--First billable, billed, vtolled (doesn''t use prior logic of ''qCompVioln'' as secondary way to look at time for billed)
left join ops_analytics.dbo.WFHist_Bill_Queues_Count_Min_v cc (nolock) on t.biWorkFlowID=cc.biworkflowid 

--first Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im on t.biWorkFlowID=im.biWorkflowID and im.IR_Queue_Counter_Overall=1 
	and (im.image_review_updtime <cc.First_Billed_Date or im.image_review_updtime<cc.First_Billable_Date or im.Image_Review_UpdTime<cc.First_Vtoll_Date or im.Image_Review_UpdTime<cc.First_Rejected_Date 
		or im.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

--second Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im2 on t.biWorkFlowID=im2.biWorkflowID and im2.IR_Queue_Counter_Overall=2 
	and (im2.image_review_updtime <cc.First_Billed_Date or im2.image_review_updtime<cc.First_Billable_Date or im2.Image_Review_UpdTime<cc.First_Vtoll_Date or im2.Image_Review_UpdTime<cc.First_Rejected_Date
	    or im2.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im2.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

----third Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im3 on t.biWorkFlowID=im3.biWorkflowID and im3.IR_Queue_Counter_Overall=3
	and (im3.image_review_updtime <cc.First_Billed_Date or im3.image_review_updtime<cc.First_Billable_Date or im3.Image_Review_UpdTime<cc.First_Vtoll_Date or im3.Image_Review_UpdTime<cc.First_Rejected_Date
		or im3.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im3.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

--fourth Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im4 on t.biWorkFlowID=im4.biWorkflowID and im4.IR_Queue_Counter_Overall=4
	and (im4.image_review_updtime <cc.First_Billed_Date or im4.image_review_updtime<cc.First_Billable_Date or im4.Image_Review_UpdTime<cc.First_Vtoll_Date or im4.Image_Review_UpdTime<cc.First_Rejected_Date
		or im4.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im4.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

---Total Image Reviews
left join Ops_Analytics.dbo.ImgP_Queue_Totals_v iq on t.biWorkFlowID=iq.biworkflowid

--sensitive review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) sr on t.biWorkFlowID=sr.biWorkflowID and sr.vcQueue=''qV1SenRev''
		
--special review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) sp on t.biWorkFlowID=sp.biWorkflowID and sp.vcQueue in (''qV1MCRWSPL'',''qV1Special'') 
		

where 1=1
and t.siMsgType =350 --LPT trxn code
AND (t.siMsgFilterCode is null or t.siMsgFilterCode in (5,17,31,32,34,35)) --Full LPT logic
and cast(t.dtMsgDate as date) between ''2023-01-01'' and dateadd(day,-1,cast(getdate() as date))

/* V1
insert into ops_analytics.dbo.[etl.ImgP_Before_Billing]
SELECT 
t.[biWorkflowID] WorkflowID
,t.dtMsgDate Transaction_Date

--location details
,loc.vcRevAgency Rev_Agency
,loc.vcRoadway Roadway
,loc.vcLocationName Location_Name
,la.cLaneDirection Lane_Direction
,case when t.vcOrigin=''O'' then ''E470'' when t.vcOrigin=''E'' then ''ETC'' end Lane_System

--Bring in from image hdr table
,i.tiPlateConfidence Plate_Confidence
,i.tiStateConfidence State_Confidence
,i.tiETCCOverallImageConf ETCC_Overall_Image_Conf
,coalesce(i.vcFrontPlateNum,i.vcPlateNum) Plate_Num
,coalesce(i.vcFrontPlateState,i.vcPlateState) Plate_State

--image 1-4
,im.vcQueue First_Image_Queue
,im.vcQueueDesc First_Image_queue_desc
,im.vcUserName First_Image_Username
,im.image_review_updtime First_Image_Review_Date
,case when im.bPlatechanged=1 or im.bstatechanged=1 then 1 else 0 end First_Image_Changed_Flag

,im2.vcQueue Second_Image_Queue
,im2.vcQueueDesc Second_Image_Queue_Desc
,im2.vcUserName Second_Image_Username
,im2.image_review_updtime Second_Image_Review_Date
,case when im2.bPlatechanged=1 or im2.bstatechanged=1 then 1 else 0 end Second_Image_Changed_Flag

,im3.vcQueue Third_Image_Queue
,im3.vcQueueDesc Third_Image_Queue_Desc
,im3.vcUserName Third_Image_Username
,im3.image_review_updtime Third_Image_Review_Date
,case when im3.bPlatechanged=1 or im3.bstatechanged=1 then 1 else 0 end Third_Image_Changed_Flag

,im4.vcQueue Fourth_Image_Queue
,im4.vcQueueDesc Fourth_Image_Queue_Desc
,im4.vcUserName Fourth_Image_Username
,im4.image_review_updtime Fourth_Image_Review_Date
,case when im4.bPlatechanged=1 or im4.bstatechanged=1 then 1 else 0 end Fourth_Image_Changed_Flag

,iq.Total_IR_Queues --if this exceeds the # of image reviews I''m including (right now 4), its because there were additional reviews after the billed date

,sr.image_review_updtime Sensitive_Review_Date
,case when sr.biWorkflowID is not null then 1 else 0 end Sensitive_Review_Flag
,sp.image_review_updtime Special_Review_Date
,case when sp.biWorkflowID is not null then 1 else 0 end Special_Review_Flag
,case 
		when cc.First_Vtoll_Date is null then ''No VToll''
		when im.image_review_updtime is null and cc.First_Vtoll_Date is not null then ''No Image Review, VTolled''
		when im.image_review_updtime< cc.First_Vtoll_Date then ''First Image Review Before VToll''
		when im.image_review_updtime> cc.First_Vtoll_Date then ''First Image Review After VToll''
	end VToll_Timing_Review
,cc.First_Vtoll_Date
,cc.First_Billable_Date
,cc.First_Billed_Date
,case when cc.First_Rejected_Date is not null then cc.First_Rejected_Date 
	when cc.First_LES_No_Process_Date is not null then  cc.First_LES_No_Process_Date 
	when cc.First_Comp_LPT_Trxn_Date<cc.First_Billable_Date then cc.First_Comp_LPT_Trxn_Date
	when cc.First_Comp_LPT_Trxn_Date<cc.First_Billed_Date then cc.First_Comp_LPT_Trxn_Date
	end First_Non_Billed
,cc.First_Comp_LPT_Trxn_Date
, case when cc.First_Billable_Date is null and cc.First_Billed_Date is null and cc.First_Vtoll_Date is null 
		and cc.First_Rejected_Date is null and cc.First_LES_No_Process_Date is null and cc.First_Comp_LPT_Trxn_Date is null then 1 else 0
end In_Initial_Image_Review_Flag
, case 
	when cc.First_Vtoll_Date is not null and (cc.First_Billable_Date is not null or cc.First_Billed_Date is not null) then 1 
	when cc.First_Rejected_Date>cc.First_Billable_Date then 1
	when cc.First_Rejected_Date>cc.First_Billed_Date then 1
	when cc.First_Rejected_Date>cc.First_Vtoll_Date then 1
	else 0 
end Post_IR_Accuracy_Issue_Flag
,getdate() Updated_Date
--into ops_analytics.dbo.[etl.ImgP_Before_Billing]
FROM TCSReports.dbo.tbTollTrxnHdr t (nolock) 
JOIN [TCSReports].[dbo].[stbLocation] loc (nolock) ON t.tiLocationID=loc.tiLocationID
JOIN [TCSReports].[dbo].[stbLane] la (nolock) on t.tiLaneID = la.tiLaneID
--Bring in original plate info and confidence
left join TCSReports.dbo.tbImageHdr i (nolock) on t.biWorkFlowID = i.biWorkFlowID and i.dtUpdTime>=''2023-01-01''

--First billable, billed, vtolled (doesn''t use prior logic of ''qCompVioln'' as secondary way to look at time for billed)
left join ops_analytics.dbo.WFHist_Bill_Queues_Count_Min_v cc (nolock) on t.biWorkFlowID=cc.biworkflowid 

--first Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im on t.biWorkFlowID=im.biWorkflowID and im.IR_Queue_Counter_Overall=1 
	and (im.image_review_updtime <cc.First_Billed_Date or im.image_review_updtime<cc.First_Billable_Date or im.Image_Review_UpdTime<cc.First_Vtoll_Date or im.Image_Review_UpdTime<cc.First_Rejected_Date 
		or im.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

--second Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im2 on t.biWorkFlowID=im2.biWorkflowID and im2.IR_Queue_Counter_Overall=2 
	and (im2.image_review_updtime <cc.First_Billed_Date or im2.image_review_updtime<cc.First_Billable_Date or im2.Image_Review_UpdTime<cc.First_Vtoll_Date or im2.Image_Review_UpdTime<cc.First_Rejected_Date
	    or im2.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im2.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

----third Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im3 on t.biWorkFlowID=im3.biWorkflowID and im3.IR_Queue_Counter_Overall=3
	and (im3.image_review_updtime <cc.First_Billed_Date or im3.image_review_updtime<cc.First_Billable_Date or im3.Image_Review_UpdTime<cc.First_Vtoll_Date or im3.Image_Review_UpdTime<cc.First_Rejected_Date
		or im3.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im3.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

--fourth Image Review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) im4 on t.biWorkFlowID=im4.biWorkflowID and im4.IR_Queue_Counter_Overall=4
	and (im4.image_review_updtime <cc.First_Billed_Date or im4.image_review_updtime<cc.First_Billable_Date or im4.Image_Review_UpdTime<cc.First_Vtoll_Date or im4.Image_Review_UpdTime<cc.First_Rejected_Date
		or im4.Image_Review_UpdTime<cc.First_LES_No_Process_Date or im4.Image_Review_UpdTime<cc.First_Comp_LPT_Trxn_Date)

---Total Image Reviews
left join Ops_Analytics.dbo.ImgP_Queue_Totals_v iq on t.biWorkFlowID=iq.biworkflowid

--sensitive review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) sr on t.biWorkFlowID=sr.biWorkflowID and sr.vcQueue=''qV1SenRev''
		
--special review
left join ops_analytics.dbo.[etl.ImgP_WFHistStats_Combined] (nolock) sp on t.biWorkFlowID=sp.biWorkflowID and sp.vcQueue in (''qV1MCRWSPL'',''qV1Special'') 
		

where 1=1
and t.siMsgType =350 --LPT trxn code
AND (t.siMsgFilterCode is null or t.siMsgFilterCode in (5,17,31,32,34,35)) --Full LPT logic
and cast(t.dtMsgDate as date) between ''2023-01-01'' and dateadd(day,-1,cast(getdate() as date))
*/', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate and load etl.ImgP_IR_Percentage_by_Location]    Script Date: 7/22/2024 1:45:40 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate and load etl.ImgP_IR_Percentage_by_Location', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'truncate table ops_analytics.dbo.[etl.ImgP_IR_Percentage_by_Location];
insert into ops_analytics.dbo.[etl.ImgP_IR_Percentage_by_Location]

select x.*
,sum(transaction_count) over(partition by transaction_date_end_of_month, rev_agency, roadway, location_name, lane_direction, commissioned_date, lane_system) Location_Sum
,cast(transaction_count as float)/sum(transaction_count) over(partition by transaction_date_end_of_month, rev_agency, roadway, location_name, lane_direction, commissioned_date, lane_system) Location_Percentage
,getdate() Updated_Date
--into ops_analytics.dbo.[etl.ImgP_IR_Percentage_by_Location]
from(
SELECT --top 1000
eomonth(transaction_date) Transaction_Date_End_of_Month
,[Rev_Agency]
,[Roadway]
,b.[Location_Name]
,b.[Lane_Direction]
,c.Commissioned_Date
,[Lane_System]
,case 
	when Total_IR_Queues>4 then ''5+'' 
	when Total_IR_Queues is null then ''0'' 
	else cast(Total_IR_Queues as varchar(3)) 
end Total_IR_Queues_Category

, count([WorkflowID]) Transaction_Count
--, count(*) over(partition by eomonth(transaction_date),[Rev_Agency],[Roadway],b.[Location_Name],b.[Lane_Direction],c.Commissioned_Date,[Lane_System]) Location_Sum
FROM [Ops_Analytics].[dbo].[etl.ImgP_Before_Billing] b (nolock)
left join [Ops_Analytics].[dbo].[Tolling_Lane_Commissioning] c (nolock) on b.Location_Name=c.Location_name and b.Lane_Direction=c.Lane_Direction 

where 1=1
and In_Initial_Image_Review_Flag=0
--and b.Location_Name=''6th pkwy''
--and b.Lane_Direction=''N''
--and eomonth(transaction_date)=''2023-09-30''

group by
eomonth(transaction_date) 
,[Rev_Agency]
,[Roadway]
,b.[Location_Name]
,b.[Lane_Direction]
,c.Commissioned_Date
,[Lane_System]
,case 
	when Total_IR_Queues>4 then ''5+'' 
	when Total_IR_Queues is null then ''0'' 
	else cast(Total_IR_Queues as varchar(3)) 
end 

) x', 
		@database_name=N'Ops_Analytics', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [truncate and load Ops_Analytics.dbo.[etl.ImgP_Action_Last30Days]]    Script Date: 7/22/2024 1:45:41 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'truncate and load Ops_Analytics.dbo.[etl.ImgP_Action_Last30Days]', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'truncate table Ops_Analytics.dbo.[etl.ImgP_Action_Last30Days];
insert into Ops_Analytics.dbo.[etl.ImgP_Action_Last30Days]

SELECT 
COALESCE(wl.dtEndTime,wl.dtUpdTime) Review_Date
, wl.vcProcessName Queue_Code
,q.vcQueueDesc Queue_Name
,case 
	when wl.vcUpdUser like ''CCI%'' then ''CCI''
	when wl.vcUpdUser not like ''CCI%'' and wl.vcUpdUser<>''SmartRev'' then ''WSP''
	when wl.vcUpdUser=''SmartRev'' then ''SmartRev''
end Reviewer_Type
 ,wl.vcUpdUser Reviewer_Username
, u.[Company]
      ,u.[Organization]
      ,u.[Department]
      ,u.[Level]
      ,u.[Title]
      ,u.[Full_Name]
,lptv.[vcActionDone] Action_Done
,count(wl.biworkflowid) Review_Count
,GETDATE() Updated_Date
--into Ops_Analytics.dbo.[etl.ImgP_Action_Last30Days]
from [TCSReports].[dbo].[tbWorkflowLPT]  wl (nolock)
left join [TCSReports].[dbo].[tbWorkflowLPTImgRev] lptv (nolock) on wl.biLPTWorkflowID=LPTV.biLPTWorkflowID 
join TCSReports.dbo.stbWorkflowQueue q (nolock) on wl.vcProcessName=q.vcQueue and q.vcQueueType=''IMGREVIEW''
left join [Ops_Analytics].[dbo].[ref_universal_workforce_dim] u (nolock) on wl.vcUpdUser=u.TCS_Username1
where 1=1
--and wl.vcProcessName in(''qV1Review'',''qV2Review'', ''qV2MCRW'')
and CAST(COALESCE(wl.dtEndTime,wl.dtUpdTime) as date) between dateadd(day,-31,cast(getdate() as date)) and getdate() -- @startdate and @enddate --''2023-10-15'' and ''2023-11-14'' --''2023-09-15'' and ''2023-10-14''
--and wl.vcUpdUser like ''CCI%''

group by
COALESCE(wl.dtEndTime,wl.dtUpdTime) 
,wl.vcProcessName
,q.vcQueueDesc
,case 
	when wl.vcUpdUser like ''CCI%'' then ''CCI''
	when wl.vcUpdUser not like ''CCI%'' and wl.vcUpdUser<>''SmartRev'' then ''WSP''
	when wl.vcUpdUser=''SmartRev'' then ''SmartRev''
end
 ,wl.vcUpdUser
 , u.[Company]
      ,u.[Organization]
      ,u.[Department]
      ,u.[Level]
      ,u.[Title]
      ,u.[Full_Name]
,lptv.[vcActionDone]', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'daily', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20230918, 
		@active_end_date=99991231, 
		@active_start_time=50000, 
		@active_end_time=235959, 
		@schedule_uid=N'd5790bc7-ab5b-4cb3-bd93-11159fe5b3e3'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

