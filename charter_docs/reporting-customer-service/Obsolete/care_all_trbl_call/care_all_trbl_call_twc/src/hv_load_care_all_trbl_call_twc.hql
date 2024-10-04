use ${env:ENVIRONMENT};

INSERT INTO care_all_trbl_call_twc PARTITION(partition_date)
SELECT *
FROM
    (SELECT
        account__number_aes256,
        account__id_aes256,
        Order_Number,
        Job_Number,
        Entry_DateTime,
        Completed_DateTime,
        Work_Order_Reason_Cd,
        Work_Order_Reason_Desc,
        Work_Order_Resolution_Cd,
        Work_Order_Resolution_Desc,
        Work_Order_Job_Type_Cd,
        Work_Order_Job_Type_Desc,
        Work_Order_Category_Cd,
        Work_Order_Class_Cd,
        Work_Order_Class_Category_Cd,
        Work_Order_Installation_Category,
        TruckRoll_flag,
        TruckRoll_Reason_Cd,
        Technician,
        Work_Order_Status_Cd,
        Contracting_Firm,
        Tech_First_Nm,
        Tech_Last_Nm,
        Blg_Stin_ID,
        Technician_ID,
        KMA_Desc,
        Extract_Start,
        Extract_Date,
        meta_source_filename,
        meta_load_timestamp,
        Row_Number() OVER (PARTITION BY Order_Number ORDER BY report_date DESC) as row_num,
        Entry_DateTime AS partition_date
    FROM care_all_trbl_call_history_twc
    WHERE report_date > '${hiveconf:part_date_twc}'
        and Entry_DateTime >= '${hiveconf:part_date_twc}')A
WHERE row_num = 1
;
