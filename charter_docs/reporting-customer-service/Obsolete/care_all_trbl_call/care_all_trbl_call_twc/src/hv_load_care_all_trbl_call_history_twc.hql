use ${env:ENVIRONMENT};

INSERT INTO care_all_trbl_call_history_twc PARTITION(report_date)
SELECT
    aes_encrypt256(AcctNum)                                                                            AS account__number_aes256,
    aes_encrypt256(Acct_Id)                                                                            AS account__id_aes256,
    OrdNum                                                                                             AS Order_Number,
    JobNum                                                                                             AS Job_NUmber,
    from_unixtime(unix_timestamp(EnterDttm,'yyyyMMdd'), 'yyyy-MM-dd')                                  AS Entry_DateTime,
    CASE
        WHEN ComplDttm = '0' THEN NULL
        ELSE from_unixtime(unix_timestamp(ComplDttm,'yyyyMMdd'), 'yyyy-MM-dd')
    END                                                                                                AS Completed_DateTime,
    CASE
        WHEN WOJobRsnCd = 'N/A' THEN NULL
        ELSE WOJobRsnCd
    END                                                                                                AS Work_Order_Reason_Cd,
    CASE
        WHEN WOJobRsnCdShortDesc = 'N/A' THEN NULL
        ELSE WOJobRsnCdShortDesc
    END                                                                                                AS Work_Order_Reason_Desc,
    CASE
        WHEN WOJobResCd = 'N/A' THEN NULL
        ELSE WOJobResCd
    END                                                                                                AS Work_Order_Resolution_Cd,
    CASE
        WHEN WOJobResCdShortDesc = 'N/A' THEN NULL
        ELSE WOJobResCdShortDesc
    END                                                                                                AS Work_Order_Resolution_Desc,
    WOJobTypeCd                                                                                        AS Work_Order_Job_Type_Cd,
    WOJobTypeDesc                                                                                      AS Work_Order_Job_Type_Desc,
    WOJobCatCd                                                                                         AS Work_Order_Category_Cd,
    WOJobClassCd                                                                                       AS Work_Order_Class_Cd,
    WOJobClassCatCd                                                                                    AS Work_Order_Class_Category_Cd,
    CASE
        WHEN WOInstallationCat = 'N/A' THEN NULL
        ELSE WOInstallationCat
    END                                                                                                AS Work_Order_Installation_Category,
    TrkRollFl                                                                                          AS TruckRoll_flag,
    CASE
        WHEN TrkRollRsnCdCat = 'N/A' THEN NULL
        ELSE TrkRollRsnCdCat
    END                                                                                                AS TruckRoll_Reason_Cd,
    CASE
        WHEN Technician = '0' THEN NULL
        WHEN Technician = 'N/A' THEN NULL
        ELSE Technician
    END                                                                                                AS Technician,
    WOJobStatCd                                                                                        AS Work_Order_Status_Cd,
    CASE
        WHEN ContractingFirmNm = 'N/A' THEN NULL
        ELSE ContractingFirmNm
    END                                                                                                 AS Contracting_Firm,
    CASE
        WHEN FirstNm = 'N/A' THEN NULL
        ELSE FirstNm
    END                                                                                                 AS Tech_First_Nm,
    CASE
        WHEN LastNm = 'N/A' THEN NULL
        ELSE LastNm
    END                                                                                                 AS Tech_Last_Nm,
    CASE
        WHEN BlgStnId = 'N/A' THEN NULL
        ELSE BlgStnId
    END                                                                                                 AS Blg_Stin_ID,
    CASE
        WHEN TechId = '0' THEN NULL
        WHEN TechID = 'N/A' THEN NULL
        ELSE TechId
    END                                                                                                 AS Technician_ID,
    KmaDesc                                                                                             AS KMA_Desc,
    from_unixtime(unix_timestamp(ExtractDateStartRange,'yyyyMMdd'), 'yyyy-MM-dd')                       AS Extract_Start,
    from_unixtime(unix_timestamp(ExtractDate,'MM/dd/yyyy'), 'yyyy-MM-dd')                               AS extract_date,
    reverse(split(reverse(INPUT__FILE__NAME), "/")[0])                                                  AS meta_source_filename,
    unix_timestamp()                                                                                    AS meta_load_timestamp,
    date_add(from_unixtime(unix_timestamp(ExtractDateStartRange,'yyyyMMdd'), 'yyyy-MM-dd'),7)           AS report_date
FROM ${env:TMP_db}.care_all_trbl_call_twc;

