use ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS care_all_trbl_call_twc
(
    account__number_aes256                  string,
    account__id_aes256                      string,
    Order_Number                            string,
    Job_Number                              string,
    Entry_DateTime                          string,
    Completed_DateTime                      string,
    Work_Order_Reason_Cd                    string,
    Work_Order_Reason_Desc                  string,
    Work_Order_Resolution_Cd                string,
    Work_Order_Resolution_Desc              string,
    Work_Order_Job_Type_Cd                  string,
    Work_Order_Job_Type_Desc                string,
    Work_Order_Category_Cd                  string,
    Work_Order_Class_Cd                     string,
    Work_Order_Class_Category_Cd            string,
    Work_Order_Installation_Category        string,
    TruckRoll_flag                          string,
    TruckRoll_Reason_Cd                     string,
    Technician                              string,
    Work_Order_Status_Cd                    string,
    Contracting_Firm                        string,
    Tech_First_Nm                           string,
    Tech_Last_Nm                            string,
    Blg_Stin_ID                             string,
    Technician_ID                           string,
    KMA_Desc                                string,
    Extract_Start                           string,
    Extract_Date                            string,
    meta_source_filename                    string,
    meta_load_timestamp                     bigint,
    row_num                                 int
)
PARTITIONED BY 
(
    partition_date                          string
)
STORED AS ORC
TBLPROPERTIES
(
    'ORC.COMPRESS'='SNAPPY',
    'ORC.CREATE.INDEX'='true'
)
;
