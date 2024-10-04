use ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.care_all_trbl_call_twc
(
    AcctNum                                 string,
    Acct_Id                                 string,
    OrdNum                                  string,
    JobNum                                  string,
    EnterDttm                               string,
    ComplDttm                               string,
    WOJobRsnCd                              string,
    WOJobRsnCdShortDesc                     string,
    WOJobResCd                              string,
    WOJobResCdShortDesc                     string,
    WOJobTypeCd                             string,
    WOJobTypeDesc                           string,
    WOJobCatCd                              string,
    WOJobClassCd                            string,
    WOJobClassCatCd                         string,
    WOInstallationCat                       string,
    TrkRollFl                               string,
    TrkRollRsnCdCat                         string,
    Technician                              string,
    WOJobStatCd                             string,
    ContractingFirmNm                       string,
    FirstNm                                 string,
    LastNm                                  string,
    BlgStnId                                string,
    TechId                                  string,
    KmaDesc                                 string,
    ExtractDateStartRange                   string,
    ExtractDate                             string
)
PARTITIONED BY
(
    partition_date                          string,
    partition_hour                          string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'serialization.null.format'='');
