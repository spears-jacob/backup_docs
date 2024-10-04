CREATE EXTERNAL TABLE if not exists ${db_name}.quantum_dma_master(
    zip               string,
    city              string,
    county            string,
    state             string,
    dma               string,
    dma_name          string,
    fips_stcity       string,
    crossover_code    string,
    hhs               string,
    split_county_node string
)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
