CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_video_manifest
(
    level_1    INT,
    level_2    INT,
    level_3    INT,
    level_4    INT,
    level_5    INT,
    level_6    INT,
    level_7    INT,
    level_8    INT,
    video_type STRING,
    date_start STRING,
    date_end   STRING
)

STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress" = "SNAPPY");
