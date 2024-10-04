use ${env:ENVIRONMENT};

create table if not exists cs_qc (
data_date     string      COMMENT 'Date in the QCed table.',
data_desc         string      COMMENT 'If data is needed to understand QC check, it is described here.',
numeric_data      float       COMMENT 'If any type of numerical data is needed to understand QC check, it is included here.'
)

partitioned by (
qc_date           date        COMMENT 'Date that the QC job was executed.',
qc_table          string      COMMENT 'Table in prod being QCed.',
qc_check          string      COMMENT 'Description of QC check being performed.'
)

tblproperties ('retention_policy'='Aggregate - Without PII (3 Years)');
