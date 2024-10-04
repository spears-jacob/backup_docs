set hive.vectorized.execution.enabled = false;

USE {queryEnv};

DROP TABLE IF EXISTS {columnsTable};
CREATE TEMPORARY TABLE {columnsTable} AS
    SELECT
        {company} as company,
