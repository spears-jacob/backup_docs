#!/bin/bash

# trims last line off of JOB_OUTPUT_PROP_FILE, which is the closing curly bracket
sed -i '$d' "${JOB_OUTPUT_PROP_FILE}"

numDynPartitions=$(hive -e "SET hive.exec.max.dynamic.partitions.pernode;");
export numDynPartitions="${numDynPartitions/hive.exec.max.dynamic.partitions.pernode=/}";
echo $numDynPartitions


echo ',"NUMDYNPARTITIONS": "'"$numDynPartitions"'"
      }' >> "${JOB_OUTPUT_PROP_FILE}"

cat "${JOB_OUTPUT_PROP_FILE}"
