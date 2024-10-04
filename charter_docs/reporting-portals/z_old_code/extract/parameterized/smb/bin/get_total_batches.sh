#!/bin/bash

# trims last line off of JOB_OUTPUT_PROP_FILE, which is the closing curly bracket
sed -i '$d' "${JOB_OUTPUT_PROP_FILE}"


export batchTable=$1

echo "

The batch table is the following:  $batchTable

Now selecting the maximum batch number from $batchTable;

hive -e \"SELECT MAX(insert_batch_number) from ${batchTable} ;\"

"

 TOTALBATCHES=$(hive -e "SELECT MAX(insert_batch_number) from $1 ;");
#TOTALBATCHES=1


echo ',"TOTALBATCHES": "'"$TOTALBATCHES"'"
      }' >> "${JOB_OUTPUT_PROP_FILE}"


cat "${JOB_OUTPUT_PROP_FILE}"
