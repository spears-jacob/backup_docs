#!/bin/sh
# this script takes the run_date value from the text file and passes it to Azkaban
# Now, the variable can be used in the next step of the job
if [ $# -ne 1 ]; then
    echo $0: Usage: rd.sh {Local working Folder}
    exit 1
fi

echo "This is the local FS working variable: $1 \n\n"

RD=$(cat $1run_date.text)

echo "Now setting Job Output Property File with RD (run_date) variable"
echo $RD
echo '{"RD" : "'"$RD"'"}' > $JOB_OUTPUT_PROP_FILE

rm $1run_date.text
