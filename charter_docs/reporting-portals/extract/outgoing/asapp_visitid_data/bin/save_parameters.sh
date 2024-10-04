#!/bin/bash

echo "RUN_DATE: "    "${RUN_DATE}"
echo "VAR01: "    "${VAR01}"

echo "{\"RUN_DATE\" : \"$RUN_DATE\"," > ${JOB_OUTPUT_PROP_FILE}
echo "\"VAR01\" : \"$VAR01\"}" >> ${JOB_OUTPUT_PROP_FILE}

cat ${JOB_OUTPUT_PROP_FILE}
