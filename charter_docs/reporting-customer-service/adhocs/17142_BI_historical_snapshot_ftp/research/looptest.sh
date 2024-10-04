START_DATE="2019-12-01"
END_DATE="2019-01-01"

START_DATE_SECS=$(date -d "${START_DATE}" +%s)
END_DATE_SECS=$(date -d "${END_DATE}" +%s)

while [[ ${START_DATE_SECS} -gt ${END_DATE_SECS} ]]; do 
   echo "setting start day -90"
   START_DATE=$(date -d "`date +"%Y-%m-%d" --date="${START_DATE}"` -90 day" +%Y-%m-%d)
   echo "90 days back: ${START_DATE}"
   START_DATE_SECS=$(date -d "${START_DATE}" +%s)
done
