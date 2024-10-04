#!/bin/bash

# This script downloads a png image of a Tableau report view, encodes it into a
# long string of characters (base64), chops up the long string into managable bites,
# places it into a predefined E-mail message template, replaces some E-mail headers,
# adds some requisite characters and boundaries to make it work, and sends the E-mail.

# Please keep in mind that even if an E-mail list is blank, it must include "''" as the value,
# otherwise the input variables do not properly match the order in which they are passed.

# 0. - Prepare a few variables
export tableau_workbook=${5}
export tableau_first_view=${6}

export email_FROM=${15}
export email_SUBJECT=${16}
export email_BOUNDARY=${17}
export email_NOTIFY=${18}

export report_table=${22}
export report_table_counted_field=${23}
export report_table_date_field=${24}
export report_table_criterion1=${25}

# 1. - First check that data exists before proceeding

echo "### Loading run date $RUN_DATE ###"

if [ "$RUN_DATE" != "" ]; then
  data_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`

  echo "### Looking for $data_date data using the following query: ###

  SELECT COUNT($report_table_counted_field)
  FROM $report_table
  WHERE $report_table_date_field = '$data_date'
  AND domain='$report_table_criterion1';

  "
  data_cnt=$(hive -e "set hive.cli.print.header=false; USE $ENVIRONMENT; SELECT COUNT($report_table_counted_field) FROM $report_table WHERE $report_table_date_field = '$data_date' AND domain='$report_table_criterion1';");

  echo " --> Row count for $data_date: ${data_cnt[0]}"

  if [ ${data_cnt[0]} -eq 0 ]; then
    echo "### ERROR: No data available for run date $RUN_DATE ($data_date data) in $report_table in the $report_table_date_field field"
    echo "$tableau_workbook/${tableau_first_view} had an issue regarding current data for report preparation
          ### ERROR: No data available for run date $RUN_DATE ($data_date data) in $report_table in the $report_table_date_field field
         " | mailx -s "E-Mail Prep / Data missing - $tableau_workbook/${tableau_first_view}" -r "$email_FROM" $email_NOTIFY
  fi

else
  echo "### ERROR: Run date ($RUN_DATE) value not available" && exit 1
fi

# Bypass all the E-mail prep if no data
if [ ${data_cnt[0]} -gt 0 ]; then

  # 2. - Output export statement, set up, and echo all variables passed in from project.properties


  echo "
           ‡‡‡‡‡‡‡ Send PNG from Tableau embedded in an E-mail started ‡‡‡‡‡‡‡

           ‡‡‡‡‡‡‡ See all values for properties below.  Copy and paste them locally for use debugging ‡‡‡‡‡‡‡

           export tableaupass_aes128=${1}
           export tableau_workbook_no=${2}
           export tableau_output_filename=${3}
           export tableau_project=${4}
           export tableau_workbook=${5}
           export tableau_first_view=${6}
           export tableau_width=${7}
           export tableau_height=${8}
           export email_TO_list_comma=${9}
           export email_CC_list_comma=${10}
           export email_BCC_list_comma=${11}
           export email_DEBUG_TO_list_comma=${12}
           export email_DEBUG_CC_list_comma=${13}
           export email_DEBUG_BCC_list_comma=${14}
           export email_FROM=${15}
           export email_SUBJECT=${16}
           export email_BOUNDARY=${17}
           export email_NOTIFY=${18}
           export email_TEMPLATE=${19}
           export email_POPULATED=${20}
           export email_FOLLOWUP_NOTE=${21}
           export report_table=${22}
           export report_table_counted_field=${23}
           export report_table_date_field=${24}
           export report_table_criterion1=${25}
           export IsDebuggingEnabled=${26}

  "

  export tableaupass_aes128=${1}
  export tableau_workbook_no=${2}
  export tableau_output_filename=${3}
  export tableau_project=${4}
  export tableau_workbook=${5}
  export tableau_first_view=${6}
  export tableau_width=${7}
  export tableau_height=${8}
  export email_TO_list_comma=${9}
  export email_CC_list_comma=${10}
  export email_BCC_list_comma=${11}
  export email_DEBUG_TO_list_comma=${12}
  export email_DEBUG_CC_list_comma=${13}
  export email_DEBUG_BCC_list_comma=${14}
  export email_FROM=${15}
  export email_SUBJECT=${16}
  export email_BOUNDARY=${17}
  export email_NOTIFY=${18}
  export email_TEMPLATE=${19}
  export email_POPULATED=${20}
  export email_FOLLOWUP_NOTE=${21}
  export report_table=${22}
  export report_table_counted_field=${23}
  export report_table_date_field=${24}
  export report_table_criterion1=${25}
  export IsDebuggingEnabled=${26}

  echo "
          ‡‡‡‡‡‡‡ See the current values for local environment variables below. ‡‡‡‡‡‡‡

  "
  echo tableaupass_aes128=$tableaupass_aes128
  echo tableau_workbook_no=$tableau_workbook_no
  echo tableau_output_filename=$tableau_output_filename
  echo tableau_project=$tableau_project
  echo tableau_workbook=$tableau_workbook
  echo tableau_first_view=$tableau_first_view
  echo tableau_width=$tableau_width
  echo tableau_height=$tableau_height
  echo email_TO_list_comma=$email_TO_list_comma
  echo email_CC_list_comma=$email_CC_list_comma
  echo email_BCC_list_comma=$email_BCC_list_comma
  echo email_DEBUG_TO_list_comma=$email_DEBUG_TO_list_comma
  echo email_DEBUG_CC_list_comma=$email_DEBUG_CC_list_comma
  echo email_DEBUG_BCC_list_comma=$email_DEBUG_BCC_list_comma
  echo email_FROM=$email_FROM
  echo email_SUBJECT=$email_SUBJECT
  echo email_BOUNDARY=$email_BOUNDARY
  echo email_NOTIFY=$email_NOTIFY
  echo email_TEMPLATE=$email_TEMPLATE
  echo email_POPULATED=$email_POPULATED
  echo email_FOLLOWUP_NOTE=$email_FOLLOWUP_NOTE
  echo report_table=$report_table
  echo report_table_counted_field=$report_table_counted_field
  echo report_table_date_field=$report_table_date_field
  echo report_table_criterion1=$report_table_criterion1
  echo IsDebuggingEnabled=$IsDebuggingEnabled

  # 3. - We no longer need to deal with credentials at rest by storing encrypted ones.
  #  However, for testing in the development and test environments (as of 4 Feb 2018),
  #  passwords are needed for testing.
  # $ echo foobar | openssl enc -aes-128-cbc -a -salt -pass pass:asdffdsa
  #  tableauuser=$(echo $tableauuser_aes128 | openssl enc -aes-128-cbc -a -d -salt -pass pass:asdffdsa)
  #  tableaupass=$(echo $tableaupass_aes128 | openssl enc -aes-128-cbc -a -d -salt -pass pass:asdffdsa)

  # 4. - set up Tableau report generation vaiables and folders
  echo "project" $tableau_project
  echo "workbook" $tableau_workbook
  echo "view" $tableau_first_view
  echo "width:" $tableau_width " height:" $tableau_height
  echo "output filename" $tableau_output_filename

  #data_sources_path="/data/dev/data_sources"
  #echo $data_sources_path

  #echo "Create directory in" $data_sources_path
  #if [ ! -d "$data_sources_path/tableau/asp" ]; then mkdir -p $data_sources_path/tableau/asp; else rm $data_sources_path/tableau/asp/*; fi

  rm -f $tableau_output_filename

  echo "Prepare and download png of report"

##if [[ -f "$tableau_output_filename" ]]; then  echo "PNG is downloaded: $tableau_output_filename"

  /usr/local/bin/tabcmd export "$tableau_workbook/${tableau_first_view}" --png -f "$tableau_output_filename" --pagelayout landscape --pagesize unspecified --width ${tableau_width} --height ${tableau_height}

  ls -al

  if [[ -f "$tableau_output_filename" ]]; then  echo "PNG is downloaded: $tableau_output_filename"

    # 5 - Build entire E-mail message, piece by piece, using a existing E-mail message in the desired format (such as the ones that come from Tableau)
    #     that has had the TO/FROM/BCC/CC/SUBJECT headers and embedded image removed.  The boundary follows the embedded image from the template E-mail message.
    #     email_NOTIFY_comma is a list of comma delimited E-mail addresses to notify if the Tableau process did not produce a valid report.
    #     email_FOLLOWUP_NOTE is a note that follows the tableau report image if there is an inquiry about the report... i.e. contact the following DL for more information....

    if [ $IsDebuggingEnabled -eq 0 ]; then
      echo "
          ‡‡‡‡‡‡‡ DEBUGGING is NOT enabled. ‡‡‡‡‡‡‡
           "
      email_TO_list_comma=$email_TO_list_comma
      email_CC_list_comma=$email_CC_list_comma
      email_BCC_list_comma=$email_BCC_list_comma
    else
      echo "
          ‡‡‡‡‡‡‡ DEBUGGING IS enabled, so E-mail messages go out to the DEBUG lists. ‡‡‡‡‡‡‡
           "
      email_TO_list_comma=$email_DEBUG_TO_list_comma
      email_CC_list_comma=$email_DEBUG_CC_list_comma
      email_BCC_list_comma=$email_DEBUG_BCC_list_comma
    fi

    email_FROM=$email_FROM;
    email_SUBJECT=$email_SUBJECT;
    email_BOUNDARY=$email_BOUNDARY=;
    email_NOTIFY=$email_NOTIFY;
    email_TEMPLATE=$email_TEMPLATE;
    email_POPULATED=$email_POPULATED;

    rm -f $email_POPULATED

    cp $email_TEMPLATE $email_POPULATED

    # find and replace, in place, each of the below elements
    sed -i -e "s/∞∞∞TO∞∞∞/$email_TO_list_comma/g" $email_POPULATED
    sed -i -e "s/∞∞∞CC∞∞∞/$email_CC_list_comma/g" $email_POPULATED
    sed -i -e "s/∞∞∞BCC∞∞∞/$email_BCC_list_comma/g" $email_POPULATED
    sed -i -e "s/∞∞∞FROM∞∞∞/$email_FROM/g" $email_POPULATED
    sed -i -e "s/∞∞∞SUBJECT∞∞∞/$email_SUBJECT/g" $email_POPULATED
    sed -i -e "s/∞∞∞FOLLOWUP_NOTE∞∞∞/$email_FOLLOWUP_NOTE/g" $email_POPULATED
    sed -i -e "s/∞∞∞WORKBOOK_NAME∞∞∞/$tableau_workbook/g" $email_POPULATED
    sed -i -e "s/∞∞∞WORKBOOK_VIEW∞∞∞/$tableau_first_view/g" $email_POPULATED
    sed -i -e "s/∞∞∞WORKBOOK_NO∞∞∞/$tableau_workbook_no/g" $email_POPULATED

    # find and replace, in place, any placeholder empty strings
    sed -i -e "s/^TO: ''/TO:/g" $email_POPULATED
    sed -i -e "s/^CC: ''/CC:/g" $email_POPULATED
    sed -i -e "s/^BCC: ''/BCC:/g" $email_POPULATED

    echo "" >> $email_POPULATED

    #encode image to base 64
    img=$(base64 $tableau_output_filename);
    echo $img > img.base64

    #chop up long base 64 string to bite-sized pieces by replacing spaces with new lines
    sed -i -e "s/ /\n/g" img.base64

    cat img.base64 >> $email_POPULATED

    #removes newline character after base64 image so "=" can be placed at the end
    truncate -s -1 $email_POPULATED

    echo "=" >> $email_POPULATED

    echo "$email_BOUNDARY" >> $email_POPULATED

    echo "
            ‡‡‡‡‡‡‡ See the head and the tail of the E-mail message ($email_POPULATED) below. ‡‡‡‡‡‡‡

    "
    head $email_POPULATED
    tail $email_POPULATED

    echo "
            ‡‡‡‡‡‡‡ Now sending ($email_POPULATED). ‡‡‡‡‡‡‡

    "

    # Now send the E-mail message, else, send a notification that the E-mail was not sent
    cat $email_POPULATED |sendmail -t
  else
    echo "$tableau_workbook/${tableau_first_view} had an issue regarding automated report preparation and sending" | mailx -s "$tableau_workbook/${tableau_first_view} did not send " -r "$email_FROM" $email_NOTIFY
  fi
fi
