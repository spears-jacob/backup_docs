#!/bin/bash

# This script downloads a png image of a Tableau report view, encodes it into a
# long string of characters (base64), chops up the long string into managable bites,
# places it into a predefined E-mail message template, replaces some E-mail headers,
# adds some requisite characters and boundaries to make it work, and sends the E-mail.
# All of the above happens after checking both in hive and Tableau that data exists
# is current.

# Please keep in mind that even if an E-mail list is blank, it must include "''" as the value,
# otherwise the input variables do not properly match the order in which they are passed.

# 0. - Prepare a few variables for checking hive and then Tableau
export tableau_workbook=${5}
export tableau_views=${6}
export tableau_metric_checking_view=${7}

export email_FROM=${16}
export email_SUBJECT=${17}
export email_NOTIFY=${19}

export report_table=${24}
export report_table_counted_field=${25}
export report_table_date_field=${26}
export report_table_criterion1=${27}
export report_table_where_field1=${29}
export report_table_where_value1=${30}
export csv_file_select=${31}
export report_table_where_field2=${32}
export report_table_where_value2=${33}

# 1. - First check that data exists before proceeding

echo "### Loading run date $RUN_DATE ###"

if [ "$RUN_DATE" != "" ]; then
  data_date=`date --date="$RUN_DATE -1 day" +%Y-%m-%d`
  data_date_csv=`date --date="$RUN_DATE -1 day" +%-m/%-d/%Y`

  echo "### Looking for $data_date data using the following query: ###

  SELECT cast($report_table_counted_field as int)
  FROM $report_table
  WHERE $report_table_date_field = '$data_date'
  AND $report_table_where_field1 = '$report_table_where_value1'
  AND $report_table_where_field2 = '$report_table_where_value2'
  ;

  "
  data_cnt="$(hive -e "set hive.cli.print.header=false; USE $ENVIRONMENT; SELECT cast($report_table_counted_field as int) FROM $report_table WHERE $report_table_date_field = '$data_date' AND $report_table_where_field1 = '$report_table_where_value1' AND $report_table_where_field2 = '$report_table_where_value2';")";
  if [ ${data_cnt[0]} = "NULL" ]; then
     data_cnt=0
  fi

  echo "

   --> Row count for $data_date: ${data_cnt[0]}

  "

  if [ ${data_cnt[0]} -eq 0 ]; then
    echo "### ERROR: No data available for run date $RUN_DATE ($data_date data) in $report_table in the $report_table_date_field field"
    echo "$tableau_workbook/${tableau_views} had an issue regarding current data for report preparation
          ### ERROR: No data available for run date $RUN_DATE ($data_date data) in $report_table in the $report_table_date_field field
         " | mailx -s "E-Mail Prep / Data missing - $tableau_workbook/${tableau_views} {no data in hive}" -r "$email_FROM" $email_NOTIFY
  fi

else
  echo "### ERROR: Run date ($RUN_DATE) value not available" && exit 1
fi

# Bypass all the E-mail prep if no data
if [ ${data_cnt[0]} -gt 0 ]; then


  # 2. - Check that workbook has been refreshed with data for yesterday.
  #       This requires a view that shows the number of metrics.  Also, the refresh is forced
  #       with the ?:refresh=yes at the end of the tabcmd call.


  echo "

        ### Since there is indeed data in hive for $data_date,  ###

        Now checking for the existence of the correct number of metrics for the same date.

      "
  rm -f $tableau_metric_checking_view.csv

  /usr/local/bin/tabcmd get "/views/$tableau_workbook/$tableau_metric_checking_view.csv?:refresh=yes"

  export tab_ck_vw=$tableau_metric_checking_view.csv

  if [[ -f "$tab_ck_vw" ]]; then echo "
    $tab_ck_vw has been downloaded to evaluate."
  else echo "
    There was an error attempting to download $tab_ck_vw."
  fi

  # command.1=/usr/local/bin/tabcmd get "/views/ResiandSmbPerformanceStudy/ResiDailyTab.csv"

  #  The metric checking view used in this case is shown below
  #
  #  → head -2 ResiDaily.csv
  #
  #  Measure Names,Page Name Group,daily_fix_domain_type_page_diff_%,Measure Values
  #  Total Page Views,home-unauth,-0.016172721,"327,220"

  #  Since the view has column headers, we need to just use the second line,
  #  as the view is sorted to have the most recent date at the top (2nd line).
  export SecondLineOfCheckingView=$(cat $tab_ck_vw | grep $data_date_csv | grep $csv_file_select |sed -n '1 p' | sed 's/"//g')

  echo "

        ### The second line of the checking view is the following.  ###

        $SecondLineOfCheckingView

      "
  #  prepare array of the second line of the metric checking view
  IFS=',' read -r -a metric_checking_array <<< "$SecondLineOfCheckingView"

  for index in "${!metric_checking_array[@]}"
    do
      echo "
      $index ${metric_checking_array[index]}
      "
    done

    export MetricStatus="${metric_checking_array[2]}"
    export ThresholdMatch="${metric_checking_array[3]}"
    export TotalMetricCount="${data_cnt[0]}"

    # remove leading whitespace characters
    TotalMetricCount="${TotalMetricCount#"${TotalMetricCount%%[![:space:]]*}"}"
    ThresholdMatch="${ThresholdMatch#"${ThresholdMatch%%[![:space:]]*}"}"
    # remove trailing whitespace characters
    TotalMetricCount="${TotalMetricCount%"${TotalMetricCount##*[![:space:]]}"}"
    ThresholdMatch="${ThresholdMatch%"${ThresholdMatch##*[![:space:]]}"}"
    echo "===$TotalMetricCount=== ===$ThresholdMatch==="

    if [ "$MetricStatus" == "True" ] && [ "$TotalMetricCount" == "$ThresholdMatch" ]; then echo "
      The number of metrics found in the Tableau matches the threshold as evaluated by the tableau report csv extract.";
    else
      TableuMetricCheckingIssue="The number of metrics found in the Tableau report does not match the threshold as evaluated by the tableau report csv extract."
    fi

    #Bypass all following code if unless the report refresh evaluation matches
    if [ "$TotalMetricCount" == "$ThresholdMatch" ]; then echo "
      All the Tableau report refresh evaluation is successful.
      ";

      # 3. - Output export statement, set up, and echo all variables passed in from project.properties


      echo "
               ‡‡‡‡‡‡‡ Send PNG from Tableau embedded in an E-mail started ‡‡‡‡‡‡‡

               ‡‡‡‡‡‡‡ See all values for properties below.  Copy and paste them locally for use debugging ‡‡‡‡‡‡‡

               export tableau_server=${1}
               export tableau_workbook_no=${2}
               export tableau_output_filename=${3}
               export tableau_project=${4}
               export tableau_workbook=${5}
               export tableau_views=${6}
               export tableau_metric_checking_view=${7}
               export tableau_width=${8}
               export tableau_height=${9}
               export email_TO_list_comma=${10}
               export email_CC_list_comma=${11}
               export email_BCC_list_comma=${12}
               export email_DEBUG_TO_list_comma=${13}
               export email_DEBUG_CC_list_comma=${14}
               export email_DEBUG_BCC_list_comma=${15}
               export email_FROM=${16}
               export email_SUBJECT=${17}
               export email_BOUNDARY=${18}
               export email_NOTIFY=${19}
               export email_TEMPLATE=${20}
               export email_POPULATED=${21}
               export email_FOLLOWUP_NOTE=${22}
               export email_BODY_IMG=${23}
               export report_table=${24}
               export report_table_counted_field=${25}
               export report_table_date_field=${26}
               export report_table_criterion1=${27}
               export IsDebuggingEnabled=${28}
               export report_table_where_field1=${29}
               export report_table_where_value1=${30}
               export csv_file_select=${31}
               export report_table_where_field2=${32}
               export report_table_where_value2=${33}
      "

      export tableau_server=${1}
      export tableau_workbook_no=${2}
      export tableau_output_filename=${3}
      export tableau_project=${4}
      export tableau_workbook=${5}
      export tableau_views=${6}
      export tableau_metric_checking_view=${7}
      export tableau_width=${8}
      export tableau_height=${9}
      export email_TO_list_comma=${10}
      export email_CC_list_comma=${11}
      export email_BCC_list_comma=${12}
      export email_DEBUG_TO_list_comma=${13}
      export email_DEBUG_CC_list_comma=${14}
      export email_DEBUG_BCC_list_comma=${15}
      export email_FROM=${16}
      export email_SUBJECT=${17}
      export email_BOUNDARY=${18}
      export email_NOTIFY=${19}
      export email_TEMPLATE=${20}
      export email_POPULATED=${21}
      export email_FOLLOWUP_NOTE=${22}
      export email_BODY_IMG=${23}
      export report_table=${24}
      export report_table_counted_field=${25}
      export report_table_date_field=${26}
      export report_table_criterion1=${27}
      export IsDebuggingEnabled=${28}
      export report_table_where_field1=${29}
      export report_table_where_value1=${30}
      export csv_file_select=${31}
      export report_table_where_field2=${32}
      export report_table_where_value2=${33}

      echo "
              ‡‡‡‡‡‡‡ See the current values for local environment variables below. ‡‡‡‡‡‡‡

      "
      echo tableau_server=$tableau_server
      echo tableau_workbook_no=$tableau_workbook_no
      echo tableau_output_filename=$tableau_output_filename
      echo tableau_project=$tableau_project
      echo tableau_workbook=$tableau_workbook
      echo tableau_views=$tableau_views
      echo tableau_metric_checking_view=$tableau_metric_checking_view
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
      echo email_BODY_IMG=$email_BODY_IMG
      echo report_table=$report_table
      echo report_table_counted_field=$report_table_counted_field
      echo report_table_date_field=$report_table_date_field
      echo report_table_criterion1=$report_table_criterion1
      echo IsDebuggingEnabled=$IsDebuggingEnabled
      echo report_table_where_field1=$report_table_where_field1
      echo report_table_where_value1=$report_table_where_value1
      echo report_table_where_field2=$report_table_where_field2
      echo report_table_where_value2=$report_table_where_value2

      # 4. - We no longer need to deal with credentials at rest by storing encrypted ones.
      # $ echo foobar | openssl enc -aes-128-cbc -a -salt -pass pass:asdffdsa
      #  tableauuser=$(echo $tableauuser_aes128 | openssl enc -aes-128-cbc -a -d -salt -pass pass:asdffdsa)
      #  tableaupass=$(echo $tableaupass_aes128 | openssl enc -aes-128-cbc -a -d -salt -pass pass:asdffdsa)

      # 5. - set up Tableau report generation vaiables and folders
      echo "server" $tableau_server
      echo "project" $tableau_project
      echo "workbook" $tableau_workbook
      echo "view" $tableau_views
      echo "width:" $tableau_width " height:" $tableau_height
      echo "output filename" $tableau_output_filename

      #data_sources_path="/data/dev/data_sources"
      #echo $data_sources_path

      #echo "Create directory in" $data_sources_path
      #if [ ! -d "$data_sources_path/tableau/asp" ]; then mkdir -p $data_sources_path/tableau/asp; else rm $data_sources_path/tableau/asp/*; fi

      rm -f $tableau_output_filename
      rm -f "$email_BODY_IMG"_complete

      echo "Prepare and download png(s) of report"


      #  prepare array of views in workbook
      IFS=', ' read -r -a views_array <<< "$tableau_views"

      for index in "${!views_array[@]}"
      do
          echo "

          $index ${views_array[index]}

          "

          echo "

          running:  /usr/local/bin/tabcmd export $tableau_workbook/${views_array[index]} --png -f $index.png --pagelayout landscape --pagesize unspecified --width ${tableau_width} --height ${tableau_height}

          "

          /usr/local/bin/tabcmd export "$tableau_workbook/${views_array[index]}" --png -f "$index.png" --pagelayout landscape --pagesize unspecified --width ${tableau_width} --height ${tableau_height}

      done

      ls -al

      isPNG=1
      if [[ -f "$index.png" ]]; then
         file $index.png | grep PNG
         isPNG=$?
         echo "download $index.png file is PNG: $isPNG"
      fi

      if [ $isPNG -eq 0 ]; then  echo "PNG is downloaded: $tableau_output_filename"

        # 6 - Build entire E-mail message, piece by piece, using a existing E-mail message in the desired format (such as the ones that come from Tableau)
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
        email_BOUNDARY=$email_BOUNDARY;
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
        sed -i -e "s/∞∞∞WORKBOOK_NO∞∞∞/$tableau_workbook_no/g" $email_POPULATED
        sed -i -e "s/∞∞∞SERVER∞∞∞/$tableau_server/g" $email_POPULATED

        # find and replace, in place, any placeholder empty strings
        sed -i -e "s/^TO: ''/TO:/g" $email_POPULATED
        sed -i -e "s/^CC: ''/CC:/g" $email_POPULATED
        sed -i -e "s/^BCC: ''/BCC:/g" $email_POPULATED


        # Now, build the HTML image holders piece by piece
        sed -i -e "s/∞∞∞SERVER∞∞∞/$tableau_server/g" $email_BODY_IMG
        sed -i -e "s/∞∞∞WORKBOOK_NAME∞∞∞/$tableau_workbook/g" $email_BODY_IMG

        for index2 in "${!views_array[@]}"
        do
            echo "$index2 ${views_array[index2]}"

            cat "$email_BODY_IMG" > "$email_BODY_IMG$index2"

            sed -i -e "s/∞∞∞WORKBOOK_VIEW∞∞∞/${views_array[index2]}/g" "$email_BODY_IMG$index2"
            sed -i -e "s/∞∞∞INDEX∞∞∞/$index2/g" "$email_BODY_IMG$index2"

            cat "$email_BODY_IMG$index2" >> "$email_BODY_IMG"_complete

            echo "" >> $email_POPULATED

            #adds boundary and items before img
            echo "$email_BOUNDARY" >> $email_POPULATED
            echo "Content-type: image/png; name=\"tableauImage$index2.png\";
 x-mac-creator=\"4F50494D\";
 x-mac-type=\"504E4766\"
Content-ID: <tableauImage$index2>
Content-disposition: inline;
  filename=\"tableauImage$index2.png\"
Content-transfer-encoding: base64
"  >> $email_POPULATED


            #encode image to base 64
            img=""
            img=$(base64 "$index2.png");
            echo $img > img.base64

            #chop up long base 64 string to bite-sized pieces by replacing spaces with new lines
            sed -i -e "s/ /\n/g" img.base64

            cat img.base64 >> $email_POPULATED

            truncate -s -1 $email_POPULATED

        done

        #replaces body img section
        export body_img_complete=$(cat "$email_BODY_IMG"_complete)

        echo "\$body_img_complete is now:
        $body_img_complete

        "

        #used alternate delimiter "^" and perl to replace a multi line block of body_img
        #that includes all the HTML for each image
        perl -p -i -e "s^∞∞∞BODY_IMG∞∞∞^$body_img_complete^g" $email_POPULATED

        #removes newline character after base64 image so "=" can be placed at the end
        #truncate -s -1 $email_POPULATED

        echo "=" >> $email_POPULATED

        echo "$email_BOUNDARY--" >> $email_POPULATED

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
        echo "$tableau_workbook/${views_array[index2]} had an issue regarding automated report preparation and sending" | mailx -s "$tableau_workbook/${views_array[index2]} did not send {png image download issue}" -r "$email_FROM" $email_NOTIFY
      fi # Bypass all the E-mail prep if no png was downloaded
    else
      echo "$tableau_workbook/${views_array[index2]}: $TableuMetricCheckingIssue" | mailx -s "$tableau_workbook/${views_array[index2]} did not send {report refresh issue}" -r "$email_FROM" $email_NOTIFY
    fi  # Bypass all the E-mail prep if no data report
fi  # Bypass all the E-mail prep if no data in hive
