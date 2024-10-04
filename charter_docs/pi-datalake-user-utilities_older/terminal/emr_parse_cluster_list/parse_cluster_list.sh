#!/usr/bin/env bash

# parse_cluster_list.sh
# The shell script uses the current region and credentials to capture and parse
# all EMR clusters for a specified number of days going backwards from today.


# Ensures that there is at least one input but not more than two inputs from the script execution
# and that the number of days to go back is a positive integer, defaulting to three
if (! [ $# -gt 0 ] && [ $# -lt 3 ] && [ -z "$1" ]  && [ ! "$1" ] ) ; then echo -e "\n\nUsage:\n\nsource parse_cluster_list.sh string_to_grep_from_cluster_name <days to lag>\n\n"; kill -INT $$
elif [ -z "$2" ]; then export num_days_to_go_back=3 ;
elif [ "$2" -gt 0 ] 2>/dev/null; then export num_days_to_go_back="$2";
else  echo -e "\n\n Usage:\n\nsource parse_cluster_list.sh string_to_grep_from_cluster_name <days to lag>\n\n\ndays to lag: is not a valid positive integer: \nPlease revise $2 to a valid integer\n\n"; kill -INT $$
fi

aws sts get-caller-identity
# evaluate error code;  giving 0 on success and 255 if you have no credentials.
if [ $? -ne 0 ]; then echo -e "\n\tPlease try again with valid credentials and region"; kill -INT $$; fi

gdate=/usr/local/bin/gdate
if [ ! -x $gdate ]; then
  echo "
  Please install coreutils:
          \->   brew install coreutils" >&2
  kill -INT $$;
fi

verifyjq=/usr/local/bin/jq​; [ ! -x $verifyjq ];
if [ $? -ne 0 ]; then  echo "
  Please install jq:
          \->   brew install jq" >&2
  kill -INT $$;
fi
export string_to_grep_from_cluster_name=$1

#folder housekeeping
export cwd=`pwd`
cd `mktemp -d`
export twd=`pwd`

function format_seconds() {
  (($1 >= 86400)) && printf '%d days and ' $(($1 / 86400)) # days
  (($1 >= 3600)) && printf '%02d:' $(($1 / 3600 % 24))     # hours
  (($1 >= 60)) && printf '%02d:' $(($1 / 60 % 60))         # minutes
  printf '%02d%s\n' $(($1 % 60)) "$( (($1 < 60 )) && echo ' s.' || echo '')"
}

tomorrow=$(date -u -v+1d +%Y-%m-%d)
for n in $( seq 1 $num_days_to_go_back )
do
    let "b= $n + 1"
    let "e= $n - 1"
    beginning_date_utc=$(date -j -f %Y-%m-%d -v-"$b"d $tomorrow +%Y-%m-%dT23:59:59.)
    ending_date_utc=$(date -j -f %Y-%m-%d -v-"$e"d $tomorrow +%Y-%m-%dT00:00:00.)
    echo -e "\nNow processing the day $n.\n\tafter\t$beginning_date_utc\n\tbefore\t$ending_date_utc"
    awlc="aws emr list-clusters --output json --created-after  $beginning_date_utc --created-before $ending_date_utc "
    unset NEXT_TOKEN

    function parse_output() {
      if [ ! -z "$cli_output" ]; then
        echo $cli_output  >> listOfclusters.json
        jq -r '.Clusters [] | "\(.Status.Timeline.CreationDateTime), \(.Id), \(.Name), \(.Status.State), \(.NormalizedInstanceHours), §\(.Status.Timeline.EndDateTime)ø\(.Status.Timeline.ReadyDateTime)§ https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:\(.Id) "' listOfclusters.json >> listOfManyclusters.txt


        NEXT_TOKEN=$(echo $cli_output | jq -r ".NextToken")
      fi
    }

    cli_output=$($awlc)
    parse_output

    while [ "$NEXT_TOKEN" != "null" ]; do
      if [ "$NEXT_TOKEN" == "null" ] || [ -z "$NEXT_TOKEN" ] ; then
        echo "now running: $awlc --max-items 333 --page-size 333 --no-paginate"
        sleep 3
        cli_output=$($awlc)
        parse_output
      else
        echo "now paginating: $awlc --max-items 333 --page-size 333 --starting-token $NEXT_TOKEN"
        sleep 3
        cli_output=$($awlc --starting-token $NEXT_TOKEN)
        parse_output
      fi
   done  #pagination loop
   sleep 3
done #day loop

cat listOfManyclusters.txt | sort -u > listOfDurationLessclusters.txt

while read line
do
  # pluck out times
  # test that each one is not a 'null' or return 'null'
  # subtract and format them
  ending=$(echo $line | perl -ple's/^.+§(.+)ø(.+)§.+$/$1/')
  ready=$(echo $line | perl -ple's/^.+§(.+)ø(.+)§.+$/$2/')

  if [[ $ready =~ 'null' ]] || [[ $ending =~ 'null' ]] || [[ ${#ready} -ne 32 ]] || [[ ${#ending} -ne 32 ]]; then elapsed='null'
  else
    r=$(gdate --date="$ready" +%s)
    e=$(gdate --date="$ending" +%s)
    seconds=$((e-r))
    elapsed=`format_seconds $seconds`
  fi
  echo $line | perl -ple"s/§(.+)ø(.+)§/$elapsed,/" >> listOfclusters.txt
done < listOfDurationLessclusters.txt

grep $string_to_grep_from_cluster_name listOfclusters.txt

cd $cwd

echo "
Note: The list of clusters for the past $num_days_to_go_back days is still available
      in the following location if needed.
$twd/listOfclusters.txt

Usage:
       grep <cluster_name_pattern> \$twd/listOfclusters.txt
"
