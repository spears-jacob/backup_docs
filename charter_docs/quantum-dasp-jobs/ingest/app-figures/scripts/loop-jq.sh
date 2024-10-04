#!/bin/bash
# $1 is original input, $2 is output directory, $3 is name of jq
input_dir=$1
output_dir=$2
script=$3

gzs=($(find $input_dir -type f -name '*.gz'))
echo ${#gzs[@]}

if [ ! -d "$output_dir" ]; then mkdir -p $output_dir; fi

index=0
while [ $index -lt ${#gzs[@]} ];
do
  file=${gzs[$index]}
  if [ -f $file ]; then gzip -d -f $file; fi
  in_file=`sed 's/.gz//g' <<< $file`
  # out_file=`cut -d '/' -f3 <<< $in_file`
  out_file=`sed 's/[/]//g' <<< $in_file`
  jq -r -f ./scripts/jq/$script-${SCRIPT_VERSION}.jq $in_file > $output_dir/$out_file'.csv'
  (( index++ ))
done
