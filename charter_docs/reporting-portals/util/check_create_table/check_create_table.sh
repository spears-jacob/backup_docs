#!/bin/bash
# This script checks the <grep_search> on any files that start with 'table_prefix'
# <table prefix and underscore go into the next variable>

export table_prefix=${1}
export grep_search=${2}

export query_text="use prod; show tables like '$table_prefix*';"
echo $query_text

hive -e "$query_text" > "$table_prefix"_tables.txt

echo "

The list of tables with the $table_prefix prefix is below.

"
cat "$table_prefix"_tables.txt

cp "$table_prefix"_tables.txt "$table_prefix"_tables.hql

# removes header line from query results
sed -i '1d;' "$table_prefix"_tables.hql

# adds show create table
sed -i 's/^/SHOW CREATE TABLE /' "$table_prefix"_tables.hql

# adds semicolon at end of line
sed -i 's/$/ ;/' "$table_prefix"_tables.hql

# adds 'USE prod;' to beginning of hql
echo "USE prod;" | cat - "$table_prefix"_tables.hql > tmp_out && mv tmp_out "$table_prefix"_tables.hql

# runs the hql and outputs to 'Show Create Table output file'
hive -f  "$table_prefix"_tables.hql > "$table_prefix"_sct_output.txt

mkdir sct_output

cd sct_output

mkdir tables
mkdir views

#makes lots of files that each contain create table statements
awk '/CREATE /{n++}{print >"out" n ".txt" }' ../"$table_prefix"_sct_output.txt

# ununsed placeholder file out.txt does not contain any create table statements
rm out.txt

# go through sct_output folder and rename files to correspond to table/view names
# the variable "$REPLY" is the file name in this loop
while IFS= read -r -d '' -u 9
do
  export table_name=$(grep -oP '(?<=CREATE TABLE `)\w+' "$REPLY" )
  export view_name=$(grep -oP '(?<=CREATE VIEW `)\w+' "$REPLY" )

  [ ! -z "$table_name" ] &&   mv "$REPLY" tables/"$table_name"
  [ ! -z "$view_name" ]  &&   mv "$REPLY" views/"$view_name"

done 9< <( find . -type f -exec printf '%s\0' {} + )

cd tables
export havert=$(grep -ri  ${grep_search})

echo "


Below is a list of tables having ${grep_search}

$havert


"


echo "


Below is a list of views

"
cd ../views
ls

cd ../tables
export wout=$(grep -riL  ${grep_search})
echo "


#########################################################################
## Please see the following list of Tables WITHOUT ${grep_search}: ##
#########################################################################

$wout
"

# uncomment below if you want to clean up after running.
# cd ../..
# rm -r sct_output
