#!/bin/bash
# This script checks the <table_for_checking> corresponds to the create view
# in the <view_for_checking>  -- currently we are only concerned with prod db

export table_for_checking=${1}
export view_for_checking=${2}

hive -e "use prod; show create table $view_for_checking;" > cv_out || exit 17152320


export table_name=$(grep -oP '(?<=FROM \`prod\`.\`)\w+' cv_out );
echo "

    $table_name

      is being evaluated to ensure that it is the basis for view:

    $view_for_checking

";

if [ "$table_name" != "$table_for_checking" ]; then
  echo "

      ### ERROR: $table_name != $table_for_checking
              -- the underlying table for the view ($view_for_checking) 
              -- differs from the one being evaluated


      ### EXITING JOB


      " && exit 1;
fi
