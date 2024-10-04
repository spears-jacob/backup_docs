## pull_down_portion_of_table_to_local_hdfs.sh
#### Use this script to pull down certain data from S3 to local hdfs, in a manner that looks a lot like what partition pruning is supposed to look like.  The use case is for queries that perform better locally, which is often the case when using hdfs pointing to S3.  The demonstration included allows queries to be run against CQE_SSPP for between several days to a month or more, depending on the complexity of the query.  This allows larger data sets to be queried as well as the preparation of investigations that are not currently possible otherwise.  This approach is useful for production jobs as well as ad hoc analysis.  
#### There are several steps that the script goes through to pull down the data from s3 to local hdfs.  First of all, it requires several configuration variables that are set up interactively or using a helper script.  The allows for some of the complexity and customization of the run to be abstracted away to an interactive session or script.  

*  Date processing comes first, which is then used to pull *all* the partitions down for the date range.  
*  Then the list is cut down to only the application names required.  This could be any partition field, but CQE uses application names, hence their filtering.   
*  Next, a prefix file is built by testing the utc_date_hour so that only prefixes that *include files in them* are allowed, which is uniquely sorted to remove duplicates due to multiple files per partition.
*  Then, s3-dist-cp is run, copying *only data files* into the correctly formatted folder structure, *not* including the placeholder_$folder$ objects which corrupt the table metadata.

#### In order to ensure the script will run
- Make sure that athena_utils.sh is set up and has been run in the session
- https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/athena_utilities/

#### Examples of interactive and scripted approaches
- For an interactive / pushbutton cluster example, see the following.
https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/pull_down_portion_of_table_to_local_hdfs/execute_hql_on_pushbutton_cluster.sh

- For an completely scripted EMR job example, see the following.
https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/util/daily-adhoc/scripts/execute_hql.sh


## pull_down_portion_of_table_to_local_hdfs.sh Usage
```
source pull_down_portion_of_table_to_local_hdfs.sh

  -- This script handles the movement of data from S3 to local HDFS.
  -- The creation, repair, and querying of the local table must be handled interactively
     or using a helper script.
  -- Do make sure to drop the local table after use, as it is of no use after
     the EMR spins down.
  -- The required environment variables for using this script are below
      - unq_id -- some sort of unique identifier to keep the resulting table name unique
      - s3_root -- the s3 bucket name itself
      - s3_path_till_partitions -- the bucket name, and all the folders up until the partitions
      - tablename_to_use_for_definition -- the table that uses the above path in its definition of location
      - local_tablename -- the name to use in the subsequent queries and drop table when the querying is done
      - application_names_array -- a space-delimited array of partitions to allow in the data
      - START_DATE, END_DATE -- start and end dates
      - START_DATE_TZ, END_DATE_TZ -- start and end dates with desired timezone-adjusted UTC hour offset
        for limiting partitions
```
