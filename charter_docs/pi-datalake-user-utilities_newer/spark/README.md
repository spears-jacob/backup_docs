# spark utilities
- utilities intended to be shared across teams for use in Spark

## Nelson Rules
- See the [nelson_rules folder](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/tree/master/spark/nelson_rules) for a PySpark implementation of Nelson Rules for Anomaly Detection

## spark_run.sh: This script sets up the environment in an EMR for running SQL using PySpark
### Usage: source spark_run.sh, dependent on functions in emr_utils.sh, so source that first
### Features:
- Sanitizes input (either file or query string) by removing 1) comments 2) set statements and by replacing either shell environment ${env:...} or hiveconf ${hiveconf:...} variables as long as they are named the same as environment variables available in the shell from which the script is called
- Choose to run a file or a query, becausing passing a query inside quotation marks is treated just like a file.
- file prep is done in *prep_sql* function
- *run_sql_in_pyspark* calls *prep_sql* and then submits the job
- Each time *run_sql_in_pyspark* is called, all the variables are evaluated, so that you can adjust them and run the query again as needed because each time a new file is prepared based on the input that lives in a temporary folder.
- Deals with cross-account references by adjusting where the backticks are located
- Finds and replaces all hivevars (hive variables) in the input file
- Checks that DISTRIBUTE BY is included if there is an INSERT OVERWRITE query being run.  This is important because without it, spark writes thousands of splintered files instead of large ones.
- As suggested by Z Gendreau, includes functions that add spark and log configuration files.

## pyspark_sql_wrapper.py: This script takes a SQL file, splits them on semicolons, and submits them to spark using PySpark
### Usage: see emr_utils.sh
### Features:
- takes configuration from files specified in the emr_utils readme
- includes appName that is the JOB_STEP, when declared
- shows all configurations that were set when the session was submitted
