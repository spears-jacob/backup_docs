### metadata_repair.sh
- The shell script compares available metadata with populated folders in s3 and adjust the metadata to match what is available.

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- Load Athena Utilities, [also found in this repository](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/athena_utilities/athena_utils.sh)
- Configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)

## metadata_repair.sh Usage
```
mdr db.tablename [max-items integer]


  -- The db.tablename relate to the credentials currently exported
  -- So, make sure each of the following are available from the current terminal
   $AWS_ACCESS_KEY_ID
   $AWS_SECRET_ACCESS_KEY
   $AWS_SESSION_TOKEN
  -- PLEASE use the max-items integer, in the low thousands, if pulling from large
  -- tables such as core_quantum_events / core_quantum_events_sspp.
  -- This way, everyone else querying against the table at that time will have a
  -- chance to run properly.  Otherwise, all the other queries are blocked.

-- In the unlikely event that the resultant script(s) need to be run in the background,
-- consider the following approach:  nohup bash -c "mdr" > nohup_log_for_adding_partitions &
-- or just run 'caffeinate' from a terminal on your mac, and it will not go to sleep until <ctrl>-c is used to escape the command
