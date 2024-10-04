### metadata_repair.sh
- The shell script compares available metadata with populated folders in s3 and adjust the metadata to match what is available.

In order to ensure the script will run
- Make sure to have AWS credentials set beforehand
- and update Athena output locations to match what is seen in Athena in the console (Query result location in Athena workgroup being used)
- by updating lines 22-25 to correspond to the values used by your team:
- https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/metadata_repair/metadata_repair.sh#L22

#### Requirements for running this script on a mac laptop.
  1. first install xcode:  `xcode-select --install`
  2. then homebrew:        `/usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)" `
  3. then aws cli:         `brew install awscli`
  4. then jq:              `brew install awscli`
  5. bash 5 -- check first: (bash --version), install if needed: https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3


## metadata_repair.sh Usage
```
source metadata_repair.sh db.tablename [max-items integer]


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
-- consider the following approach:  nohup bash -c "source athena_utils.sh; source add_commands.sh" > nohup_log_for_adding_partitions &
-- or just run 'caffeinate' from a terminal on your mac, and it will not go to sleep until <ctrl>-c is used to escape the command
