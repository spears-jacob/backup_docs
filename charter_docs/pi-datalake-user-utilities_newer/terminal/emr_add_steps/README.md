## add_steps_emr.sh
#### Add steps to an already-running EMR using a mac laptop from the command line.  This is an effective way to avoid the startup time between EMR clusters for backfills of consecutive runs.  Most relevant options are selectable, including which steps need to be executed -- any number is valid, as there is an interactive choice dialog in the script.

### Things you'll need
- An example emr run that has successfully run the code you want to add
- Know the dates that you want to run this code for
- Know how many days at a time the code covers
- Know whether the code uses prior runs' data, or whether each time period runs independently of the others
- (If there are multiple steps in the job you're copying) know whether the steps depend on the prior steps' data, or whether each step runs independently

### Input parameters
#### nodbr (Number Of Days Between Runs)
- number of days between runs
- use 1 for daily and 7 for weekly, 30 for monthly (start on 10th of month)
    - NB: this affects how the date range is used.  If nodbr is not 1, be prepared to do some extra math in the date range section

#### start date / end date
- start date must come before end date
- script will build steps as follows:
    - 1: create a step for the end date (or start date, if the forward option is set -- see below)
    - 2: subtract nodbr from that date.  If the result is after the start date then create a step for that date.  If it is not, then stop creating steps. 
    - 3: repeat step 2 until there are no more steps to be created.
  - This means that depending on the length of your date range and the remainder after it's divided by nodbr, some days outside your range may be run, or some days inside your range may not be run. 
  - EXAMPLE: You have a job that runs for 7 days each time, and you need to backfill January (yyyy-01-01 to yyyy-01-31)
    - If running backwards, this job will run for 31Jan, 24Jan, 17Jan, 10Jan, and 3Jan.  Since a run-date of 3Jan will cover 7 days, this backfill will overwrite all of January, and 4 days of the prior December.  If it's essential that nothing in December is changed, you'll have to set start_date as yyyy-01-09, and then run this script again using start_date = end_date = yyyy-01-07

#### run_direction
- run forward or backward
- forward means start date -> end date
- backward means end date -> start date (default)

#### multistep
- For multiple step runs, iterate through steps-by-date or dates-by-step
- Choose steps-by-date if running multiple steps in sequence, day-at-a-time
- Choose dates-by-step if running step A for a series of dates, then step B etc

#### After providing the credentials to the terminal as would be needed for any AWS CLI operation, the region needs to be set or will be default to us-east-1. The EMR id is found as described in the chalk page linked below, or by using the ```gac``` / [get-active-cluster utility](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/tree/master/terminal/get_active_cluster_master_ec2_id) found in this repository.

#### Below is the chalk documentation for cloning and adding steps to an EMR.
- https://chalk.charter.com/display/XGDATAMART/AWS+Backfilling+-+cloning+and+adding+steps+to+EMR+clusters

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- Configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)

## add_steps_emr.sh Usage
```
Usage: eas <emr_id> <example_emr_id> <start_date or "use-list"> <end_date or space-delimited date list> <nodbr> <run_direction> <multistep>

  -- EMR ids and date fields are mandatory inputs.
  -- Default for nodbr (number of days between runs) is 1, which is what happens when "use-list" is used
  -- Default run direction is backward
  -- Default for multistep is dates-by-step

  -- The EMR to which steps are added must be accessible with the credentials currently exported

  -- So, make sure each of the following are available from the current terminal
   $AWS_ACCESS_KEY_ID
   $AWS_SECRET_ACCESS_KEY
   $AWS_SESSION_TOKEN

  -- Examples:
  eas j-2JJ4OCGAM136 j-3SP3TD1RWB1KN use-list "2022-11-28 2023-01-31 2019-11-12"
  eas j-1P5ZV5BO14KKE j-1XHF8ZH8U042U 2022-12-01 2022-12-22
  eas j-3TJHLBQP8ZMWU j-EM8B2TIWFH2S 2022-12-28 2023-01-06 1 forward
  eas j-2W4FQANUQ5AQT j-31BRCXK0AUQLM 2022-12-28 2023-01-06 1 backward dates-by-step
  eas j-2W4FQANUQ5AQT j-31BRCXK0AUQLM 2022-12-28 2023-01-06 1 forward steps-by-date
  eas j-L7G5XOCFXDD8 j-11LUKWZKISM3N 2021-11-28 2023-01-06 44

```
