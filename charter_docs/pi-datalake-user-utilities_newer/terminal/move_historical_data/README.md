## move_historical_data.sh
#### This script is for backing up historical data in prod, deleting it, and moving up the same named data/table from stg.  Scenarios for use include backing up old data because there is revised data as well as schema changes in production tables.

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- Load Athena Utilities, [also found in this repository](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/athena_utilities/athena_utils.sh)
- See the [common_utilities readme](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md) for requirements.

## move_historical_data.sh setup
Use one of the following approaches to make available this set of functions:
  1. run the script as needed:  `source move_historical_data.sh`
  2. configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)

## move_historical_data.sh Usage
Specify the table to archive and the step
```
# Usage:  mhd <db>.<tablename_in_prod_to_archive> <step_number>
##           Step 1 - copy data to historical folder in production
##           Step 2 - prepares historical table ddl, creates and repairs historical table
##           Step 3 - delete data from production table
##           Step 4 - move data up from stg environment
```


## move_historical_data to copy data to historical folder in production, use:
```
mhd prod_dasp.asp_digital_adoption_monthly 1
```


