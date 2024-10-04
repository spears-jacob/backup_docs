## add_steps_emr.sh
#### After providing the credentials to the terminal as would be needed for any AWS CLI operation, The main things to adjust are the parameters around the EMR ID, region, and dates.  The EMR id is found as described in the chalk page linked below.  The start and end dates contain and bound what will be passed as steps to the EMR specified.  The export name suffix is just nice to have.  The number of day between runs (nodbr) defaults to one, but can be set to 7 for weekly, etc.  The run direction (run_direction) allows runs to go either forward or backward based on the start and end dates.  Essentially, revise the variables in lines 25-38 of the script, and the script will add steps that are based on the example emr with dates between the start and end date set.

### Alternatively, it is possible to provide a list of run dates in lieu of specifying start and end dates, separated by spaces in a single set of double quotes.

```
export emr_id=''

export start_date='YYYY-MM-DD'
export end_date='YYYY-MM-DD'
export name_suffix='_my_backfilled_agg'

export example_emr_id='j-3OAGFP7Q9YCOX'

nodbr=1

run_direction=backward
```

#### Below is the chalk documentation for cloning and adding steps to an EMR.
- https://chalk.charter.com/display/XGDATAMART/AWS+Backfilling+-+cloning+and+adding+steps+to+EMR+clusters

#### In order to ensure the script will run
- Make sure to have AWS credentials set beforehand

#### Requirements for running this script on a mac laptop:
  1. first install xcode:  `xcode-select --install`
  2. then homebrew:        `/usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)" `
  3. then aws cli:         `brew install awscli`
  4. then jq:              `brew install jq`

## add_steps_emr.sh Usage
```
source add_steps_emr.sh [optional list of dates which overrides start and end dates]

  -- The EMR to which steps are added must be accessible with the credentials currently exported

  -- So, make sure each of the following are available from the current terminal
   $AWS_ACCESS_KEY_ID
   $AWS_SECRET_ACCESS_KEY
   $AWS_SESSION_TOKEN

  -- Example of date list use:
  source add_steps_emr.sh "2021-03-08 2026-11-17 2022-03-19 2020-06-24 2021-03-25"

```
