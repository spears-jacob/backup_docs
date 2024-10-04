## emr_clone.sh
#### Clone an EMR in a single command, where all steps will continue on failure for rerun and debug mode.
#### See EMR Add Steps, [also found in this repository](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/emr_add_steps/add_steps_emr.sh), to add steps once the EMR is cloned.

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- See the [common_utilities readme](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md) for requirements.
- If needed, load EMR Add Steps, [also found in this repository](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/emr_add_steps/add_steps_emr.sh).  This is only necessary if the setup for common_utilities has not already been performed as described in its README.

## emr_clone.sh setup
Use one of the following approaches to make available this set of functions:
  1. run the script as needed:  `source emr_clone.sh`
  2. configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)


## emr_clone.sh Usage
```
Usage: ec <emr_id> <run_mode> <backfill_suffix>

        [rerun/debug/backfill]:second option (run_mode) defaults to 'rerun' if nothing is entered
        -- 'rerun' is a straight clone that terminates after steps are complete, name includes 'rerun'
        -- 'debug' includes all steps, but has a 3h timeout, name includes 'debug'
        -- 'backfill' includes no steps because they need to be added directly thereafter (using emr-add-steps)

        [backfill_suffix]
        third option is not required, but is used as the suffix to 'backfill' on the name set for backfill EMRs
```

## emr_clone.sh examples
```
ec j-238ELGZSUBXZH backfill jan_mar23

ec j-3KZGWT26LCNU

ec j-57BQUMYWVRJ5 rerun

ec j-USNVD4OWC7BO debug
```


