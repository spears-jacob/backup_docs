## emr_get_logs.sh
#### Pull down an application log from an EMR cluster based on the current credentials and region.  By in large, it takes some effort to find these logs, but this approach allows straightforward inspection of the main application log using the EMR id (j-XXXXXXXXX).

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- See the [common_utilities readme](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md) for requirements.

## emr_get_logs.sh setup
Use one of the following approaches to make available this set of functions:
  1. run the script as needed:  `source emr_get_logs.sh`
  2. configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)



## emr_get_logs.sh to get the Spark history server log, use:
```
gsl <j-XXXXXXXXXXXX>
```

## emr_get_logs.sh to get the Hive user log, use:
```
ghl <j-XXXXXXXXXXXX>
```
