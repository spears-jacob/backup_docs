## emr_get_logs.sh
#### Pull down an application log from an EMR cluster based on the current credentials and region.  By in large, it takes some effort to find these logs, but this approach allows straightforward inspection of the main application log using the EMR id (j-XXXXXXXXX).

#### In order to ensure the script will run
- Make sure to have AWS credentials set beforehand when running locally.

#### Requirements for running this script on a mac laptop.
  1. first install xcode:  `xcode-select --install`
  2. then homebrew:        `/usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)" `
  3. then aws cli:         `brew install awscli`
  4. then jq:              `brew install jq`
  5. bash 5 -- check first: (bash --version), install if needed: https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3

## emr_get_logs.sh setup
Use one of the following approaches to make available this set of functions:
  1. run the script as needed:  `source emr_get_logs.sh`
  2. allow for execution in any terminal by placing the contents of the script in `~/.bash_profile`
  3. create an alias that points to the script in `~/.bash_profile`


## emr_get_logs.sh to get the Spark history server log, use:
```
gsl <j-XXXXXXXXXXXX>
```

## emr_get_logs.sh to get the Hive user log, use:
```
ghl <j-XXXXXXXXXXXX>
```
