## common_utilities.sh
#### This set of common utilities allows prequisites, credentials, and other functions to be loaded and used by other scripts.  Included are a host of prerequisite checking functions, a credential checking function, a choose-credential function, a tag-harvesting function, functions for starting EMRs using lambdas (including passing RUN_TIME for specifying the date), get terminated_with_errors clusters for your team (filtered by team tags and a job filter), multiselector for interactively choosing from a list (used in the EMR-add-steps script), choose-from-menu selector used in get active cluster master ec2 script, aliases for upgrading packages in pip/python and homebrew, and general formatting items.

#### Demo of many of the functions in this script and ones referenced in this readme [is available here](https://chalk.charter.com/download/attachments/721183210/Common%20Utilities%20Updates%202023-02-10.mp4?version=1&modificationDate=1676062336000&api=v2), from the [Training Sessions Recording chalk page](https://chalk.charter.com/display/XGDATAMART/Training+Session+Recordings)

#### Requirements for running this script on a mac laptop:
  1. first install xcode:  `xcode-select --install`
     - This also fixes an error that occurs whenever the OS is upgradeded.
     - `xcrun: error: invalid active developer path (/Library/Developer/CommandLineTools), missing"`
  2. then homebrew:        `/bin/bash -c "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/HEAD/install.sh)" `
     - for M1 Macs, add the following to your ~/.bash_rc: `eval "$(/opt/homebrew/bin/brew shellenv)"`
     - [Mac M1 homebrew reference](https://earthly.dev/blog/homebrew-on-m1/)
  3. bash 5 -- check first: (`echo ${BASH_VERSION}`)
      - and then install if needed: `brew install bash`
      - for Intel macs: [Mac intel reference](https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3)
      - for M1 macs: [Mac M1 reference](https://apple.stackexchange.com/questions/450766/where-is-bash-installed-by-brew)
      - The following shows where bash is installed and sets the shell appropriately.  Run it line by line.
        - `if [[ "$(sysctl -n machdep.cpu.brand_string)" =~ 'Apple' ]]; then sp="/opt/homebrew/bin/bash"; else sp="/usr/local/bin/bash"; fi ;`
        - `echo -e "\nHomebrew-installed Bash shell path is the following: $sp\n"; if [ -f "$sp" ]; then echo "$sp exists: and \$BASH_VERSION: $BASH_VERSION"; else echo "$sp not found. Do not proceed until this is resolved."; fi`
        - `sudo bash -c "echo ${sp} >> /etc/shells"`
        - `sudo chsh -s "${sp}"`
      - For those using VS Code, in the Command Pallette, set the  “Terminal: Select Default Profile” to the bash just installed.
      - Also, to open any file from the VS Code terminal with `code <filename>`, set the "Shell command 'code' successfully installed in PATH"
      - check your work: `echo ${BASH_VERSION}; dscl . -read ~/ UserShell`
        - and take the output of `UserShell` and see where it points: `ls -al $(dscl . -read ~/ UserShell | cut -d ' ' -f 2)`
        - results from the above show where the link actually points
        - if the value of `echo ${BASH_VERSION}` is not recent (5+), see the following troubleshooting articles: [1](https://apple.stackexchange.com/questions/430529/upgrade-bash-on-m1-but-cannot-change-to-the-new-bash), [2](https://stackoverflow.com/questions/55833235/i-cant-use-bash-5-syntax-even-after-configuring-it-in-macosx)
        - if using these scripts leads to an error regarding _mapfile or readarray: command not found_, see the following. [3](https://stackoverflow.com/questions/61647118/bash-5-mapfile-readarray-command-not-found)
        - if Mac OS Terminal shows the older BASH version, see: [4](https://osxdaily.com/2012/03/21/change-shell-mac-os-x/)
  4. then using homebrew, install the following: aws cli, jq, coreutils, and duti:
     - `brew install awscli jq coreutils duti`
  5. then, also using homebrew, install the session manager plugin
     - `brew install --cask session-manager-plugin`

## common_utilities.sh Usage
1. Find your group's `teamtag` and `jobgrepfilter` as the arguments to pass to the common utilities that allow for filtering of EMRs by team and to easily find your jobs when using `gtc` - to get terminated-with-errors clusters.
   - to find your team tag: ` aws emr describe-cluster --output json --cluster-id j-XXXXXXXXXX | jq -r '.Cluster.Tags | .[] | {Key,Value}  | join(": ")' | grep Team` using an EMR id (j-XXXXXXXXXX) from one of your clusters.
   - For example, `aws emr describe-cluster --output json --cluster-id j-YM6Y2FM3LGGD | jq -r '.Cluster.Tags | .[] | {Key,Value}  | join(": ")' | grep Team ` yields the following result: `Team: self-service-platforms-reporting`
   - whereas the `jobgrepfilter` is simply something in your teams job names that can be used for filtering, such as `pi-qtm-ipv` for example, and can be adjusted at any time by running the `source ....common_utilties.sh <team_tag> <jobgrepfilter>` as needed.
2. Please see [documentation on athena_utilities](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/athena_utilities/README.md) for instructions on how to find workgroup_folder_name for athena-utilities.
3. Source the following scripts in `~/.bash_profile`  The following need to point to the code from the repo wherever it has been placed (~/Documents/gitlab may or may not be where this repo has been cloned), so that each script is made available.
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/common_utilities/common_utilities.sh <team_tag> <jobgrepfilter>`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/athena_utilities/athena_utils.sh <workgroup_folder_name>/output/`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/emr_add_steps/add_steps_emr.sh`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/emr_clone/emr_clone.sh`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/emr_get_logs/emr_get_logs.sh`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/emr_parse_cluster_list/parse_cluster_list.sh`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/get_active_cluster_master_ec2_id/get_active_cluster_master_ec2_id.sh`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/glue_metadata_repair/metadata_repair.sh`
   - `source ~/Documents/gitlab/pi-datalake-user-utilities/terminal/move_historical_data/move_historical_data.sh`
   - `alias al="python3  ~/Documents/gitlab/pi-datalake-user-utilities/terminal/ssm-login/sso-login-all.py"`


## gtc (get-terminated-clusters) Usage
```
gtc
```

## jk (job kickoff) Usage
```
Usage: jk jobname <optional run_date>

Examples:
jk pi-qtm-dasp-prod-medalia-extract
jk pi-qtm-dasp-prod-medalia-extract "2023-12-23"
```

## jkl (job kickoff list of jobs in a text file) Usage
```
Usage: jkl filename
```

## ccr (choose-credentials from current ~/.aws/credentials file) Usage
```
ccr
```

## bu (brew update) and pu (pip update) Usage
```
Usage: bu

Usage: pu
```

## formatting Usage -- See [common_utilities.sh](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/blob/master/terminal/common_utilities/common_utilities.sh#L233) for tag list.
```
Usage: wrap text in color tag and no-color tag to provide cli formatting using 'echo -e' or 'print f' statements.

echo -e "${CyanBold}Blue${NC} is more colorful than ${Gray}gray${NC}.";
```

## get tags from an EMR
```
Usage: gtags <cluster-id>

Example:
gtags j-1MXWW505NIGSD
```

## isValidDate on OSX BASH shell
```
Usage: isYYYYMMDDdate <YYYY-MM-DD>
```

## Display Duration: convert number of seconds to whole days, hours, minutes, or seconds
```
Usage: display_duration <number of seconds>
```

## rawurlencode: returns a string in which all non-alphanumeric characters except -_.~ have been replaced with a percent (%) sign followed by two hex digits.
```
Usage: rawurlencode <url with special characters in it>
```

## Outlook Email from Mailto: generates an E-mail message in Outlook from the terminal
```
Usage: oem <to_list> <cc_list> <bcc_list> <subject> <body>
```
Email lists are comma delimited and can be empty strings ("").  The subject and body require quotation whenever they include spaces.

```
Example:
oem tom@charter.net "" "" \
"Our seasonal key rotation will happen in the next 30 days" \
"Hi Tom,
The keys to our kingdom have to change periodically, and this is one of those times.

Please allow us to provide your best technical contact with the new keys so we all
can keep driving without any interruption.

Moving forward,
The Management
";
```

## keyrot: utility for easing the periodic key-rotation process
```
Usage: keyrot <folder to search for files called key-rotation> <file to use for Email body> <YYYY-MM-DD date_rotation_required> <optional account number to use>

Example:
keyrot "$HOME/Documents/gitlab/quantum-dasp-jobs/outgoing" "$HOME/Documents/gitlab/quantum-dasp-jobs/outgoing/keyrotation_email_template" 2022-02-02
```
- different modes of this utility:
  1. show key-rotation files
  1. pull secrets from AWS based on account used, place them in LOCAL_FOLDER specified in the key-rotation file
  1. generate E-mail messages from template
  1. removes secrets from LOCAL_FOLDER locations
  1. describe secrets, which shows secret metadata and no actual secrets

### This utility searches a folder for files named `key-rotation`.  The values used in this file are specific to an AWS Secret Key to access an S3 location by an outside group.  Sharing with specific users based on their Email address (internal and external users both) using OneDrive allows for LINK_TO_KEYS to be prepared for each key file and/or folder.
- An example of a key-rotation file:
  - https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/outgoing/bi_shared_services/key-rotation
- key-rotation fields:
  - BUSINESS_CONTACT=`<email address(es) of business contact(s)>`
  - TECHNICAL_CONTACT=`<email address(es) of technical contact(s)>`
  - TECHNICAL_DL=`<email address(es) of technical Email Distribution Lists(s)>`
  - SECURE_FOLDER=`<s3 location to which the feed refers>`
  - FEED_DESCRIPTION=`<friendly name and/or description of feed that is easily recognizable>`
  - SECRET_ID=`<secret name in AWS>`
  - LOCAL_FOLDER=`<spot on local mac laptop in OneDrive to copy the secret>`
  - LINK_TO_KEYS=`<link that will be used by email recipient to get secret>`

### The email opened in Outlook using this utility is entirely dependent on the template, *and the values substituted into it from the key-rotation file*, so the key rotation files need to be updated with any new information prior to running this utility.  The template uses values defined in the key-rotation file instead of the corresponding ${VARIABLE_NAME}, as is variable substitution in most shells.
- An example of an Email template for use with the feed-specific key-rotation files:
   - https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/outgoing/keyrotation_email_template


## isDir tests to see if input is a folder, and returns the actual location in cases where there is a link/alias being used, such as Documents and Desktop folders stored in OneDrive; Use $HOME to specify the home folder rather than a ~ (tilda), as $HOME plays better with most shells.
```
isDir <foldername>

isDir $HOME/Documents
```