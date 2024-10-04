# PI Datalake User Utilities

#### In order to ensure most scripts in the terminal folder will run
- Make sure to have AWS credentials set beforehand
- See the common_utilities readme for requirements for several scripts: https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md

## Much of the json parsing with jq that is needed for working with AWS can be assisted using the following resources.
- standalone: [jpterm/jmespath](https://github.com/jmespath/jmespath.terminal)
- on line: https://jqterm.com, https://jqkungfu.com/, and https://jqplay.org/


## Please prepare a folder for each utility with a formatted README.md for documentation.
Formatting markup documentation is found at the following location.
 - https://gitlab.spectrumflow.net/help/user/markdown.md


## In order to use any script here in a jobs repository
- Specify ```JOB_TEMPLATE_REF: "v2.5.3"``` or greater in the job repository ```dot_gitlab-ci.yml``` file variables section
- Optionally specify a particular ```PI_UTILS_REF``` tag or branch in the same place as above if that is preferred.  ```PI_UTIL_REF: "v.1.4.1"```
- Then, in the jobs repo, add lines referring to the specific files that should be included in the job when deployed in the job ```dot_gitlab-ci.yml``` file in the build section, such as is found in the [scp-portals-daily job](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/scp/scp_portals_daily/dot_gitlab-ci.yml#L14-19).
```
    PI_DL_USER_UTILS: >
      spark/pyspark_sql_wrapper.py
      spark/spark_run.sh
      spark/conf/spark-defaults.conf.addendums
      spark/conf/log4j2.properties.addendums
      emr/emr_utils.sh
```
