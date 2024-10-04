Example Azkaban project that corresponds to the following chalk page.<br>
https://chalk.charter.com/display/XGDATAMART/Azkaban+-+from+code+to+scheduled+job+quick+start<br>
<p>
Below is an example of a general Azkaban project folder

```
gitlab-repository
\-> project_folder
  \-> bin: contains shell and/or other reusable scripts
      - process_dates.sh - sets date variables, good for day-by-day processing
      - run_jar_file.sh - processes data using ml-jar, specific to ml
  \-> jobs: contains Azkaban-specific job files.  number prefix helps ordering
      - 00_database_init.job: starts hql for setting up environment for the first time
      - 01_parameter_init.job: kicks off shell script to set parameters for the flow
      - 02_ml_azkaban_intro.job: starts hql that does the work, may use parameters from prior step
      - ml_azkaban_intro_end.job: end job, keeps logs organized even if prior jobs change names
  \-> sampleData: specific to ml, used in this example
  \-> src: all the actual HQL files go here
    \-> init: the create tables and views hql files go in this folder
        - create_tables_and_views.hql: 00_database_init calls this, often run only once
      - process_data_here.hql: the actual hive work is done by one or more hql files here
  - Jenkinsfile: allows gitlab to talk to Azkaban once gatekeeper promotes the job
  - project.properties: Azkaban-project wide parameters, always available
  - README.md: good documentation opportunity here
```
