Process Dates Testing Instructions:

Upon making any changes to the process_dates.sh file, please run this test script to determine that you get the expected results:

1. Open a new Terminal
2. Log into the Ad-Hoc Cluster (start_emr_ssh is the easiest shortcut if you've set the alias)
3. Copy local files to cluster:
  3.1 In a separate Terminal, navigate to local folder where file exist (~/quantum-dasp-jobs/util/common_files/process_dates_jar_files)
  3.2 RUN: cp2r file.name (You must have the "cp2r" and "cp2l" aliases set up. Do this for each individual file, listed below)
      cp2r pd_test.sh
      cp2r process_dates.sh
      cp2r unix_timezones.tsv
      cp2r tzp
      cp2r process_dates_test_output.sh
4. In your Ad-Hoc Cluster window, check to see that the files have copied correctly with "ls"
5. RUN: bash pd_test.sh
6. A new test output file will be in the cluster if the test succeeds.
7. Copy the new test output file back to local
  7.1 In your local Terminal, navigate to the test output directory (~/quantum-dasp-jobs/util/common_files/pd_test_output)
  4.2 RUN: cp2l file.name (ex: cp2l 2021_01_15_pd_test.txt)
