locals {

  fininsh_process_failure_bash_function = <<EOF
      trap "exit 101" TERM
      export TOP_PID=$$

      # Stop execution of execute_hql.sh script
      finish_process_with_failure (){
          kill -s TERM $TOP_PID;
      }
  EOF
}