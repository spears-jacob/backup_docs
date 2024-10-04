COORDINATOR_TEMPLATE_DIR=$1

apk add --update zip
mkdir -p $COORDINATOR_TEMPLATE_DIR/lambda/pkg
zip -j $COORDINATOR_TEMPLATE_DIR/lambda/pkg/check_sfn_status_fn.zip $COORDINATOR_TEMPLATE_DIR/lambda/check_sfn_status_fn.py
zip -j $COORDINATOR_TEMPLATE_DIR/lambda/pkg/check_tasks_status_fn.zip $COORDINATOR_TEMPLATE_DIR/lambda/check_tasks_status_fn.py
zip -j $COORDINATOR_TEMPLATE_DIR/lambda/pkg/lambda_executor_fn.zip $COORDINATOR_TEMPLATE_DIR/lambda/lambda_executor_fn.py
zip -j $COORDINATOR_TEMPLATE_DIR/lambda/pkg/lambda_sfn_adapter_fn.zip $COORDINATOR_TEMPLATE_DIR/lambda/lambda_sfn_adapter_fn.py
zip -j $COORDINATOR_TEMPLATE_DIR/lambda/pkg/mc_input_formatter_fn.zip $COORDINATOR_TEMPLATE_DIR/lambda/mc_input_formatter_fn.py