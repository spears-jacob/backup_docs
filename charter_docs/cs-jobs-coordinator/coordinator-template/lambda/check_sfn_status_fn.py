import logging
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    preArn = event['steps']
    
    if 'executionArn' in preArn:
        arn = preArn['executionArn']
    else:
        LOGGER.info ("ExecutionArn is not exist")
        return {"code": "ExecutionArn is required"}
        
    step_func = boto3.client('stepfunctions')
    response = step_func.describe_execution(executionArn=arn)
    status = response['status']
    LOGGER.info("Arn: '{0}' Response from lambda: {1}".format(arn, status))
    
    if status == 'SUCCEEDED':
        return {"code": "Success"}
    elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
        return {"code": "Fail"}
    else:
        return {"code": "Waiting"}