import logging
import json
import traceback

from boto3 import client as boto3_client

lambda_client = boto3_client('lambda')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        arn = event['LambdaArn']
        skip_jobs = event['SkipJobs']
        current_job = event['CurrentJob']
        run_only = event['RunOnly']
        LOGGER.info("Current Job: '{0}', Skip Jobs: '{1}',  ".format(current_job,skip_jobs))

        if not isinstance(skip_jobs,list) and (skip_jobs is not None):
            return {"code": 1, "JobStatus":"Error. Incorrect 'SkipJobs' value"}
        elif not isinstance(run_only,list) and (run_only is not None):
            return {"code": 1, "JobStatus":"Error. Incorrect 'RunOnly' value"}

        if (skip_jobs and (current_job in skip_jobs)) or (run_only and (current_job not in run_only)):
            return {"code": 0, "JobStatus":"skip"}

        lambda_args = {"FunctionName":arn, "InvocationType":"RequestResponse"}

        if 'RunTime' in event:
            run_time = event['RunTime']
            run_time_obj = {"RUN_TIME": run_time}
            run_time_json = json.dumps(run_time_obj)
            lambda_args["Payload"] = run_time_json
        invoke_response = lambda_client.invoke(**lambda_args)
        status = json.loads(invoke_response["Payload"].read())
        status['JobStatus'] = "execute"
        LOGGER.info("Arn: '{0}' Response from lambda: {1}" .format(arn, status))
        return status

    except Exception as exceptions:
        trace = traceback.format_exc()
        response_fail_message = "Failed to execute: {0}\n\n{1}".format(str(exceptions), trace)
        LOGGER.error(response_fail_message)
        return {'code': 1, 'msg': response_fail_message}