import json
import logging
import copy
from datetime import timedelta, date, datetime

# Examples of current formats of multidate coordinator inputs:
# 1) For dates 28. 29, 01:
#   {
# 	"RUN_TIMES": [
# 		"2020-02-28",
# 		"2020-02-29",
# 		"2020-03-01"
# 	],
# 	"params": {
# 		"SKIP_JOBS": [],
# 	    "RUN_ONLY": [
# 	      "idm-paths"
# 	    ]
# 	}
# }
# 2) For dates: 28, 29, 01, 02, 03, 04, 05
# {
# 	"DATE_RANGE": [
# 		"2020-02-28",
# 		"2020-03-05"
# 	],
# 	"params": {
# 		"SKIP_JOBS": [],
# 	    "RUN_ONLY": [
# 	      "idm-paths"
# 	    ]
# 	}
# }
# 3) Old format: 28, 29 , 30
# {
#   "params":[
#     {
#       "RUN_TIME":"2020-01-28",
#       "SKIP_JOBS":[
#       ],
#       "RUN_ONLY":[
#         "idm-paths"
#       ]
#     },
#     {
#       "RUN_TIME":"2020-01-29",
#       "SKIP_JOBS":[
#       ],
#       "RUN_ONLY":[
#         "idm-paths"
#       ]
#     },
#     {
#       "RUN_TIME":"2020-01-30",
#       "SKIP_JOBS":[
#       ],
#       "RUN_ONLY":[
#         "idm-paths"
#       ]
#     }
# }
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def date_str_to_object(date_string):
    date_object = datetime.strptime(date_string,'%Y-%m-%d')
    return date_object

def date_range_converter(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

def event_separator(event):
    params = event["params"]
    new_event = copy.deepcopy(event)
    new_params = []
    for run_time in event["RUN_TIMES"]:
        params_copy = copy.deepcopy(params)
        params_copy['RUN_TIME'] = run_time
        new_params.append(params_copy)
    new_event["params"] = new_params
    del new_event["RUN_TIMES"]
    return new_event

def lambda_handler(event, context):
    LOGGER.info("Event before separation: {0}".format(event))
    if "DATE_RANGE" in event:
        start_date, end_date = event["DATE_RANGE"]
        start_date = date_str_to_object(start_date)
        end_date = date_str_to_object(end_date)
        dates_in_range = []
        for new_date in date_range_converter(start_date,end_date):
            dates_in_range.append(new_date.strftime("%Y-%m-%d"))
        event["RUN_TIMES"] = dates_in_range
        
    if "RUN_TIMES" in event:
        event = event_separator(event)
        LOGGER.info("Event after separation: {0}".format(event))
    return event
