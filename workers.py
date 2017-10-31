from config import Config as config
from job_states import JobStates
from utils import *

import redis
from rq import get_current_job
import time
import json

import sys
import random

from sklearn.metrics import mean_squared_error, r2_score
import numpy as np

import requests
import helpers
import uuid

from compute_score import grade_predictions

POOL = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=config.redis_db)


def _obtain_presigned_url(filename, context):
    return helpers.obtain_presigned_url(filename)

def grade_submission(data, _context):
    file_key = data["file_key"]
    # Start the download of the file locally to the temp folder
    _update_job_event(_context, job_info_template(_context,"Beginning grading of the submission"))
    local_file_path = helpers.download_file_from_s3(file_key)


    scores = grade_predictions(local_file_path, config.GOLD_LABEL_PATH, _context)
    for _key in scores.keys():
        # converting to simple `floats` (even if we loose a bit of precision)
        # as the default JSON serization did not work with numpy floats
        scores[_key] = float(scores[_key])

    _update_job_event(_context, job_info_template(_context, "Scores Computed Successfully !!"))
    _update_job_event(_context, job_info_template(_context, "IPS : {}".format(scores["ips"])))
    _update_job_event(_context, job_info_template(_context, "IPS_std: {}".format(scores["ips_std"])))
    _update_job_event(_context, job_info_template(_context, "ImpWt : {}".format(scores["impwt"])))
    _update_job_event(_context, job_info_template(_context, "ImpWt_std: {}".format(scores["impwt_std"])))
    _update_job_event(_context, job_info_template(_context, "SNIPS : {}".format(scores["snips"])))
    _update_job_event(_context, job_info_template(_context, "SNIPS_std: {}".format(scores["snips_std"])))
    _update_job_event(_context, job_info_template(_context, "Uploading scores to the leaderboard...."))
    #Upload to CrowdAI Leaderboard
    headers = {'Authorization' : 'Token token='+config.CROWDAI_TOKEN, "Content-Type":"application/vnd.api+json"}
    _payload = {}
    _payload["score"] = scores["ips"]
    _payload["score_secondary"] = scores["ips_std"]
    _payload["metadata"] = scores
    _payload["metadata"]["file_key"] = file_key
    _payload['challenge_client_name'] = config.challenge_id
    _payload['api_key'] = _context['api_key']
    _payload['grading_status'] = 'graded'
    _payload['comment'] = "" #TODO: Allow participants to add comments to submissions

    print "Making POST request...."
    r = requests.post(config.CROWDAI_GRADER_URL, params=_payload, headers=headers, verify=False)
    print "Status Code : ",r.status_code
    if r.status_code == 202:
        data = json.loads(r.text)
        submission_id = str(data['submission_id'])
        _context['redis_conn'].set(config.challenge_id+"::submissions::"+submission_id, json.dumps(_payload))
        _update_job_event(_context, job_info_template(_context, "Scores Submited Successfully !!! "))
        del scores["file_key"]
        _update_job_event(_context, job_complete_template(_context, scores))
    else:
        raise Exception(r.text)

def _update_job_event(_context, data):
    """
        Helper function to serialize JSON
        and make sure redis doesnt messup JSON validation
    """
    redis_conn = _context['redis_conn']
    response_channel = _context['response_channel']
    data['data_sequence_no'] = _context['data_sequence_no']

    redis_conn.rpush(response_channel, json.dumps(data))

def job_execution_wrapper(data):
    redis_conn = redis.Redis(connection_pool=POOL)
    job = get_current_job()

    _context = {}
    _context['redis_conn'] = redis_conn
    _context['response_channel'] = data['broker_response_channel']
    _context['job_id'] = job.id
    _context['data_sequence_no'] = data['data_sequence_no']
    _context['api_key'] = data['extra_params']['api_key']

    # Register Job Running event
    _update_job_event(_context, job_running_template(_context['data_sequence_no'], job.id))
    result = {}
    try:
        if data["function_name"] == "obtain_presigned_url":
            filename = "{}.gz".format(uuid.uuid4())
            file_key, presigned_url = _obtain_presigned_url(filename, _context)
            _update_job_event(_context, job_complete_template(_context, {"presigned_url":presigned_url, "file_key":file_key}))
        elif data["function_name"] == "grade_submission":
            grade_submission(data["data"], _context)
        else:
            _error_object = job_error_template(job.id, "Function not implemented error")
            _update_job_event(_context, job_error_template(job.id, result))
            result = _error_object
    except Exception as e:
        _error_object = job_error_template(_context['data_sequence_no'], job.id, str(e))
        _update_job_event(_context, _error_object)
    return result
