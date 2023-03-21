import json
import logging
import typing

import azure.functions as func

def main(timer: func.TimerRequest, msgout: func.Out[typing.List[str]]):
    '''
        Wakes up on a timer and kickstarts the execution of subsequent tasks by 
        submitting a message to Azure Storage Queue.
    '''

    msgout.set([json.dumps({'task': 'ListVisibleSubscriptions'})])
