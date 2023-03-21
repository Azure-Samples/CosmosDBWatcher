import json
import logging
import os

import azure.functions as func
from .helper import *

def list_visible_subscriptions(subscription_client, msgout):
    '''
        List all Azure Subscriptions to which this Azure Function 
        has been granted access.
    '''
    subscriptions = [subscription for subscription in subscription_client.subscriptions.list()]

    msgout.set(
        [
            json.dumps(
                {
                    'task': 'ListCosmosDatabaseAccounts',
                    'rid': subscription.id, 
                    'taskData': {
                        'subscriptionName': subscription.display_name
                    }
                } 
            )
            for subscription in subscriptions
        ]
    )