import json
import logging
import os

import azure.functions as func
from .helper import *

def list_cosmos_database_accounts(subscription_name, cosmos_client, msgout):
    '''
        List available Cosmos DB accounts within a specific subscription.
    '''

    cosmos_accounts = [cosmos_account for cosmos_account in cosmos_client.database_accounts.list()]

    msgout.set(
        [
            json.dumps(
                {
                    'task': 'GetCosmosDatabaseAccountServices', 
                    'rid': cosmos_account.id, 
                    'taskData': {
                        'accountData': serialize_cosmos_object(cosmos_account),
                        'subscriptionName': subscription_name
                    }
                }
            ) 
            for cosmos_account in cosmos_accounts
        ]
    )
