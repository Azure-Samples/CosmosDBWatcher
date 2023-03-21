import datetime
import json
import logging
import os

import azure.functions as func
from .helper import *

def get_cosmos_database_account_services(subscription_id, subscription_name, resource_group, account_name, cosmos_account, cosmos_client, monitor_client, msgout):
    '''
        List all Cosmos DB services within a specific Cosmos DB database account.
    '''

    cosmos_account_services = next(cosmos_client.service.list(resource_group, account_name), None)
    
    time_generated = generate_iso8601_timestamp()
    data = [
        {
            'TimeGenerated': time_generated,
            'SubscriptionId': subscription_id,
            'SubscriptionName': subscription_name,
            'ResourceGroup': resource_group,
            'DatabaseAccountName': account_name,
            'APIKind': get_api_kind(cosmos_account),
            'CapacityMode': get_capacity_mode(cosmos_account),
            'AdditionalData': include_service_data(cosmos_account, cosmos_account_services)
        }
    ]
    
    monitor_client.upload(
        rule_id=os.environ['AzureMonitorDataCollectionRuleIdDatabaseAccountsConfig'],
        stream_name=os.environ['AzureMonitorDataCollectionStreamNameDatabaseAccountsConfig'],
        logs=data
    )

    msgout.set(
        [
            json.dumps(
                {
                    'task': 'ListCosmosDatabases', 
                    'rid': cosmos_account.id,
                    'taskData': {
                        'APIKind': get_api_kind(cosmos_account)
                    }
                }
            )
        ]
    )

def include_service_data(cosmos_account, cosmos_account_services):
    additional_data = cosmos_account.as_dict()
    additional_data['services'] = [] if cosmos_account_services is None else cosmos_account_services.as_dict() 
    return additional_data

def get_api_kind(cosmos_account):
    capabilities = {capability.name for capability in cosmos_account.capabilities}
    account_types = {'EnableMongo': 'Mongo', 'EnableCassandra': 'Cassandra', 'EnableTable': 'Table', 'EnableGremlin': 'Gremlin'}
    return next((account_types[x] for x in account_types if x in capabilities), 'NoSQL')

def get_capacity_mode(cosmos_account):
    capabilities = {capability.name for capability in cosmos_account.capabilities}
    return 'Serverless' if 'EnableServerless' in capabilities else 'Provisioned throughput'



