import json
import logging
import os

import azure.functions as func
from .helper import *

def get_cosmos_database_throughput(resource_group, account_name, database_name, cosmos_database, api_kind, cosmos_client, monitor_client, msgout):
    '''
        Get database-level provisioned throughput, if available.
    '''

    try:
        if api_kind == 'NoSQL':
            database_throughput = cosmos_client.sql_resources.get_sql_database_throughput(resource_group_name=resource_group, account_name=account_name, database_name=database_name)
        elif api_kind == 'Mongo':
            database_throughput = cosmos_client.mongo_db_resources.get_mongo_db_database_throughput(resource_group_name=resource_group, account_name=account_name, database_name=database_name)
        elif api_kind == 'Cassandra':
            database_throughput = cosmos_client.cassandra_resources.get_cassandra_keyspace_throughput(resource_group_name=resource_group, account_name=account_name, keyspace_name=database_name)
        elif api_kind == 'Table':
            # Azure Cosmos DB for Table does not support database shared throughput.
            # Hence, let's raise an exception and set throughput as dedicated.
            raise ResourceNotFoundError 
        elif api_kind == 'Gremlin':
            database_throughput = cosmos_client.gremlin_resources.get_gremlin_database_throughput(resource_group_name=resource_group, account_name=account_name, database_name=database_name)
        else:
            raise ValueError('Received unexpected input.')
            
        
        database_throughput_mode = 'Shared'
        if database_throughput.resource.autoscale_settings is not None:
            database_throughput_type = 'Autoscale'
            database_throughput_value = database_throughput.resource.autoscale_settings.max_throughput
        else:
            database_throughput_type = 'Manual'
            database_throughput_value = database_throughput.resource.throughput

    except ResourceNotFoundError as e:
        # DB does not use shared throughput
        database_throughput_mode = 'Dedicated'
        database_throughput_type = None
        database_throughput_value = None
    except HttpResponseError as e:
        # Check if account is serverless
        if e.status_code == 400 and 'Reading or replacing offers is not supported for serverless accounts.' in e.message:
            database_throughput_mode = 'Serverless'
            database_throughput_type = 'Serverless'
            database_throughput_value = None
        else:
            raise ValueError('Received unexpected input.')

    time_generated = generate_iso8601_timestamp()
    data = [
        {
            'TimeGenerated': time_generated,
            'DatabaseAccountName': account_name,
            'DatabaseName': database_name,
            'DatabaseThroughputMode': database_throughput_mode, 
            'DatabaseThroughputType': database_throughput_type, 
            'DatabaseThroughput': database_throughput_value,
            'AdditionalData': cosmos_database.as_dict()
        }
    ]

    monitor_client.upload(
        rule_id=os.environ['AzureMonitorDataCollectionRuleIdDatabasesConfig'],
        stream_name=os.environ['AzureMonitorDataCollectionStreamNameDatabasesConfig'],
        logs=data
    )

    msgout.set(
        [
            json.dumps(
                {
                    'task': 'ListCosmosContainers', 
                    'rid': cosmos_database.id, 
                    'taskData': {
                        'APIKind': api_kind
                    }
                }
            ) 
        ]
    )
