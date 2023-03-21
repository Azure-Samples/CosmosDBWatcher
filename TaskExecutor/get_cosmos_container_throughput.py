import json
import logging
import os

import azure.functions as func
from .helper import *

def get_cosmos_container_throughput(resource_group, account_name, database_name, container_name, cosmos_container, api_kind, cosmos_client, monitor_client, msgout):
    '''
        Get container provisioned throughput, if available.
    '''
    
    try:
        if api_kind == 'NoSQL':
            container_throughput = cosmos_client.sql_resources.get_sql_container_throughput(resource_group_name=resource_group, account_name=account_name, database_name=database_name, container_name=container_name)
        elif api_kind == 'Mongo':
            container_throughput = cosmos_client.mongo_db_resources.get_mongo_db_collection_throughput(resource_group_name=resource_group, account_name=account_name, database_name=database_name, collection_name=container_name)
        elif api_kind == 'Cassandra':
            container_throughput = cosmos_client.cassandra_resources.get_cassandra_table_throughput(resource_group_name=resource_group, account_name=account_name, keyspace_name=database_name, table_name=container_name)
        elif api_kind == 'Table':
            container_throughput = cosmos_client.table_resources.get_table_throughput(resource_group_name=resource_group, account_name=account_name, table_name=container_name)
        elif api_kind == 'Gremlin':
            container_throughput = cosmos_client.gremlin_resources.get_gremlin_graph_throughput(resource_group_name=resource_group, account_name=account_name, database_name=database_name, graph_name=container_name)
        else:
            raise ValueError('Received unexpected input.')

        container_throughput_mode = 'Dedicated'
        if container_throughput.resource.autoscale_settings is not None:
            container_throughput_type = 'Autoscale'
            container_throughput_value = container_throughput.resource.autoscale_settings.max_throughput
        else:
            container_throughput_type = 'Manual'
            container_throughput_value = container_throughput.resource.throughput

    except ResourceNotFoundError as e:
        # container uses shared throughput
        container_throughput_mode = 'Shared'
        container_throughput_type = None
        container_throughput_value = None
    
    except HttpResponseError as e:
        # Check if account is serverless
        if e.status_code == 400 and 'Reading or replacing offers is not supported for serverless accounts.' in e.message:
            container_throughput_mode = 'Serverless'
            container_throughput_type = 'Serverless'
            container_throughput_value = None
        else:
            raise ValueError('Received unexpected input.')

    time_generated = generate_iso8601_timestamp()
    data = [
        {
            'TimeGenerated': time_generated,
            'DatabaseAccountName': account_name,
            'DatabaseName': database_name,
            'ContainerName': container_name,
            'ContainerThroughputMode': container_throughput_mode, 
            'ContainerThroughputType': container_throughput_type, 
            'ContainerThroughput': container_throughput_value,
            'ContainerIndexingIsDefault': indexing_isdefault(cosmos_container, api_kind),
            'ContainerTTL': container_ttl(cosmos_container, api_kind),
            'AdditionalData': cosmos_container.as_dict()
        }
    ]

    monitor_client.upload(
        rule_id=os.environ['AzureMonitorDataCollectionRuleIdContainersConfig'],
        stream_name=os.environ['AzureMonitorDataCollectionStreamNameContainersConfig'],
        logs=data
    )

    msg = []
    msg.append(json.dumps({'task': 'GetCosmosContainerMetrics', 'rid': cosmos_container.id, 'taskData': {'metricType': 'Requests', 'APIKind': api_kind}}))
    msg.append(json.dumps({'task': 'GetCosmosContainerMetrics', 'rid': cosmos_container.id, 'taskData': {'metricType': 'ThroughputStorage', 'APIKind': api_kind, 'isSharedThroughput': True if container_throughput_mode == 'Shared' else False}}))
    msg.append(json.dumps({'task': 'GetCosmosContainerMetrics', 'rid': cosmos_container.id, 'taskData': {'metricType': 'PartitionKeyUsage', 'APIKind': api_kind, 'isSharedThroughput': True if container_throughput_mode == 'Shared' else False}}))
    msgout.set(msg)


def indexing_isdefault(cosmos_container, api_kind):
    '''
        Checks whether Cosmos DB container policy matches default indexing policy.
    '''
    if api_kind == 'NoSQL':
        default_indexing_policy  = {'automatic': True, 'indexing_mode': 'consistent', 'included_paths': [{'path': '/*'}], 'excluded_paths': [{'path': '/"_etag"/?'}]}
        isdefault = cosmos_container.resource.indexing_policy.as_dict() == default_indexing_policy 
    elif api_kind == 'Mongo':
        default_indexing_policy = [{'key': {'keys': ['_id']}}]
        isdefault = [index.as_dict() for index in cosmos_container.resource.indexes] == default_indexing_policy
    elif api_kind == 'Cassandra':
        # The resource provider does not return indexing policy settings.
        isdefault = True
    elif api_kind == 'Table':
        default_indexing_policy  = {'automatic': True, 'indexing_mode': 'consistent', 'included_paths': [{'path': '/*'}], 'excluded_paths': [{'path': '/"_etag"/?'}]}
        # The resource provider does not return indexing policy settings.
        isdefault = True
    elif api_kind == 'Gremlin':
        default_indexing_policy  = {'automatic': True, 'indexing_mode': 'consistent', 'included_paths': [{'path': '/*'}], 'excluded_paths': [{'path': '/"_etag"/?'}]}
        isdefault = cosmos_container.resource.indexing_policy.as_dict() == default_indexing_policy
    else:
        raise ValueError('Received unexpected input.')

    return isdefault


def container_ttl(cosmos_container, api_kind):
    if api_kind == 'NoSQL':
        container_ttl = cosmos_container.resource.default_ttl
    elif api_kind == 'Mongo':
        container_ttl = None
        indexes = [index.as_dict() for index in cosmos_container.resource.indexes]
        for index in indexes:
            for key in index['key']['keys']:
                if key == '_ts':
                    if index.get('options') is not None:
                        container_ttl = index['options'].get('expireAfterSeconds') 
    elif api_kind == 'Cassandra':
        container_ttl = None
    elif api_kind == 'Table':
        container_ttl = None
    elif api_kind == 'Gremlin':
        container_ttl = cosmos_container.resource.default_ttl
    else:
        raise ValueError('Received unexpected input.')
    
    return container_ttl