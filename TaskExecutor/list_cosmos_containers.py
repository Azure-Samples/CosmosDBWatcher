import json
import logging
import os

import azure.functions as func
from .helper import *

def list_cosmos_containers(resource_group, account_name, database_name, api_kind, cosmos_client, msgout):
    '''
        List available containers wtihin a specific Cosmos DB database.
    '''
    
    if api_kind == 'NoSQL':
        cosmos_containers = [cosmos_container for cosmos_container in cosmos_client.sql_resources.list_sql_containers(resource_group_name=resource_group, account_name=account_name, database_name=database_name)]
    elif api_kind == 'Mongo':
        cosmos_containers = [cosmos_container for cosmos_container in cosmos_client.mongo_db_resources.list_mongo_db_collections(resource_group_name=resource_group, account_name=account_name, database_name=database_name)]
    elif api_kind == 'Cassandra':
        cosmos_containers = [cosmos_container for cosmos_container in cosmos_client.cassandra_resources.list_cassandra_tables(resource_group_name=resource_group, account_name=account_name, keyspace_name=database_name)]
    elif api_kind == 'Table':
        cosmos_containers = [cosmos_container for cosmos_container in cosmos_client.table_resources.list_tables(resource_group_name=resource_group, account_name=account_name)]
    elif api_kind == 'Gremlin':
        cosmos_containers = [cosmos_container for cosmos_container in cosmos_client.gremlin_resources.list_gremlin_graphs(resource_group_name=resource_group, account_name=account_name, database_name=database_name)]
    else:
        raise ValueError('Received unexpected input.')

    msg = []
    for cosmos_container in cosmos_containers:
        if not api_kind == 'Table':
            rid = cosmos_container.id
        else:
            rid = parse_resource_id(cosmos_container.id)
            rid.pop('child_type_1')
            rid['child_type_2'] = 'colls'
            rid['child_name_2'] = rid.pop('child_name_1')
            rid = resource_id(**rid, child_type_1='dbs', child_name_1='TablesDB')
        msg.append(json.dumps({'task': 'GetCosmosContainerThroughput', 'rid': rid, 'taskData': {'containerData': serialize_cosmos_object(cosmos_container), 'APIKind': api_kind}}))
    msgout.set(msg)
