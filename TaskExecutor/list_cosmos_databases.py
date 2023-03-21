import json
import logging
import os

import azure.functions as func
from .helper import *

def list_cosmos_databases(resource_group, account_name, account_rid, api_kind, cosmos_client, msgout):
    '''
        List available databases within a specific Cosmos DB account.
    '''
    if api_kind == 'NoSQL':
        cosmos_databases = [cosmos_database for cosmos_database in cosmos_client.sql_resources.list_sql_databases(resource_group_name=resource_group, account_name=account_name)]
    elif api_kind == 'Mongo':
        cosmos_databases = [cosmos_database for cosmos_database in cosmos_client.mongo_db_resources.list_mongo_db_databases(resource_group_name=resource_group, account_name=account_name)]
    elif api_kind == 'Cassandra':
        cosmos_databases = [cosmos_database for cosmos_database in cosmos_client.cassandra_resources.list_cassandra_keyspaces(resource_group_name=resource_group, account_name=account_name)]  
    elif api_kind == 'Table':
        # Azure Cosmos DB for Table does not support multiple databases. All tables are in TablesDB. 
        # There does not appear to be any control plane API to get metadata of TablesDB. Hence, let's 
        # instantiate NoSQL DB and only pass id. This is done to support serialization and avoid 
        # needing to handle special cases elsewhere in the repo.
        tablesdb_rid = resource_id(**parse_resource_id(account_rid), child_type_1='dbs', child_name_1='TablesDB')
        tablesdb = SqlDatabaseGetResults()
        tablesdb.__setattr__('id', tablesdb_rid)
        cosmos_databases = [tablesdb]
    elif api_kind == 'Gremlin':
        cosmos_databases = [cosmos_database for cosmos_database in cosmos_client.gremlin_resources.list_gremlin_databases(resource_group_name=resource_group, account_name=account_name)]
    else:
        raise ValueError('Received unexpected input.')

    msgout.set(
        [
            json.dumps(
                {
                    'task': 'GetCosmosDatabaseThroughput', 
                    'rid': cosmos_database.id,
                    'taskData': {
                        'databaseData': serialize_cosmos_object(cosmos_database),
                        'APIKind': api_kind
                    }
                }
            )
            for cosmos_database in cosmos_databases
        ]
    )
