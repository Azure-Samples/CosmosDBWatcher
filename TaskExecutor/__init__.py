import json
import logging
import os
import typing

import azure.functions as func
from .helper import *
from .list_visible_subscriptions import list_visible_subscriptions
from .list_cosmos_database_accounts import list_cosmos_database_accounts
from .get_cosmos_database_account_services import get_cosmos_database_account_services
from .list_cosmos_databases import list_cosmos_databases
from .get_cosmos_database_throughput import get_cosmos_database_throughput
from .list_cosmos_containers import list_cosmos_containers
from .get_cosmos_container_throughput import get_cosmos_container_throughput
from .get_cosmos_container_metrics import get_cosmos_container_metrics

mgmt_credential = None
subscription_client = None
cosmos_clients = {}
monitor_credential = None
monitor_client = None
metrics_client = None

def main(msgin: func.QueueMessage, msgout: func.Out[typing.List[str]]):
    '''
        Wrapper function that delegates work.
    '''

    _input = json.loads(msgin.get_body().decode('utf-8'))
    task = _input['task']
    task_data = _input.get('taskData')
    rid = parse_resource_id(_input.get('rid'))
    subscription_id = rid.get('subscription')
    resource_group = rid.get('resource_group')
    account_name = rid.get('name')
    database_name = rid.get('child_name_1')
    container_name = rid.get('child_name_2')

    global mgmt_credential, subscription_client, cosmos_clients, monitor_credential, monitor_client, metrics_client
    if mgmt_credential is None:
        mgmt_credential = get_azure_credential(scope='https://management.azure.com/.default')
    if subscription_client is None:
        subscription_client = get_azure_subscription_client(mgmt_credential)
    if subscription_id is not None and subscription_id not in cosmos_clients:
        cosmos_clients[subscription_id] = get_cosmos_mgmt_client(subscription_id, mgmt_credential)
    if monitor_credential is None:
        monitor_credential = get_azure_credential(scope='https://monitor.azure.com/.default')
    if monitor_client is None:
        monitor_client = get_monitor_ingest_client(os.environ['AzureMonitorDataCollectionEndpoint'], monitor_credential)
    if metrics_client is None:
        metrics_client = get_metrics_client(mgmt_credential)

    if task == 'ListVisibleSubscriptions':
        list_visible_subscriptions(subscription_client, msgout)
    elif task == 'ListCosmosDatabaseAccounts':
        list_cosmos_database_accounts(task_data['subscriptionName'], cosmos_clients[subscription_id], msgout)
    elif task == 'GetCosmosDatabaseAccountServices':
        get_cosmos_database_account_services(subscription_id, task_data['subscriptionName'], resource_group, account_name, deserialize_cosmos_object(task_data['accountData']), cosmos_clients[subscription_id], monitor_client, msgout)
    elif task == 'ListCosmosDatabases':
        list_cosmos_databases(resource_group, account_name, _input['rid'], task_data['APIKind'], cosmos_clients[subscription_id], msgout)
    elif task == 'GetCosmosDatabaseThroughput':
        get_cosmos_database_throughput(resource_group, account_name, database_name, deserialize_cosmos_object(task_data['databaseData']), task_data['APIKind'], cosmos_clients[subscription_id], monitor_client, msgout)
    elif task == 'ListCosmosContainers':
        list_cosmos_containers(resource_group, account_name, database_name, task_data['APIKind'], cosmos_clients[subscription_id], msgout)
    elif task == 'GetCosmosContainerThroughput':
        get_cosmos_container_throughput(resource_group, account_name, database_name, container_name, deserialize_cosmos_object(task_data['containerData']), task_data['APIKind'], cosmos_clients[subscription_id], monitor_client, msgout)
    elif task == 'GetCosmosContainerMetrics':
        account_rid = resource_id(subscription=subscription_id, resource_group=resource_group, namespace=rid['namespace'], type=rid['type'], name=account_name)
        get_cosmos_container_metrics(task_data['metricType'], account_rid, account_name, database_name, container_name, task_data.get('isSharedThroughput'), metrics_client, monitor_client)
    else:
        raise ValueError('Received unexpected input.')
