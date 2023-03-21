import base64
import datetime
import logging
import pickle
import sys

from azure.mgmt.core.tools import resource_id, parse_resource_id
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from azure.mgmt.cosmosdb.models import SqlDatabaseGetResults
from azure.mgmt.subscription import SubscriptionClient
from azure.monitor.ingestion import LogsIngestionClient
from azure.monitor.query import MetricAggregationType, MetricsQueryClient


def get_azure_credential(scope, logging_enable=False, logger=None):
    '''
        Acquires Azure Credential. Credential is specific to a scope 
        (e.g., management, monitor, vault, ...) and cannot be reused 
        across scopes. Credential type is derived automatically 
        and depends on the environment in which code is running.
    '''
    return DefaultAzureCredential(scopes=scope, logging_enable=logging_enable, logger=logger)

def get_azure_subscription_client(credential, logging_enable=False, logger=None):
    '''
        Acquires a new client for interacting with Azure Subscriptions.
        Client is not specific to one subscription. 
    '''
    return SubscriptionClient(credential, logging_enable=logging_enable, logger=logger)

def get_cosmos_mgmt_client(subscription_id, credential, logging_enable=False, logger=None):
    '''
        Acquires Cosmos DB Management client. Client is specific to Azure subscription.
        If HTTP logging is enabled, patch logger to emit remaining API quota 
        that is otherwise redacted.
    '''
    cosmos_client = CosmosDBManagementClient(credential, subscription_id, logging_enable=logging_enable, logger=logger)
    cosmos_client._client._config.http_logging_policy.allowed_header_names.add('x-ms-ratelimit-remaining-subscription-reads')
    return cosmos_client

def get_metrics_client(credential, logging_enable=False, logger=None):
    '''
        Acquires Azure Metrics client. Client can be reused across subscriptions. 
        If HTTP logging is enabled, patch logger to emit remaining API quota 
        that is otherwise redacted.
    '''
    metrics_client = MetricsQueryClient(credential, logging_enable=logging_enable, logger=logger)
    metrics_client._client._config.http_logging_policy.allowed_header_names.add('x-ms-ratelimit-remaining-subscription-reads')
    return metrics_client

def get_monitor_ingest_client(endpoint, credential, logging_enable=False, logger=None):
    '''
        Acquires Azure Monitor Ingestion client. Client is specific to ingestion endpoint.
        One ingestion endpoint can, however, support multiple targets (tables).
    '''
    return LogsIngestionClient(endpoint, credential, logging_enable=logging_enable, logger=logger)

def generate_iso8601_timestamp():
    return datetime.datetime.utcnow().isoformat() + 'Z'

def today_utc():
    d = datetime.datetime.utcnow()
    return datetime.datetime(d.year, d.month, d.day, tzinfo=datetime.timezone.utc)

def serialize_cosmos_object(cosmos_class):
    return base64.b64encode(pickle.dumps(cosmos_class)).decode('utf-8')

def deserialize_cosmos_object(cosmos_class):
    return pickle.loads(base64.b64decode(cosmos_class))
