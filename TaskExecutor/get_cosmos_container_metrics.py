import datetime
import json
import logging
import os

import azure.functions as func
from .helper import *

def get_cosmos_container_metrics(metric_type, account_rid, account_name, database_name, container_name, is_shared_throughput, metrics_client, monitor_client):
    '''
        Based on metric_type retrieves either request, throughput, storage, or 
        partition key ranges metrics from Azure Metrics.
    '''

    #TODO: Handle special case when all containers within a shared throughput database use dedicated throughput.

    if metric_type == 'Requests':
        metrics = get_cosmos_container_metrics_requests(account_rid, database_name, container_name, metrics_client)
    elif metric_type == 'ThroughputStorage':
        metrics = get_cosmos_container_metrics_throughput_storage(account_rid, database_name, container_name, is_shared_throughput, metrics_client)
    elif metric_type == 'PartitionKeyUsage':
        metrics = get_cosmos_container_metrics_pkusage(account_rid, database_name, container_name, is_shared_throughput, metrics_client)
    else:
        raise ValueError('Received unexpected input.')

    time_generated = generate_iso8601_timestamp()
    data = [
        {
            'TimeGenerated': time_generated,
            'DatabaseAccountName': account_name,
            'DatabaseName': database_name,
            'ContainerName': container_name,
            'MetricTimestamp': metric[0],
            'MetricName': metric[1],
            'MetricValue': metric[2],
            'MetricMetadata': metric[3]
        }
        for metric in metrics
    ]

    monitor_client.upload(
        rule_id=os.environ['AzureMonitorDataCollectionRuleIdContainersMetrics'],
        stream_name=os.environ['AzureMonitorDataCollectionStreamNameContainersMetrics'],
        logs=data
    )

def get_cosmos_container_metrics_requests(account_rid, database_name, container_name, metrics_client):

    container_metrics = metrics_client.query_resource(
        resource_uri=account_rid,
        metric_names=[
            'TotalRequests',
            'TotalRequestUnits',
        ],
        metric_namespace='microsoft.documentdb/databaseaccounts',
        timespan=(today_utc()-datetime.timedelta(days=1), today_utc()),
        granularity=datetime.timedelta(minutes=1),
        aggregations=[
            MetricAggregationType.COUNT,
        ],
        filter=f"DatabaseName eq '{database_name}' and CollectionName eq '{container_name}' and OperationType eq '*' and Region eq '*' and StatusCode eq '*'"
    )

    metrics_results = []
    
    for metric in container_metrics.metrics:
        for time_series_element in metric.timeseries:
            for metric_value in time_series_element.data:
                metadata = {
                    'OperationType': time_series_element.metadata_values['operationtype'],
                    'Region': time_series_element.metadata_values['region'],
                    'StatusCode': int(time_series_element.metadata_values['statuscode'])
                }
                metrics_results.append((metric_value.timestamp.replace(tzinfo=datetime.timezone.utc).isoformat(), metric.name, metric_value.count, metadata))

    return metrics_results

def get_cosmos_container_metrics_throughput_storage(account_rid, database_name, container_name, is_shared_throughput, metrics_client):

    metrics_results = []

    if not is_shared_throughput:
        container_metrics = metrics_client.query_resource(
            resource_uri=account_rid,
            metric_names=[
                'ProvisionedThroughput',
                'AutoscaleMaxThroughput',
                'DataUsage',
                'IndexUsage',
                'DocumentCount'
            ],
            metric_namespace='microsoft.documentdb/databaseaccounts',
            timespan=(today_utc()-datetime.timedelta(days=1), today_utc()),
            granularity=datetime.timedelta(minutes=5),
            filter=f"DatabaseName eq '{database_name}' and CollectionName eq '{container_name}'"
        )
        
        for metric in container_metrics.metrics:
            for time_series_element in metric.timeseries:
                for metric_value in time_series_element.data:
                    if metric.name == 'ProvisionedThroughput' or metric.name == 'AutoscaleMaxThroughput':
                        metrics_results.append((metric_value.timestamp.replace(tzinfo=datetime.timezone.utc).isoformat(), metric.name, metric_value.maximum, None))
                    else:
                        metrics_results.append((metric_value.timestamp.replace(tzinfo=datetime.timezone.utc).isoformat(), metric.name, metric_value.total, None))
    else:
        container_metrics = metrics_client.query_resource(
            resource_uri=account_rid,
            metric_names=[
                'ProvisionedThroughput',
                'AutoscaleMaxThroughput',
            ],
            metric_namespace='microsoft.documentdb/databaseaccounts',
            timespan=(today_utc()-datetime.timedelta(days=1), today_utc()),
            granularity=datetime.timedelta(minutes=5),
            filter=f"DatabaseName eq '{database_name}' and CollectionName eq '__Empty'"
        )

        for metric in container_metrics.metrics:
            for time_series_element in metric.timeseries:
                for metric_value in time_series_element.data:
                    metrics_results.append((metric_value.timestamp.replace(tzinfo=datetime.timezone.utc).isoformat(), metric.name, metric_value.maximum, None))

        container_metrics = metrics_client.query_resource(
            resource_uri=account_rid,
            metric_names=[
                'DataUsage',
                'DocumentCount'
            ],
            metric_namespace='microsoft.documentdb/databaseaccounts',
            timespan=(today_utc()-datetime.timedelta(days=1), today_utc()),
            granularity=datetime.timedelta(minutes=5),
            filter=f"DatabaseName eq '{database_name}' and CollectionName eq '{container_name}'"
        )

        for metric in container_metrics.metrics:
            for time_series_element in metric.timeseries:
                for metric_value in time_series_element.data:
                    metrics_results.append((metric_value.timestamp.replace(tzinfo=datetime.timezone.utc).isoformat(), metric.name, metric_value.total, None))

        container_metrics = metrics_client.query_resource(
            resource_uri=account_rid,
            metric_names=[
                'IndexUsage',
            ],
            metric_namespace='microsoft.documentdb/databaseaccounts',
            timespan=(today_utc()-datetime.timedelta(days=1), today_utc()),
            granularity=datetime.timedelta(minutes=5),
            filter=f"DatabaseName eq '{database_name}'"
        )

        for metric in container_metrics.metrics:
            for time_series_element in metric.timeseries:
                for metric_value in time_series_element.data:
                    metrics_results.append((metric_value.timestamp.replace(tzinfo=datetime.timezone.utc).isoformat(), metric.name, metric_value.total, None))

    return metrics_results


def get_cosmos_container_metrics_pkusage(account_rid, database_name, container_name, is_shared_throughput, metrics_client):

    container_metrics = metrics_client.query_resource(
        resource_uri=account_rid,
        metric_names=[
            'NormalizedRUConsumption',
        ],
        metric_namespace='microsoft.documentdb/databaseaccounts',
        timespan=(today_utc()-datetime.timedelta(days=1), today_utc()),
        granularity=datetime.timedelta(minutes=1),
        filter=f"DatabaseName eq '{database_name}' and CollectionName eq '{container_name if not is_shared_throughput else '<empty>'}' and Region eq '*' and PartitionKeyRangeId eq '*' and PhysicalPartitionId eq '*'"
    )

    metrics_results = []

    for metric in container_metrics.metrics:
        for time_series_element in metric.timeseries:
            for metric_value in time_series_element.data:
                metadata = {
                    'Region': time_series_element.metadata_values['region'],
                    'PartitionKeyRangeId': time_series_element.metadata_values['partitionkeyrangeid'],
                    'PhysicalPartitionId': time_series_element.metadata_values['physicalpartitionid']
                }
                metrics_results.append((metric_value.timestamp.replace(tzinfo=datetime.timezone.utc).isoformat(), metric.name, metric_value.maximum, metadata))

    return metrics_results
