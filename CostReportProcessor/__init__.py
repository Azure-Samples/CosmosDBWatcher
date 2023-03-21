import csv
import json
import os
import logging
import typing

import azure.functions as func
from ..TaskExecutor.helper import *

def main(blob: func.InputStream):
    '''
        Extracts Cosmos DB cost data from newly arrived cost report and uploads
        it to Azure Monitor. Processing happens in chunks to limit memory usage.
    '''
    
    monitor_credential = get_azure_credential(scope='https://monitor.azure.com/.default')
    monitor_client = get_monitor_ingest_client(os.environ['AzureMonitorDataCollectionEndpoint'], monitor_credential)
    time_generated = generate_iso8601_timestamp()

    chunk_size = 4194304 # 4194304 = 4MB
    file_buffer = bytearray(chunk_size)
    batch_size = 10000
    row_buffer = []
    header = []
    line_fragment = ''
    while True:
        file_buffer[:] = blob.read(chunk_size)
        lines = file_buffer.decode('utf-8-sig').splitlines()
        
        if not(header):
            header.extend(lines[0].split(','))
            lines = lines[1:]
        
        if line_fragment:
            lines[0] = line_fragment + lines[0]
            line_fragment = ''
        
        if len(file_buffer) == chunk_size:
            line_fragment = lines[-1]
            lines = lines[:-1]

        reader = csv.DictReader(lines, header)
        for row in reader:
            if row['ConsumedService'].lower() == 'microsoft.documentdb':
                if row['AdditionalInfo']:
                    row['AdditionalInfo'] = json.loads(row['AdditionalInfo'])
                else:
                    row['AdditionalInfo'] = {}
                if len(row_buffer) == batch_size:
                    upload_cost_data(row_buffer, time_generated, monitor_client)
                    row_buffer.clear()
                else:
                    row_buffer.append(row)

        if len(file_buffer) < chunk_size:
            upload_cost_data(row_buffer, time_generated, monitor_client)
            row_buffer.clear()
            break

def upload_cost_data(rows, time_generated, monitor_client):

    data = [
        {
            'TimeGenerated': time_generated,
            'DatabaseAccountName': row['AdditionalInfo'].get('GlobalDatabaseAccountName') or parse_resource_id(row['InstanceId'])['name'],
            'DatabaseName': row['AdditionalInfo'].get('DatabaseName'),
            'ContainerName': row['AdditionalInfo'].get('CollectionName'),
            'ContainerRid': row['AdditionalInfo'].get('CollectionRid'),
            'UsageTimestamp': row['UsageDateTime'],
            'MeterCategory': row['MeterCategory'],
            'MeterSubcategory': row['MeterSubcategory'],
            'MeterId': row['MeterId'],
            'MeterName': row['MeterName'],
            'UsageType': row['AdditionalInfo'].get('UsageType'),
            'MeterRegion': row['AdditionalInfo'].get('Region'),
            'UsageQuantity': float(row['UsageQuantity']), 
            'ResourceRate': float(row['ResourceRate']), 
            'PreTaxCost': float(row['PreTaxCost'])
        }
        for row in rows
    ]

    monitor_client.upload(
        rule_id=os.environ['AzureMonitorDataCollectionRuleIdCostData'],
        stream_name=os.environ['AzureMonitorDataCollectionStreamNameCostData'],
        logs=data
    )
