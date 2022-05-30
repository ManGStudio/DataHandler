from contextlib import contextmanager
from csv import DictReader, DictWriter
from collections import defaultdict
import datetime
from datetime import date, timedelta
import io
import json
import csv

import boto3

s3 = boto3.client("s3")
s3_resource = boto3.resource('s3')

BUCKET = "bravad-data"
DELIMITER = ","
TABLE_OBJECT_COLUMNS = ['name', 'salesTotal', 'customer_id']

TODAY = datetime.datetime.now()
RECENTLY_CALLED = 14


def content_as_dict_reader(content, header = False):
    if header:
        yield DictReader(content.splitlines(), delimiter=DELIMITER, fieldnames=header)
    else:
        yield DictReader(content.splitlines(), delimiter=DELIMITER)
      
def parseFilters(s3_event):
    sql_filter = ""
    if 'filters' in s3_event:
        jsonFilter = s3_event['filters']
        sql_filter += " WHERE 1"
        for filter_key in jsonFilter:
            sql_filter += " AND s." + filter_key + ' ' + jsonFilter[filter_key]['operator'] + " '" + jsonFilter[filter_key]['values'] + "'"
    return sql_filter

@contextmanager
def tables_and_lines_for_deletion():
   object_ = s3.get_object(
      Bucket=bucket, Key=key
   )
   content = object_["Body"].read().decode('utf-8')
   return content_as_dict_reader(content)

# def object_data(bucket, key, sql_filter):
#     records = None
#     response_ = s3.select_object_content(
#         Bucket = bucket,
#         Key = key,
#         ExpressionType = 'SQL',
#         Expression = "Select * from s3object s" + sql_filterf,
#         InputSerialization = {'CSV': {"FileHeaderInfo": "USE", 'QuoteEscapeCharacter': ''}},
#         OutputSerialization = {'JSON': {'RecordDelimiter': ','}}
#     )
#     for event in response_['Payload']:
#         if 'Records' in event:
#             records = event['Records']['Payload'].decode('utf-8')[:-1]
#     dicrecords = content_as_dict_reader(records)
#     for val in dicrecords:
#         print(val)
#     return records

@contextmanager
def table_record(table):
   object_ = s3.get_object(
      Bucket=bucket, Key=table
   )
   content = object_["Body"].read().decode('utf-8')
   return content_as_dict_reader(content)

def object_table(table, record):
   with io.StringIO() as file_:
      writer = DictWriter(
         file_,
         fieldnames=TABLE_OBJECT_COLUMNS,
         delimiter=DELIMITER
      )
      writer.writeheader()
      writer.writerows(list(record))

      s3.put_object(
         Bucket=bucket,
         Key=table,
         Body=file_.getvalue()
      )

#returns header of file (first column of csv)
def select_file_header(bucket, key, sql_filter):
    header_records = []
    records = ''
    header_response = s3.select_object_content(
        Bucket= bucket,
        Key= key,
        ExpressionType= 'SQL',
        Expression= "select * from s3object s limit 1",
        InputSerialization= {'CSV': {"FileHeaderInfo": "NONE"}},
        OutputSerialization= {'CSV': {'QuoteFields': 'ASNEEDED'}},
    )
    for header_event in header_response['Payload']:
        if 'Records' in header_event:
            header_records = header_event['Records']['Payload'].decode('utf-8').rstrip()
    return header_records
    
def get_bucket_data(bucket, key, sql_filter):
    records = []
    response = s3.select_object_content(
        Bucket = bucket,
        Key = key,
        ExpressionType = 'SQL',
        Expression = "Select * from s3object s " + sql_filter + " LIMIT 10",
        InputSerialization = {'CSV': {"FileHeaderInfo": "USE"}},
        OutputSerialization = {'CSV': {'QuoteFields': 'ALWAYS'}}
    )
    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')[:-1]
    return records
    
def get_bucket_reader(bucket, key, sql_filter = ''):
    
    #get header and split
    splittedHeader = select_file_header(bucket, key, sql_filter).split(',')
    
    #get records
    records = get_bucket_data(bucket, key, sql_filter)
    
    #build dictReader
    return content_as_dict_reader(records, splittedHeader)
    
def get_contacted_clients_bucket(bucket, customer_id):
    contacted_file_key = 'General/user_generated/contacted_clients.csv'
    response_ = s3.select_object_content(
        Bucket = bucket,
        Key = contacted_file_key,
        ExpressionType = 'SQL',
        Expression = "Select * from s3object s WHERE s.customer_id = '" + customer_id + "'",
        InputSerialization = {'CSV': {"FileHeaderInfo": "USE"}},
        OutputSerialization = {'CSV': {'QuoteFields': 'ASNEEDED'}}
    )
    records = ''
    for event in response_['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')[:-1]
    return content_as_dict_reader(records, ['customer_id', 'lastContactDate', 'note', 'followUpDate'])

def update_contacted_date(bucket, key, client):
    s3_resource.meta.client.download_file(bucket, key, '/tmp/temp_bucket_2.csv')
    with open('/tmp/temp_bucket_2.csv', 'r+') as csv_file:
        fieldnames = ['customer_id', 'lastContactDate', 'note', 'followUpDate']
        reader = csv.DictReader(open('/tmp/temp_bucket_2.csv'), fieldnames=fieldnames)
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        next(reader, None)
        for line in reader:
            writer.writerow(line)
        writer.writerow({'customer_id': client['id'], 'lastContactDate': client['date'], 'note': client['note'], 'followUpDate': client['followUpDate']})
    s3_resource.meta.client.upload_file('/tmp/temp_bucket_2.csv', bucket, key)

def load_client_notes(bucket, client):
    notes = []
    contacted_client_dict = get_contacted_clients_bucket(bucket, client['id'])
    for contacted_rows in contacted_client_dict:
        for contacted_values in contacted_rows:
            notes.append(contacted_values)
            # notes['date'] = contacted_values['lastContactDate']
            # notes['note'] = contacted_values['note']
    return notes

def parse_informations(bucket, key, dict_reader_content):
    filtered_records = []
    called_recently = TODAY - timedelta(days=RECENTLY_CALLED)
    last_contact_date = datetime.datetime(2000, 1, 1)
    for rows in dict_reader_content:
        for values in rows:
            values['notes'] = []
            contacted_client_dict = get_contacted_clients_bucket(bucket, values['customer_id'])
            for contacted_rows in contacted_client_dict:
                for contacted_values in contacted_rows:
                    values['notes'].append(contacted_values['lastContactDate'] + ": " + contacted_values['note'])
                    if datetime.datetime(*[int(item) for item in contacted_values['lastContactDate'].split('-')]) > last_contact_date:
                        last_contact_date = datetime.datetime(*[int(item) for item in contacted_values['lastContactDate'].split('-')])
                        
            values['lastContactDate'] = str(last_contact_date.date())
            #if more than 14 days do not show warning
            values['recentlyContacted'] = 1 if last_contact_date < called_recently else 0
            values['potentialColor'] = 'green'
            values['potentialPercentage'] = 80
            filtered_records.append(values)
    return filtered_records
    
def parse_transactions(bucket, key, dict_reader_content):
    filtered_records = []
    for rows in dict_reader_content:
        for values in rows:
            values['companyName'] = get_division(values['idCompany'])
            filtered_records.append(values)
    return filtered_records
    
def upload_csv(bucket, key, csv):
    print('hello')

def get_division(idCompany):
    companyName = 'Fabrique'
    if(idCompany == '2'):
        companyName = 'Techno'
    elif(idCompany == '3'):
        companyName = 'TI'
    return companyName

#handles all events
#SelectTransactions returns all transactions for a client, need customer_id value
#SelectInformations returns 10 clients that correspond to the filters and checks if the client has been called
#Put inserts a new row in contacted_clients.csv (customer_id, date.now(), note)
def lambda_handler(event, context):
    if not event:
        print("Function must be triggered via a published event")
        return
   
    event_record, *_ = event["Records"]
    try:
        #instance variables
        s3_event = event_record['s3']
        bucket = s3_event['bucket']['name']
        key = s3_event['object']['key']
        event_name = str(event_record['eventName'])
        sql_filter = parseFilters(s3_event)
        
        #get dictReader
        dict_reader_content = get_bucket_reader(bucket, key, sql_filter)
        
        if "SelectInformations" in event_name:
            print('select client information started')
            filtered_records = parse_informations(bucket, key, dict_reader_content)
            return filtered_records
            
        if "SelectTransactions" in event_name:
            print('select client transactions started')
            filtered_records = parse_transactions(bucket, key, dict_reader_content)
            return filtered_records
            
        if "Put" in event_name:
            print('Update client contacted information')
            update_contacted_date(bucket, key, s3_event['client'])
            
        if "SelectNotes" in event_name:
            print('select client notes')
            return load_client_notes(bucket, s3_event['client'])
            
    except KeyError:
        # Handle when event_record isn't an S3 one.
        print('key error')
    return
    print("S3 event is a put one for :WATCH_KEY!")
   
    table_group = defaultdict(list)
   
    print("Reading :WATCH_KEY content")
    with tables_and_lines_for_deletion() as tables:
        for dct in tables:
            table_k = dct['customer_id']
            table_v = dct['name']
            table_group[table_k].append(table_v)
    print(table_group)
    print("Updating objects found in :WATCH_KEY content")
    return
    for t, ids in table_group.items():
        record_update = None
        with table_record(t) as record:
            record_update = (
                dct
                for dct in record
                    if dct["customer_id"] not in ids
                )
            object_table(t, record_update)
    print("Update completed!")
    return