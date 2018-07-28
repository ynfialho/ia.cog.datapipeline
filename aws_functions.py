# -*- coding: latin-1 -*-
import os
import boto3 as bt
import itertools
import datetime as DT
from random import randint
from operator import itemgetter
from time import sleep


def aws_connect_key(access_key, secret_access_key,reg = 'us-east-1'):
    # Variable profile receive a profile name configured on aws cli
    try:
        session = bt.Session(region_name= reg ,aws_access_key_id = access_key, aws_secret_access_key = secret_access_key)
        return session
    
    except Exception as e :
        print(e)
        raise(e)

def aws_connect_profile(profile):
	''' 
		Variable profile receive a profile name configured on aws cli 

	'''
	try:
		session = bt.Session(profile_name=profile)
		return session
    
	except Exception as e :
        
		print(e)
		raise(e)

def s3_download_single_file(resource, bucket, prefix, key, local):
        
    '''
    Download a single file from s3
    
    Params:
        
        prefix = Bucket sub folder with trailing slash
        key = File name
        local = Folder path to save the file with trailing slash (full path)
    
    return:
        
        return true (file downloaded) or false (file not downloaded)
        
    error: 
        
        Raise error on missing file on s3
        Raise error on missing local path              
    
    '''
    
    try:
        
        #if local[-1:] != '/': local = local + '/'    
        if prefix[-1:] != '/': prefix = prefix + '/'
        if prefix[:1] == '/': prefix = prefix[1:]

        #print('local+key: ' + local+key)
        #print('prefix+key: ' + prefix+key)
        
        resource.Bucket(bucket).download_file(prefix+key, local+key)
    
        #print(2) 
        
        if os.path.isfile(local+key): 
            return True
        else:
            return False
        
    except Exception as e:
        
        False

def s3_put_single_file(resource, bucket, prefix, key, body, contentType):
	resource.Object(bucket, prefix+key).put(Body=body, ContentType=contentType)

	

def s3_list_objects_from_bucket(resource, client, bucket, prefix ):
            
	try:               
		varObjects = []
        
		response = client.list_objects_v2(
                   Bucket = bucket,
                   Prefix = prefix
                )
        
        #print(response)

		for file in response['Contents']:
        # Get the file name
        
			name = file['Key'].rsplit('/', 1)
			varObjects.append(name[1])
            #print(name)
        
		return varObjects
    
	except Exception as e :
        
		print(e)
		raise(e)
        


def sqs_delete_message(url_sqs, message, sqs_client):
	r = sqs_client.delete_message(QueueUrl= url_sqs, ReceiptHandle= message['ReceiptHandle'])
	return(r)

def sqs_delete_batch_message(url_sqs, message, sqs_client):
	r = list(map(lambda x: sqs_client.delete_message(QueueUrl= url_sqs, ReceiptHandle= x), list(map(itemgetter('ReceiptHandle'),message))))
	return(r)


def sqs_send_message(queue, body, AttName = 'Retry', AttValue='0', AttType='Number'):
	response = queue.send_messages(Entries=[
		{
			'Id': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f')))),
        	'MessageBody': body,
			'MessageAttributes': {
            	AttName: {
					'StringValue': AttValue,
					'DataType': AttType
					}
				}
		}
	])

def sqs_create_batch_message(body, AttValue='0'
, metodo = 'NULL'
, param1 = 'NULL'
, param2 = 'NULL'
, param3 = 'NULL'
, param4 = 'NULL'
, param5 = 'NULL'
, param6 = 'NULL'
, param7 = 'NULL'
, param8 = 'NULL'):
	response ={
		'Id': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f')))),
		'MessageBody': body,
		'MessageAttributes': {
			'Retry':{
				'StringValue': AttValue,
				'DataType': 'Number'
				},
			'metodo':{
				'StringValue': metodo,
				'DataType': 'String'
				},
			'param1':{
				'StringValue': param1,
				'DataType': 'String'
				},
			'param2':{
				'StringValue': param2,
				'DataType': 'String'
				},
			'param3':{
				'StringValue': param3,
				'DataType': 'String'
				},	
			'param4':{
				'StringValue': param4,
				'DataType': 'String'
				},
			'param5':{
				'StringValue': param5,
				'DataType': 'String'
				},
			'param6':{
				'StringValue': param6,
				'DataType': 'String'
				},
			'param7':{
				'StringValue': param7,
				'DataType': 'String'
				},
			'param8':{
				'StringValue': param8,
				'DataType': 'String'
				}									
			}
		}
	return response

def sqs_send_batch_message(queue, batch_message, interval = 10):
    try:
        for seq in range(0,len(batch_message),interval):
            queue.send_messages(Entries = batch_message[seq:seq+interval])
    except Exception as e:
        print(e)

def sqs_read_message(url_sqs, sqs_client):
	messages = sqs_client.receive_message(
    QueueUrl= url_sqs,
    AttributeNames=['All'],
	MessageAttributeNames=['All'],
    MaxNumberOfMessages=1,
    VisibilityTimeout=300,
    WaitTimeSeconds=3
	)
	try:
		msg = messages.get('Messages')[0]
	except:
		msg = -1
	return(msg)

def sqs_read_batch_message(url_sqs, sqs_client):
	messages = sqs_client.receive_message(
    QueueUrl= url_sqs,
    AttributeNames=['All'],
	MessageAttributeNames=['All'],
    MaxNumberOfMessages=10,
    VisibilityTimeout=300,
    WaitTimeSeconds=3
	)
	try:
		if messages.get('Messages') is None:
			raise('error')

		msg = messages.get('Messages')
	except:
		msg = -1
	return(msg)


def s3_check_file_exists(client, bucket, prefix, key):
    try:
        client.head_object(Bucket=bucket, Key=prefix+key)
        return True
    except:     
        return False

def sqs_fifo_create_batch_message(body, AttValue='0'
, metodo = 'NULL'
, param1 = 'NULL'
, param2 = 'NULL'
, param3 = 'NULL'
, param4 = 'NULL'
, param5 = 'NULL'
, param6 = 'NULL'
, param7 = 'NULL'
, param8 = 'NULL'):
	response ={
		'Id': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f')))),
		'MessageBody': body,
		'MessageAttributes': {
			'Retry':{
				'StringValue': AttValue,
				'DataType': 'String'
				},
			'metodo':{
				'StringValue': metodo,
				'DataType': 'String'
				},
			'param1':{
				'StringValue': param1,
				'DataType': 'String'
				},
			'param2':{
				'StringValue': param2,
				'DataType': 'String'
				},
			'param3':{
				'StringValue': param3,
				'DataType': 'String'
				},	
			'param4':{
				'StringValue': param4,
				'DataType': 'String'
				},
			'param5':{
				'StringValue': param5,
				'DataType': 'String'
				},
			'param6':{
				'StringValue': param6,
				'DataType': 'String'
				},
			'param7':{
				'StringValue': param7,
				'DataType': 'String'
				},
			'param8':{
				'StringValue': param8,
				'DataType': 'String'
				}								
			},
        'MessageDeduplicationId': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f'))+1)),
        'MessageGroupId': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f'))+2))
		}
	return response

def sqs_fifo_send_batch_message(sqs_client, url_sqs, batch_message, interval = 10):
    try:
        for seq in range(0,len(batch_message),interval):
            sqs_client.send_message_batch(QueueUrl = url_sqs, Entries = batch_message[seq:seq+interval])
    except Exception as e:
        print(e)	


def sns_send_email(resource, topic, subject, message):
 
    topic = resource.Topic(topic)
 
    topic.set_attributes(AttributeName='DisplayName', 
                         AttributeValue='Monitoring Linx') 
     
    topic.publish(Message = message,
                  Subject = subject)		


def sqs_fifo_create_batch_message_json(body, bucket, prefix, size):
	response ={
		'Id': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f')))),
		'MessageBody': body,
		'MessageAttributes': {
			'size':{
				'StringValue': size,
				'DataType': 'String'
				},							
			'bucket':{
				'StringValue': bucket,
				'DataType': 'String'
				},							
			'prefix':{
				'StringValue': prefix,
				'DataType': 'String'
				}							
			},
        'MessageDeduplicationId': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f'))+1)),
        'MessageGroupId': str(randint(0,int(DT.datetime.now().strftime('%Y%m%d%H%M%S%f'))+2))
		}
	return response



def s3_list_metadata_from_bucket(client, bucket, prefix):
            
	try:               
		varObjects = []
		flagTrucated = True

		kwargs = {'Bucket': bucket, 'Prefix': prefix}

		while flagTrucated:
			response = client.list_objects_v2(**kwargs)
			
			for file in response['Contents']:
				name = file['Key'].rsplit('/', 1)[1]
				prefix = file['Key'].rsplit('/', 1)[0]
				varObjects.append([name, prefix, file['Size']])
			
			try:
				kwargs['ContinuationToken'] = response['NextContinuationToken']
			except KeyError:
				flagTrucated = False
		
		return varObjects
    
	except Exception as e :
		None

def cw_find_log(cw_client, logGroup, startT, endT, filterP):
    try:
        cw_log_info = []
        flagTrucated = True
        kwargs = {'logGroupName':logGroup, 'startTime':startT, 'endTime':endT, 'filterPattern':filterP}

        while flagTrucated:
            response = cw_client.filter_log_events(**kwargs)
            cw_log_info.append(response)
            
            try:
                # Paginacao do retorno da lista
                kwargs['nextToken'] = response['nextToken']
            except KeyError:
                flagTrucated = False

        return cw_log_info

    except Exception as e :
        None

def sf_executions(sf_client, arn):
    try:
        sf_executions_list = []
        flagTrucated = True
        kwargs = {'stateMachineArn': arn}

        while flagTrucated:
            response = sf_client.list_executions(**kwargs)
            sf_executions_list.append(response['executions'])
            
            try:
                # Paginacao do retorno da lista
                kwargs['nextToken'] = response['nextToken']
            except KeyError:
                flagTrucated = False

        return sf_executions_list

    except Exception as e :
        None


def s3_delete_files(s3_client, bucket, prefix_files):
	arquivos_prefix = s3_list_metadata_from_bucket(client = s3_client, bucket = bucket, prefix = prefix_files)
	objects_list_delete = []
	cont = 0
	flag = True

	while flag:

		if arquivos_prefix is None:
			arquivos_prefix = ''

		if len(arquivos_prefix) != 0:

			if cont == 999:
				
				reponse = s3_client.delete_objects(
					Bucket=bucket,
					Delete={
						'Objects': objects_list_delete
						}
					)              
				objects_list_delete = []
				cont = 0  
			else:

				objects_list_delete.append({'Key': arquivos_prefix[0][1] + '/' + arquivos_prefix[0][0]})
				arquivos_prefix.pop(0)             
				cont += 1
									
		elif len(objects_list_delete) != 0:

			reponse = s3_client.delete_objects(
				Bucket=bucket,
				Delete={
					'Objects': objects_list_delete
				}
			)
			objects_list_delete = []
			cont = 0 

		else: 
			flag = False


def s3_check_file_exists2(client, bucket, prefix):
    try:
        client.head_object(Bucket=bucket, Key=prefix)
        return True
    except:     
        return False

def s3_list_keys(client, bucket, prefix_list):     
	try:               
		list_keys = []
		
		for prefix in prefix_list:
			flag = True
			kwargs = {'Bucket': bucket, 'Prefix': prefix}

			while flag:
				try:
					response = client.list_objects_v2(**kwargs)
					[ list_keys.append(file['Key']) for file in response['Contents'] ]
				except KeyError:
					flag = False
				try:
					kwargs['ContinuationToken'] = response['NextContinuationToken']
				except KeyError:
					flag = False
		
		return list_keys
    
	except Exception:
		None

def athena_check_query_status(athena_client, query_id, counter=30, waiter=0.5, status='SUCCEEDED'):
	cont = 0
	try:
		while cont <= counter:
			query_status = athena_client.get_query_execution(QueryExecutionId=query_id)
			if query_status['QueryExecution']['Status']['State'] == status:
				cont = counter
			sleep(waiter)
			cont += 1
		return f'Athena query is {status}.'
	except Exception:
		print('Error in query id')