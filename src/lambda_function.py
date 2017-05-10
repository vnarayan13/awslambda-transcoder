"""While a file shows up in the unconverted bucket, starts Elastic Transcoder"""
import os
import json
import boto3
import urllib

# S3 Variables
bucket_name = '' # change this
unconverted_prefix = 'unconverted'
converted_prefix = 'converted'
thumbnail_prefix = 'thumbnails'

# Elastic Transcoder Variables
pipeline_id = '' # change this
preset_id= '' # change this

# Script Variables
delete_on_completion=True
input_media = ['.mp4'] # change this
output_file_extension = '.mp4' # change this


def delete_source(infile_key):
    s3_client = boto3.client('s3')
    for source_filetype in input_media:
        source_key = unconverted_prefix + '/' + ''.join([ os.path.splitext(os.path.basename(infile_key))[0], source_filetype ]) 
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
    return {'status': 'ok', 'message': 'Deleted Source {0}'.format(source_key)}
    

def start_et(infile_key, thumbnail_pattern):
    outfile_key = converted_prefix + '/' + ''.join([ os.path.splitext(os.path.basename(infile_key))[0], output_file_extension ])
    print("Starting Elastic Transcoder Job on {0} to {1}".format(infile_key, outfile_key))
    et_client = boto3.client('elastictranscoder')
    et_client.create_job(PipelineId=pipeline_id,
                         Input={'Key': infile_key},
                         Outputs=[{'Key': outfile_key,
                                   'ThumbnailPattern': thumbnail_pattern,
                                   'PresetId': preset_id}])
    return {'status': 'ok', 'message': 'Converted {0} to {1}'.format(infile_key, outfile_key)}


def handler(event, context):
    try:
        if (event!=None and event.has_key('Records') and
            len(event.get('Records'))==1 and
            event.get('Records')[0].has_key('s3') and
            event.get('Records')[0].get('s3').has_key('object') and
            event.get('Records')[0].get('s3').get('object').has_key('key')):

            s3_object = event.get('Records')[0].get('s3').get('object')
            infile_key = s3_object.get('key')

            if (infile_key.startswith(unconverted_prefix)) and (any([format in os.path.splitext(infile_key)[1] for format in input_media])):
                infile_key = unconverted_prefix + '/' + urllib.unquote(os.path.basename(infile_key)).decode('utf8').replace('+', ' ')
                thumbnail_pattern = thumbnail_prefix + '/' + os.path.splitext(os.path.basename(infile_key))[0] + '-{count}'
                response = start_et(infile_key, thumbnail_pattern)
                return response
                
            elif (infile_key.startswith(converted_prefix)) and (infile_key.endswith(output_file_extension)) and (delete_on_completion):
                infile_key = converted_prefix + '/' + urllib.unquote(os.path.basename(infile_key)).decode('utf8').replace('+', ' ')
                response = delete_source(infile_key)
                return response
                
            else :
                return {'status': 'ignored', 'message': 'Wrong path'}

        else :
            return {'status': 'ignored', 'message': 'Invalid input'}

    except Exception as exception:
        return {'status': 'error', 'message': exception.message}