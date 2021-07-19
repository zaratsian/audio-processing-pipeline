import datetime
import re,json
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud import speech


gcs_results_bucket = 'z-txt-dropzone'


def gcp_storage_upload_string(source_string, bucket_name, blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(source_string)
    except Exception as e:
        print('[ ERROR ] Failed to upload to GCS. {}'.format(e))


def gcp_speech_to_text(gcs_uri):
    start_time = datetime.datetime.now()
    
    speech_client = speech.SpeechClient()
    
    audio  = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        #encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
        #sample_rate_hertz=16000,
        audio_channel_count=2,
        enable_separate_recognition_per_channel=True,
        language_code="en-US",
        enable_automatic_punctuation=True,
    )
    
    operation = speech_client.long_running_recognize(config=config, audio=audio)
    
    print("[ INFO ] Waiting for operation to complete...")
    response = operation.result(timeout=300)
    
    text_blob_list = []
    for result in response.results:
        if result.alternatives[0].transcript not in text_blob_list:
            text_blob_list.append(result.alternatives[0].transcript)
        
        #print(u"Transcript: {}".format(result.alternatives[0].transcript))
        #print("Confidence: {}".format(result.alternatives[0].confidence))
    
    text_blob = ' '.join(text_blob_list)
    runtime = (datetime.datetime.now() - start_time).seconds
    print('[ INFO ] Speech-to-Text Runtime: {} seconds'.format(runtime))
    print('[ INFO ] Text Blob: {}'.format(text_blob))
    
    return text_blob


def main(event,context):
    
    gcs_uri = 'gs://{}/{}'.format(event['bucket'], event['name'])
    
    print('[ INFO ] Processing {}'.format(gcs_uri))
    text_blob = gcp_speech_to_text(gcs_uri)
    
    print('[ INFO ] Writing text blob to gs://{}'.format(gcs_results_bucket))
    blob_name = re.sub('\.[a-zA-Z0-9]{3,4}$', '.txt', event['name'])
    gcp_storage_upload_string(source_string=text_blob, bucket_name=gcs_results_bucket, blob_name=blob_name)



