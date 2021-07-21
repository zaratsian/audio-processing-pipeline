import re,json
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud import language_v1
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
nltk.download('punkt')


bq_sentiment_dataset = 'audio_text_analysis'
bq_sentiment_table   = 'sentiment'

bq_phrase_dataset    = 'audio_text_analysis'
bq_phrase_table      = 'phrases'


def gcp_storage_upload_string(source_string, bucket_name, blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(source_string)
    except Exception as e:
        print('[ ERROR ] Failed to upload to GCS. {}'.format(e))


def gcp_storage_download_as_string(bucket_name, blob_name):
    '''
        Downloads a blob from the bucket, and outputs as a string.
    '''
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob_content = blob.download_as_string()
        
        return blob_content
    
    except Exception as e:
        print('[ ERROR ] {}'.format(e))


def bq_query_table(query):
    try:
        bigquery_client = bigquery.Client()
        query_job = bigquery_client.query(
            query,
            location='US')
        
        return query_job
    except Exception as e:
        print('[ EXCEPTION ] Unable to query bq table. {}'.format(e))
        return None


def bq_streaming_insert(bq_dataset, bq_table, rows_to_insert=[{}]):
    bq_client = bigquery.Client()
    table_id = f'{bq_dataset}.{bq_table}'
    
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        print("[ INFO ] New rows have been added.")
    else:
        print("[ INFO ] Encountered errors while inserting rows: {}".format(errors))


def nlp_sentiment(text_blob, source=''):
    
    client = language_v1.LanguageServiceClient()
    
    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT
    
    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_blob, "type_": type_, "language": language}
    
    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8
    
    response = client.analyze_sentiment(request = {'document': document, 'encoding_type': encoding_type})
    # Get overall sentiment of the input document
    
    results_json = {}
    results_json['source']   = source
    results_json['doc_sentiment'] = response.document_sentiment.score
    results_json['doc_magnitude'] = response.document_sentiment.magnitude
    
    sentence_sentiment = []
    for sentence in response.sentences:
        sentence_sentiment.append({
            'text': sentence.text.content,
            'sentiment': sentence.sentiment.score,
            'magnitude': sentence.sentiment.magnitude
        })
    
    results_json['sentences'] = sentence_sentiment
    
    #results_json_str = json.dumps(results_json, indent=4)
    
    return results_json


def get_bigrams(text_blob, bigram_frequency_threshold=2, phrase_dictionary=[], source=''):
    try:
        '''
        bigram_frequency_threshold: Only return bigrams that are equal to, or more frequent, than the threshold number.
        '''
        text_blob = text_blob.decode('utf-8')
        
        stop_words = set(stopwords.words('english'))
        
        tokens = nltk.word_tokenize(text_blob)
        tokens = [t.lower() for t in tokens if t not in stop_words and len(t) >= 3]
        
        bigrams = nltk.bigrams(tokens)
        
        freqdist = nltk.FreqDist(bigrams)
        freqdist_sorted = {k: v for k, v in sorted(freqdist.items(), key=lambda item: item[1], reverse=True) if v >= bigram_frequency_threshold}
        
        bigram_results = {}
        bigram_results['source'] = source
        bigram_results['phrases'] = [{'phrase':' '.join(k) ,'frequency':v} for k,v in freqdist_sorted.items()]
        
        return bigram_results
    except Exception as e:
        print('[ EXCEPTION ] At get_bigrams. {}'.format(e))


def main(event,context):
    
    source = event['name']
    
    gcs_uri = 'gs://{}/{}'.format(event['bucket'], event['name'])
    
    print('[ INFO ] Processing {}'.format(gcs_uri))
    text_blob = gcp_storage_download_as_string(event['bucket'], event['name'])
    
    # Sentiment Analysis
    print(f'[ INFO ] Writing sentiment results to BQ table {bq_sentiment_dataset}.{bq_sentiment_table}')
    existing_records_sentiment = [row['source'] for row in bq_query_table(f''' SELECT source FROM `dz-apps.{bq_sentiment_dataset}.{bq_sentiment_table}` ''')]
    if source not in existing_records_sentiment:
        sentiment_results = nlp_sentiment(text_blob, source=source)
        bq_streaming_insert(bq_dataset=bq_sentiment_dataset, bq_table=bq_sentiment_table, rows_to_insert=[sentiment_results])
    
    # Phrase Extraction
    print(f'[ INFO ] Writing phrases to {bq_phrase_dataset}.{bq_phrase_table}')
    existing_records_phrases = [row['source'] for row in bq_query_table(f''' SELECT source FROM `dz-apps.{bq_phrase_dataset}.{bq_phrase_table}` ''')]
    if source not in existing_records_phrases:
        bigram_results = get_bigrams(text_blob, bigram_frequency_threshold=2, phrase_dictionary=[], source=source)
        bq_streaming_insert(bq_dataset=bq_phrase_dataset, bq_table=bq_phrase_table, rows_to_insert=[bigram_results])



