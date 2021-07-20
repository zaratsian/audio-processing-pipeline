# Create Google Cloud Storage Resources
gsutil mb -c standard -l US 'gs://z-audio-dropzone' # Audio file dropzone
gsutil mb -c standard -l US 'gs://z-txt-dropzone'   # Landing zone for speech-to-text results as well as raw SMS/txt messages
gsutil mb -c standard -l US 'gs://z-text-results'   # Temporary landing zone for text results

# Setup BigQuery Dataset and Table(s)
# The BigQuery tables will house the result set (which contains the top X affiliate products for each video)
bq --location=US mk --dataset audio_text_analysis
bq mk --table audio_text_analysis.sentiment bq_schema_sentiment.json
bq mk --table audio_text_analysis.phrases bq_schema_phrases.json

# Deploy Cloud Function: speech-to-text
gcloud functions deploy speech-to-text \
    --region us-central1 \
    --runtime python39 \
    --memory 1024MB \
    --entry-point main \
    --trigger-event google.storage.object.finalize \
    --trigger-resource 'z-audio-dropzone' \
    --source cloud_functions/speech_to_text \
    --max-instances 2 \
    --allow-unauthenticated

# Deploy Cloud Function: nlp
gcloud functions deploy nlp \
    --region us-central1 \
    --runtime python39 \
    --memory 1024MB \
    --entry-point main \
    --trigger-event google.storage.object.finalize \
    --trigger-resource 'z-txt-dropzone' \
    --source cloud_functions/nlp \
    --max-instances 2 \
    --allow-unauthenticated


