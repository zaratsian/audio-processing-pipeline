# Create Google Cloud Storage Resources
gsutil mb -c standard -l US 'gs://z-audio-dropzone' # Audio file dropzone
gsutil mb -c standard -l US 'gs://z-txt-dropzone'   # Test / SMS file dropzone
gsutil mb -c standard -l US 'gs://z-audio-text'     # Location of speech-to-text results
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

# Deploy Cloud Function: text-processing
gcloud functions deploy text-processing \
    --region us-central1 \
    --runtime python39 \
    --memory 1024MB \
    --entry-point main \
    --trigger-event google.storage.object.finalize \
    --trigger-resource 'z-audio-text' \
    --source cloud_functions/rv-video-catalog-matching \
    --max-instances 2 \
    --allow-unauthenticated


