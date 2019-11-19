# sync-s3-to-cloudstorage

[![Greenkeeper badge](https://badges.greenkeeper.io/hasantayyar/sync-s3-to-cloudstorage.svg)](https://greenkeeper.io/)

Event handler as Google Cloud Function for S3 create object event.

## Deploy

```
gcloud functions deploy FUNCTION_NAME --runtime nodejs10 --trigger-http --entry-point sync --region europe-west1 --project GCLOUD_PROJECT --env-vars-file .env.yaml
```
