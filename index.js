/**
*
* This function will be triggerred via AWS SNS
* which listens the create object event on a bucket
*
*/
const {Storage} = require('@google-cloud/storage');
const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const storage = new Storage()
const gcloudBucket = storage.bucket(process.env.GCLOUD_BUCKET);

const validateBody = (body) => {
  return (!!body.TopicArn && !!body.Signature && !!body.Message)
}

exports.sync = async (req, res) => {
  const allowedOrigins = []
  const storage = new Storage()
  const destinationBucketName = req.query.bucket
  let body = {}
  let message = {}

  const asyncForEach = async (array, callback) => {
    for (var index = 0; index < array.length; index++) {
      await callback(array[index], index, array)
    }
  }

  try {
    body = JSON.parse(req.body)
  } catch (e) {
    console.error('Error: body cannot be parsed as JSON')
    return res.status(400).send('invalid body')
  }

  if (!validateBody(body)) {
    console.error('Error: Invalid body')
    return res.status(400).send('<!-- -->')
  }

  try {
    message = JSON.parse(body.Message)
  } catch (e) {
    console.error('Error: body.Message cannot be parsed as JSON')
    return res.status(400).send('invalid body.Message value')
  }

  if (!message.Records) {
    console.error('No records to be synced or missing/wrong data')
    return res.status(404).send()
  }

  console.info('====== Objects to be synced', message.Records.map(r => r.s3.object.key))

  await asyncForEach(message.Records, async record => {
    const key = record.s3.object.key
    const destinationObject = gcloudBucket.file(key);
    const params = {
      Bucket: process.env.AWS_BUCKET,
      Key: key
    }
    console.log(`Syncing ${key}`)
    new Promise(function (resolve, reject) {
      s3.getObject(params)
        .createReadStream()
        .pipe(destinationObject.createWriteStream())
        .on('error', (err) => {
          console.error(err)
          resolve()
        })
        .on('finish', () => {
          console.info(`Synced ${key}`)
          resolve()
        })
    })
  });
  console.info('terminate function')
  return res.status(200).send()
}
