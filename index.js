/**
*
* This function will be triggerred via AWS SNS
* which should be configured to listen the PUT operations on a S3 bucket
*
*/
const {Storage} = require('@google-cloud/storage');
const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const storage = new Storage()
const gcloudBucket = storage.bucket(process.env.GCLOUD_BUCKET);

/**
* @TODO: add more validations
*/
const validateBody = (body) => {
  return (!!body.TopicArn && !!body.Signature && !!body.Message)
}

/**
* @TODO: Add retry for every api operation
*/
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
    console.error('Error: Invalid payload')
    return res.status(400).send()
  }

  if (!validateBody(body)) {
    console.error('Error: Payload can not be validated')
    return res.status(400).send()
  }

  try {
    message = JSON.parse(body.Message)
  } catch (e) {
    console.error(`Error: body.Message cannot be parsed as JSON ${message}`)
    return res.status(400).send('invalid body.Message value')
  }

  if (!message.Records) {
    console.error(`Error: No records to be synced or missing/wrong data: ${JSON.stringify(message)}`)
    return res.status(404).send()
  }

  console.log(`Objects to be synced: ${message.Records.map(r => r.s3.object.key)}`)

  await asyncForEach(message.Records, async record => {
    const key = record.s3.object.key
    const destinationObject = gcloudBucket.file(key);
    const params = {
      Bucket: process.env.AWS_BUCKET,
      Key: key
    }
    console.log(`Syncing ${key}`)
    const pipe = new Promise(function (resolve, reject) {
      s3.getObject(params)
        .createReadStream()
        .pipe(destinationObject.createWriteStream())
        .on('error', (err) => {
          console.error(err)
          resolve()
        })
        .on('finish', () => {
          console.log(`Synced ${key}`)
          resolve()
        })
    })
    await pipe
  });
  console.log('terminate function')
  return res.status(200).send()
}
