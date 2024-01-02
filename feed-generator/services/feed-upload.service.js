const aws = require('aws-sdk');
const fsPromises = require('fs').promises;
const dotenv = require('dotenv');
const { flog } = require('../lib');
const path = require('path');

dotenv.config();

aws.config.update({
  accessKeyId: process.env.ACCESS_KEY,
  secretAccessKey: process.env.SECRET_KEY,
  region: process.env.S3_REGION,
});

const s3 = new aws.S3();

/**
 * Upload one feed to your Bucket. Bucket is taken from env S3_BUCKET_NAME
 *
 * @param {string} filePath - Path to your feed that u want to upload.
 * @param {string} key - The text to use to name your feed in S3. If key is "random.txt", S3 name will be "random.txt"
 */
const uploadBingFeedToS3 = async ({ filePath, key }) => {
  const bucketName = process.env.S3_BUCKET_NAME;

  const mainUpload = await s3.createMultipartUpload({ Bucket: bucketName, Key: key }).promise();
  const uploadId = mainUpload.UploadId;

  const handle = await fsPromises.open(filePath);

  const ONE_MIBIBYTE = 1024 * 1024;
  const FIFTY_MIBIBYTES = 50 * ONE_MIBIBYTE;

  const stats = await handle.stat();
  flog(`-- stats: ${stats.size / ONE_MIBIBYTE}`);

  const pastReadResults = { partNumber: 0, partData: Buffer.from([]) };
  const partUploadResults = [];
  let totalReadBytes = 0;

  while (true) {
    flog(`-- totalReadBytes ${totalReadBytes / ONE_MIBIBYTE}`);
    const container = Buffer.alloc(FIFTY_MIBIBYTES);
    const readResults = await handle.read({ buffer: container, position: totalReadBytes });

    totalReadBytes += readResults.bytesRead;

    const { partNumber, partData } = pastReadResults;

    if (partNumber === 0) {
      console.log('-- pushing index 0 --');
      pastReadResults.partData = container;
      pastReadResults.partNumber += 1;
      continue;
    }

    if (readResults.bytesRead === 0) {
      console.log('-- no more data. breaking --');
      break;
    }

    if (readResults.bytesRead < FIFTY_MIBIBYTES) {
      console.log(`-- final data of size ${readResults.bytesRead / ONE_MIBIBYTE}. breaking --`);
      pastReadResults.partData = Buffer.concat([partData, container]);
      continue;
    }

    flog(`-- uploading part ${partNumber}`);
    const uploadRes = await _uploadPart({
      bucketName,
      partNumber,
      partData,
      fileName: key,
      uploadId,
    });
    flog('-- done upload');
    partUploadResults.push(uploadRes);

    pastReadResults.partNumber += 1;
    pastReadResults.partData = container;
  }

  const { partData, partNumber } = pastReadResults;

  const startOfEmptyBytes = partData.indexOf(0x00);
  const slicedBuffer = partData.subarray(0, startOfEmptyBytes);
  flog(`-- uploading final part ${partNumber} of size ${slicedBuffer.length / ONE_MIBIBYTE}`);

  const uploadRes = await _uploadPart({
    bucketName,
    partNumber: pastReadResults.partNumber,
    partData: slicedBuffer,
    fileName: key,
    uploadId,
  });
  partUploadResults.push(uploadRes);
  flog(`-- done uploading parts. total: ${partNumber} ----- ${totalReadBytes / ONE_MIBIBYTE}`);

  flog('-- completing upload');
  await s3
    .completeMultipartUpload({
      Bucket: bucketName,
      Key: key,
      UploadId: uploadId,
      MultipartUpload: { Parts: partUploadResults },
    })
    .promise();

  flog('-- completed upload. closing file');
  await handle.close();
  flog('-- DONE');
};

async function _uploadPart({ bucketName, partNumber, partData, fileName, uploadId }) {
  const uploadPartResponse = await s3
    .uploadPart({
      Bucket: bucketName,
      Key: fileName,
      PartNumber: partNumber,
      UploadId: uploadId,
      Body: partData,
    })
    .promise();

  return { PartNumber: partNumber, ETag: uploadPartResponse.ETag };
}

module.exports = {
  uploadBingFeedToS3,
};
