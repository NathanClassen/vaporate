const { readFileSync, writeFileSync, unlinkSync } = require("fs");
const { spawnSync } = require("child_process");
const AWS = require("aws-sdk");
const path = require("path");
const s3 = new AWS.S3();
const toFormat = '.mp4'; // use until sending desired format implemented

const outputBucketName = process.env.outputBucket;

module.exports.transcode = async (event) => {
  if (!event.Records) {
    console.log("not an s3 invocation!");
    return;
  }

  for (const record of event.Records) {
    if (!record.s3) {
      console.log("not an s3 invocation!");
      continue;
    }

    const segmentFileName = record.s3.object.key;

    // get the file
    const segmentFile = await s3
      .getObject({
        Bucket: record.s3.bucket.name,
        Key: segmentFileName
      })
      .promise();

    // write file to disk
    writeFileSync(`/tmp/${segmentFileName}`, segmentFile.Body);

    if (segmentFileName.endsWith(".ffcat")) { // if manifest file
      let manifest = readFileSync(`/tmp/${segmentFileName}`);
      await s3
        .putObject({
          Bucket: outputBucketName,
          Key: `${segmentFileName}`,
          Body: manifest
        })
        .promise();
      unlinkSync(`/tmp/${segmentFileName}`);
      continue;
    }

    const nameMinusExtension = path.parse(`${segmentFileName}`).name;

    // convert to toFormat!
    spawnSync(
      "/opt/ffmpeg/ffmpeg",
      [
        "-i",
        `/tmp/${segmentFileName}`,
        `/tmp/${nameMinusExtension}${toFormat}`
      ],
      { stdio: "inherit" }
    );

    // read transcodedFile from disk
    const transcodedFile = readFileSync(`/tmp/${nameMinusExtension}${toFormat}`);

    // delete the temp files
    unlinkSync(`/tmp/${nameMinusExtension}${toFormat}`);
    unlinkSync(`/tmp/${segmentFileName}`);

    // upload transcoded to s3
    await s3
      .putObject({
        Bucket: outputBucketName,
        Key: `${nameMinusExtension}${toFormat}`,
        Body: transcodedFile
      })
      .promise();
  }
};