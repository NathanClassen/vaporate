const { spawnSync } = require("child_process");
const { readFileSync, writeFileSync, unlinkSync } = require("fs");
const AWS = require("aws-sdk");
const outputBucketName = process.env.outputBucket;

const s3 = new AWS.S3();

module.exports.transcode = async (event, context) => {
  if (!event.Records) {
    console.log("not an s3 invocation!");
    return;
  }

  for (const record of event.Records) {
    if (!record.s3) {
      console.log("not an s3 invocation!");
      continue;
    }

    /*  if (record.s3.object.key.endsWith(".ffcat")) {
       //skip processing of manifest file. Edit this code later to have it sent to next bucket?
       console.log("not a video file");
       continue;
     } */

    // get the file
    const s3Object = await s3
      .getObject({
        Bucket: record.s3.bucket.name,
        Key: record.s3.object.key
      })
      .promise(); const { spawnSync } = require("child_process");
    const { readFileSync, writeFileSync, unlinkSync } = require("fs");
    const AWS = require("aws-sdk");
    const outputBucketName = process.env.outputBucket;

    const s3 = new AWS.S3();

    module.exports.transcode = async (event, context) => {
      if (!event.Records) {
        console.log("not an s3 invocation!");
        return;
      }

      for (const record of event.Records) {
        if (!record.s3) {
          console.log("not an s3 invocation!");
          continue;
        }

        if (record.s3.object.key.endsWith(".ffcat")) {
          //skip processing of manifest file. Edit this code later to have it sent to next bucket?
          console.log("not a video file");
          continue;
        }

        // get the file
        const s3Object = await s3
          .getObject({
            Bucket: record.s3.bucket.name,
            Key: record.s3.object.key
          })
          .promise();

        // write file to disk
        writeFileSync(`/tmp/${record.s3.object.key}`, s3Object.Body);

        const nameMinusExtension = `${record.s3.object.key}`.match(/[^\.]+/)[0];

        // convert to mp4!
        spawnSync(
          "/opt/ffmpeg/ffmpeg",
          [
            "-i",
            `/tmp/${record.s3.object.key}`,
            `/tmp/${nameMinusExtension}.mp4`
          ],
          { stdio: "inherit" }
        );

        // read mp4 from disk
        const mp4File = readFileSync(`/tmp/${nameMinusExtension}.mp4`);

        // delete the temp files
        unlinkSync(`/tmp/${nameMinusExtension}.mp4`);
        unlinkSync(`/tmp/${record.s3.object.key}`);

        // upload mp4 to s3
        await s3
          .putObject({
            Bucket: outputBucketName,
            Key: `${nameMinusExtension}.mp4`,
            Body: mp4File
          })
          .promise();
      }
    };

    // write file to disk
    writeFileSync(`/tmp/${record.s3.object.key}`, s3Object.Body);

    if (record.s3.object.key.endsWith(".ffcat")) { // if manifest file
      let manifest = readFileSync(`/tmp/${record.s3.object.key}`);
      await s3
        .putObject({
          Bucket: outputBucketName,
          Key: `${nameMinusExtension}.mp4`,
          Body: manifest
        })
        .promise();
      unlinkSync(`/tmp/${record.s3.object.key}`);
      continue;
    }

    const nameMinusExtension = `${record.s3.object.key}`.match(/[^\.]+/)[0];

    // convert to mp4!
    spawnSync(
      "/opt/ffmpeg/ffmpeg",
      [
        "-i",
        `/tmp/${record.s3.object.key}`,
        `/tmp/${nameMinusExtension}.mp4`
      ],
      { stdio: "inherit" }
    );

    // read mp4 from disk
    const mp4File = readFileSync(`/tmp/${nameMinusExtension}.mp4`);

    // delete the temp files
    unlinkSync(`/tmp/${nameMinusExtension}.mp4`);
    unlinkSync(`/tmp/${record.s3.object.key}`);

    // upload mp4 to s3
    await s3
      .putObject({
        Bucket: outputBucketName,
        Key: `${nameMinusExtension}.mp4`,
        Body: mp4File
      })
      .promise();
  }
};