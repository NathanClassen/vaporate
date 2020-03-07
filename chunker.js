const { spawnSync } = require("child_process");
const { readFileSync, writeFileSync, unlinkSync } = require("fs");
const AWS = require("aws-sdk");
const outputBucketName = process.env.outputBucket;

const s3 = new AWS.S3();

module.exports.chunk = async (event, context) => {
    if (!event.Records) {
        console.log("not an s3 invocation!");
        return;
    }

    for (const record of event.Records) {
        if (!record.s3) {
            console.log("not an s3 invocation!");
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
        writeFileSync("/tmp/chunks.ffcat", 'ffconcat version 1.0');


        // convert to mp4!
        spawnSync(
            "/opt/ffmpeg/ffmpeg",
            [
                "-i", `/tmp/${record.s3.object.key}`,
                "-map", "0",
                "-codec:v", "libx264",
                "-codec:a", "aac",
                "-f", "ssegment",
                "-segment_list", "/tmp/chunks.ffcat", "/tmp/chunk%03d.mp4"

                /*With current implementation, we are just testing to see if we 
                can properly segment and create the segment listing. 
                If it works, we will create the .ffcat file and the chunks in the 
                tmp file on Lambda and then send the listing file to a different s3
                bucket. Then will try to send the chunks.

                Alternatively, we could send only the manifest along; the listings
                would need a more verbose path to tell the  */

                /* "-i",
                `/tmp/${record.s3.object.key}`,
                `/tmp/${record.s3.object.key}.mp4` */
            ],
            { stdio: "inherit" }
        );

        // read segment manifest from disk
        const segmentListFile = readFileSync(`/tmp/chunks.ffcat`);

        // delete the temp files
        unlinkSync(`/tmp/chunks.ffcat`);
        unlinkSync(`/tmp/${record.s3.object.key}`);

        // upload segment manifest to s3

        /*  Next step may be, send chunks dir to S3 and have transcoder lambda do 
         a recursive transcoding of files in the directory */

        await s3
            .putObject({
                Bucket: outputBucketName,
                Key: "segmentManifest.ffcat",
                Body: segmentListFile
            })
            .promise();
    }
};
