const { spawnSync } = require("child_process");
const { readFileSync, writeFileSync, unlinkSync, readdirSync, statSync, mkdirSync } = require("fs");
const path = require("path");
const AWS = require("aws-sdk");
const outputBucketName = process.env.outputBucket;

const s3 = new AWS.S3();

const sendVideoSegments = function (s3Path, bucketName) {
    function walkSync(currentDirPath, callback) {
        readdirSync(currentDirPath).forEach(function (name) {
            var filePath = path.join(currentDirPath, name);
            var stat = statSync(filePath);
            if (stat.isFile()) {
                callback(filePath, stat);
            } else if (stat.isDirectory()) {  // should be able to remove this as we will only have files, no directories in /tmp/videoSegments
                walkSync(filePath, callback);
            }
        });
    }

    walkSync(s3Path, function (filePath, stat) {
        let bucketPath = filePath.substring(s3Path.length + 1);
        let params = { Bucket: bucketName, Key: bucketPath, Body: readFileSync(filePath) };
        s3.putObject(params, function (err, data) {
            if (err) {
                console.log(err)
            } else {
                console.log('Successfully uploaded ' + bucketPath + ' to ' + bucketName);
            }
        });

    });
};

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
        mkdirSync("/tmp/videoSegments");

        // convert to mp4!
        spawnSync(
            "/opt/ffmpeg/ffmpeg",
            [
                "-i", `/tmp/${record.s3.object.key}`,
                "-map", "0",
                "-codec:v", "libx264",
                "-codec:a", "aac",
                "-f", "ssegment",
                "-segment_list", "/tmp/chunks.ffcat", "/tmp/videoSegments/chunk%03d.mp4"
            ],
            { stdio: "inherit" }
        );

        // send all segments to s3 for transcoding
        sendVideoSegments("/tmp/videoSegments", outputBucketName);
        // read segment manifest from disk
        const segmentListFile = readFileSync(`/tmp/chunks.ffcat`);

        // delete the temp files
        unlinkSync(`/tmp/chunks.ffcat`);
        unlinkSync(`/tmp/${record.s3.object.key}`);

        // upload segment manifest to s3
        await s3
            .putObject({
                Bucket: outputBucketName,
                Key: "segmentManifest.ffcat",
                Body: segmentListFile
            })
            .promise();
    }
};
