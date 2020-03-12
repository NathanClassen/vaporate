const { readFileSync, writeFileSync, unlinkSync, readdirSync, mkdirSync } = require("fs");
const { spawnSync } = require("child_process");
const AWS = require("aws-sdk");
const path = require("path");
const s3 = new AWS.S3();

AWS.config.update({ endpoint: "https://dynamodb.us-east-1.amazonaws.com" });
const docClient = new AWS.DynamoDB.DocumentClient();
const segmentsTable = "Segments";
const jobsTable = "Jobs";

const outputBucketName = process.env.outputBucket;

const sendVideoSegments = (videoSegmentDir, segmentsBucket) => {
    readdirSync(videoSegmentDir).forEach(segment => {
        const filePath = path.join(videoSegmentDir, segment);
        const videoData = readFileSync(filePath);
        const bucketKey = segment;
        const params = { Bucket: segmentsBucket, Key: bucketKey, Body: videoData };

        s3.putObject(params, (err) => {
            if (err) {
                console.log(err);
            }
        });
    });
}

const saveJobData = (jobId, fileBasename, segments, fileExt) => {
    const id = jobId;
    const totalTasks = segments.length;
    const filename = fileBasename;
    const inputType = fileExt;

    const params = {
        TableName: jobsTable,
        Item: {
            "id": id,
            "totalTasks": totalTasks,
            "finishedTasks": 0,
            "filename": filename,
            "status": "pending",
            "inputType": inputType,
            "createdAt": new Date,
            "completedAt": null
        }
    };

    dbWriter(params, 'job');
}

const saveSegmentData = (jobId, segments) => {
    segments.forEach(segment => {
        let id = path.parse(segment).name;
        let filename = `${jobId}-${id}`;

        let params = {
            TableName: segmentsTable,
            Item: {
                "jobId": jobId,
                "id": id,
                "filename": filename,
                "status": "pending",
                "createdAt": new Date,
                "completedAt": null
            }
        };

        dbWriter(params, 'segment');
    });
}

const dbWriter = (params, item) => {
    console.log(`Adding a new ${item}...`);
    docClient.put(params, (err, data) => {
        if (err) {
            console.log(`Unable to add ${item}. Error JSON:`, JSON.stringify(err, null, 2));
        } else {
            console.log(`Added ${item}:`, JSON.stringify(data, null, 2));
        }
    });
}

module.exports.chunk = async (event) => {
    if (!event.Records) {
        console.log("not an s3 invocation!");
        return;
    }

    for (const record of event.Records) {
        if (!record.s3) {
            console.log("not an s3 invocation!");
            continue;
        }
        const uploadedVideo = record.s3.object.key;

        // get the file
        const videoToProcess = await s3
            .getObject({
                Bucket: record.s3.bucket.name,
                Key: uploadedVideo
            })
            .promise();

        const filePathObj = path.parse(`${uploadedVideo}`);
        const fileBasename = filePathObj.name;
        const fileExtension = filePathObj.ext;

        const jobId = `${Date.now()}`;

        writeFileSync(`/tmp/${uploadedVideo}`, videoToProcess.Body);
        writeFileSync("/tmp/manifest.ffcat", 'ffconcat version 1.0');
        mkdirSync("/tmp/videoSegments");

        spawnSync(
            "/opt/ffmpeg/ffmpeg",
            [
                "-i", `/tmp/${uploadedVideo}`,
                "-crf", "22",
                "-map", "0",
                "-segment_time", "5",
                "-reset_timestamps", "1",
                "-sc_threshold", "0",
                "-force_key_frames", "expr:gte(t,n_forced*5)",
                "-f", "segment",
                "-segment_list", "/tmp/manifest.ffcat", `/tmp/videoSegments/${jobId}-%03d${fileExtension}`
            ],
            { stdio: "inherit" }
        );

        const manifest = readFileSync(`/tmp/manifest.ffcat`);
        const segmentNamesArr = readdirSync('/tmp/videoSegments');

        // WRITE TO DYNAMODB
        saveJobData(jobId, fileBasename, segmentNamesArr, fileExtension);
        saveSegmentData(jobId, segmentNamesArr);
        sendVideoSegments("/tmp/videoSegments", outputBucketName);

        unlinkSync(`/tmp/manifest.ffcat`);
        unlinkSync(`/tmp/${uploadedVideo}`);
        //unlinkSync(`/tmp/videoSegments`); need to remove all segments. This does not work. 03112020

        await s3
            .putObject({
                Bucket: outputBucketName,
                Key: `${jobId}/manifest.ffcat`,
                Body: manifest
            })
            .promise();
    }
};
