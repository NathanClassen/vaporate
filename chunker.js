import { v4 as uuidv4 } from 'uuid';
const { spawnSync } = require("child_process");
const { readFileSync, writeFileSync, unlinkSync, readdirSync, mkdirSync } = require("fs");
const path = require("path");
const AWS = require("aws-sdk");
const outputBucketName = process.env.outputBucket;
const s3 = new AWS.S3();

AWS.config.update({ endpoint: "https://dynamodb.us-east-1.amazonaws.com" });

const docClient = new AWS.DynamoDB.DocumentClient();
const jobsTable = "Jobs";
const segmentsTable = "Segments";

const sendVideoSegments = (s3path, bucket) => {
    readdirSync(s3path).forEach(fileName => {
        const filePath = path.join(s3path, fileName);
        const bucketKey = fileName;
        const data = readFileSync(filePath);
        const params = { Bucket: bucket, Key: bucketKey, Body: data };

        s3.putObject(params, (err) => {
            if (err) {
                console.log(err);
            }
        });

    });
}

const saveJobData = (id, tasks, file, fileFormat) => {
    const id = id;
    const filename = file;
    const inputType = fileFormat;
    const totalTasks = tasks.length;

    const params = {
        TableName: jobsTable,
        Item: {
            "id": id,
            "type": "job",
            "filename": filename,
            "inputType": inputType,
            //"outputType": outputType,
            "totalTasks": totalTasks,
            "finishedTasks": 0,
            "status": "pending",
            "createdAt": new Date,
            "completedAt": null
        }
    };

    console.log("Adding a new job...");
    docClient.put(params, (err, data) => {
        if (err) {
            console.log("Unable to add job. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Added job:", JSON.stringify(data, null, 2));
        }
    });
}

const saveSegmentData = (jobId, file, fileFormat) => {
    const id = uuid4();
    const filename = file;
    const inputType = fileFormat;

    const params = {
        TableName: segmentsTable,
        Item: {
            "id": id,
            "type": "segment",
            "filename": filename,
            "job_id": jobId,
            "inputType": inputType,
            //"outputType": outputType,
            "status": "pending",
            "createdAt": new Date,
            "completedAt": null
        }
    };

    console.log("Adding a new segment...");
    docClient.put(params, (err, data) => {
        if (err) {
            console.log("Unable to add segment. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Added segment:", JSON.stringify(data, null, 2));
        }
    });

}

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

        const jobId = uuid4();
        const fileInfo = [...`${record.s3.object.key}`.match(/(.+)\.(.+)/)];
        const filename = fileInfo[1];
        const inputFormat = fileInfo[2];

        // write file to disk
        writeFileSync(`/tmp/${record.s3.object.key}`, s3Object.Body);
        writeFileSync("/tmp/chunks.ffcat", 'ffconcat version 1.0');
        mkdirSync("/tmp/videoSegments");
        // convert to mp4!
        spawnSync(
            "/opt/ffmpeg/ffmpeg",
            [
                "-i", `/tmp/${record.s3.object.key}`,
                "-c:v", "libx264",
                "-crf", "22",
                "-map", "0",
                "-segment_time", "5",
                "-reset_timestamps", "1",
                "-sc_threshold", "0",
                "-force_key_frames", "expr:gte(t,n_forced*5)",
                "-f", "segment",
                "-segment_list", "/tmp/chunks.ffcat", "/tmp/videoSegments/chunk%03d.mp4"

                /* "-i", `/tmp/${record.s3.object.key}`,
                "-map", "0",
                "-codec:v", "libx264",
                "-codec:a", "aac",
                "-f", "ssegment",
                "-segment_list", "/tmp/chunks.ffcat", "/tmp/videoSegments/chunk%03d.mp4" */
            ],
            { stdio: "inherit" }
        );

        const segmentListFile = readFileSync(`/tmp/chunks.ffcat`);
        const segmentList = readdirSync('/tmp/videoSegments');

        // WRITE TO DYNAMODB ##################################################
        saveJobData(jobId, segmentList, filename, inputFormat);
        saveSegmentData(jobId, filename, inputFormat);
        //#####################################################################

        // send all segments to s3 for transcoding
        sendVideoSegments("/tmp/videoSegments", outputBucketName);
        // read segment manifest from disk

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
