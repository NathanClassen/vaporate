# vaporate
#### The video segmenting and transcoding pipeline. 

After pulling and before `sls deploy`, you'll want to change names of buckets in yml file.

Severless Framework creates only those buckets that have events attached to them. In this yaml, the transcoded videos bucket does not have any events attached to it and will therefore need to be created manually.

This program facilitates:

- upload a video to the uploadedvids bucket
- have that video split into segments and have those segments sent to the chunkedvids bucket
- send the manifest file to the chunkedvids bucket
- have each segement transcoded to mp4 and sent to the transcodedvids bucket


## Useful articles

[FFMPEG doc for `ssegment` and muxers and demuxers in general](https://ffmpeg.org/ffmpeg-formats.html#segment_002c-stream_005fsegment_002c-ssegment)
<br /> _Muxers are configured elements in FFmpeg which allow writing multimedia streams to a particular type of file._
<br /> In the chunking function we use the `-f` command to force the output to `ssegment`; a muxer

[Node.js docs for ChildProcess, which lets us create processes with JS](https://nodejs.org/api/child_process.html)<br />
[Node.js docs for FileSystem, which lets us interact with a file system using JS](https://nodejs.org/api/fs.html)<br />
These libraries are used in both the chunk and transcode functions