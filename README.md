# iostream

iostream is a golang library meant for converting between streaming interfaces.

The primary goal of iostream for now is converting an io.WriterAt to an io.Writer as long as the WriterAt has a predictable pattern of spawning multiple goroutines that are try to fill out the file from the start. This was primarily built as a way to use the [AWS s3 download manager](https://godoc.org/github.com/aws/aws-sdk-go/service/s3#hdr-Download_Manager) to write to an `io.Writer` instead of an `io.WriterAt` which usually requires either a full file or a full buffer.

## Usage

### OpenWriterAtStream
```golang
writer := new(bytes.Buffer)
stream := iostream.OpenWriterAtStream(writer, 2, 2)
defer stream.Close()
if _, err := stream.WriteAt([]byte("1234"), 0); err != nil {
	panic(err)
}
```

### Using with aws s3 [download manager](https://godoc.org/github.com/aws/aws-sdk-go/service/s3#hdr-Download_Manager)
```golang
// Write the contents of S3 Object to a writer (just using a buffer in this case but could be any streaming writer)
writer := new(bytes.Buffer)
// Create a buffer with at least the number of concurreny downloader goroutines that will be running.
// Although ideally ideally we even add a few more so that if one of the first few downloads gets stalled other goroutines
// can continue making progress.
// The internal buffer size will end up being numBuffers * bufferSize.
numBuffers := s3manager.DefaultDownloadConcurrency + 3
bufferSize := s3manager.DefaultDownloadPartSize
stream := iostream.OpenWriterAtStream(writer, numBuffers, bufferSize)
defer stream.Close()
n, err := downloader.Download(f, &s3.GetObjectInput{
    Bucket: aws.String(myBucket),
    Key:    aws.String(myString),
})
if err != nil {
    return fmt.Errorf("failed to download file, %v", err)
}
```

See the [tests](iostream_test.go) for more usage examples
