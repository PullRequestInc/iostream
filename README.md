# iostream

iostream is a golang library meant for converting between streaming interfaces.

The primary goal of iostream for now is converting an io.WriterAt to an io.Writer as long as the WriterAt has a predictable pattern of spawning multiple goroutines that are try to fill out the file from the start. This was primarily built as a way to use the [AWS s3 download manager](https://godoc.org/github.com/aws/aws-sdk-go/service/s3#hdr-Download_Manager) to write to an `io.Writer` instead of an `io.WriterAt` which usually requires either a full file or a full buffer.

## Usage

### OpenWriterAtStream
```golang
  writer := new(bytes.Buffer)
	stream := iostream.OpenWriterAtStream(writer, 2, 2)
	if _, err := stream.WriteAt([]byte("1234"), 0); err != nil {
    panic(err)
  }
```

See the [tests](iostream_test.go) for more usage examples
