# iostream - a golang library

The primary goal of iostream for now is converting an io.WriterAt to an io.Writer as long as the WriterAt has a predictable pattern of spawning multiple goroutines that are try to fill out the file from the start.
