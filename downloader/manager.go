package downloader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PullRequestInc/iostream"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const userAgentKey = "s3-transfer"

// DefaultDownloadPartSize is the default range of bytes to get at a time when
// using Download().
const DefaultDownloadPartSize = 1024 * 1024 * 5

// DefaultDownloadConcurrency is the default number of goroutines to spin up
// when using Download().
const DefaultDownloadConcurrency = 5

// DefaultMaxBufferedParts is the default number of buffered parts internal to the downloader
const DefaultMaxBufferedParts = 5

// DefaultPartBodyMaxRetries is the default number of retries to make when a part fails to upload.
const DefaultPartBodyMaxRetries = 3

type errReadingBody struct {
	err error
}

func (e *errReadingBody) Error() string {
	return fmt.Sprintf("failed to read part body: %v", e.err)
}

func (e *errReadingBody) Unwrap() error {
	return e.err
}

// The Downloader structure that calls Download(). It is safe to call Download()
// on this structure for multiple objects and across concurrent goroutines.
// Mutating the Downloader's properties is not safe to be done concurrently.
type Downloader struct {
	// The size (in bytes) to request from S3 for each part.
	// The minimum allowed part size is 5MB, and  if this value is set to zero,
	// the DefaultDownloadPartSize value will be used.
	//
	// PartSize is ignored if the Range input parameter is provided.
	PartSize int64

	// PartBodyMaxRetries is the number of retry attempts to make for failed part uploads
	PartBodyMaxRetries int

	// Enable Logging of part download retry attempts
	LogInterruptedDownloads bool

	// The number of goroutines to spin up in parallel when sending parts.
	// If this is set to zero, the DefaultDownloadConcurrency value will be used.
	//
	// Concurrency of 1 will download the parts sequentially.
	Concurrency int

	// MaxBufferedParts should be greater than or equal to Concurrency
	// It represents how many PartSize buffers could potentially be buffered while
	// waiting on the next bytes to write to the writer.
	MaxBufferedParts int

	// An S3 client to use when performing downloads.
	S3 manager.DownloadAPIClient

	// List of client options that will be passed down to individual API
	// operation requests made by the downloader.
	ClientOptions []func(*s3.Options)

	// buffer pool containing bytes.Buffers
	BufPool iostream.BufPool
}

// WithDownloaderClientOptions appends to the Downloader's API request options.
func WithDownloaderClientOptions(opts ...func(*s3.Options)) func(*Downloader) {
	return func(d *Downloader) {
		d.ClientOptions = append(d.ClientOptions, opts...)
	}
}

// NewDownloader creates a new Downloader instance to downloads objects from
// S3 in concurrent chunks. Pass in additional functional options  to customize
// the downloader behavior. Requires a client.ConfigProvider in order to create
// a S3 service client. The session.Session satisfies the client.ConfigProvider
// interface.
//
// Example:
// 	// Load AWS Config
//	cfg, err := config.LoadDefaultConfig(context.TODO())
//	if err != nil {
//		panic(err)
//	}
//
//	// Create an S3 client using the loaded configuration
//	s3.NewFromConfig(cfg)
//
//	// Create a downloader passing it the S3 client
//	downloader := manager.NewDownloader(s3.NewFromConfig(cfg))
//
//	// Create a downloader with the client and custom downloader options
//	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
//		d.PartSize = 64 * 1024 * 1024 // 64MB per part
//	})
func NewDownloader(c manager.DownloadAPIClient, bufPool iostream.BufPool, options ...func(*Downloader)) *Downloader {
	d := &Downloader{
		S3:                 c,
		PartSize:           DefaultDownloadPartSize,
		PartBodyMaxRetries: DefaultPartBodyMaxRetries,
		Concurrency:        DefaultDownloadConcurrency,
		MaxBufferedParts:   DefaultMaxBufferedParts,
		BufPool:            bufPool,
	}
	for _, option := range options {
		option(d)
	}

	return d
}

// Download downloads an object in S3 and writes the payload into w
// using concurrent GET requests. The n int64 returned is the size of the object downloaded
// in bytes.
//
// DownloadWithContext is the same as Download with the additional support for
// Context input parameters. The Context must not be nil. A nil Context will
// cause a panic. Use the Context to add deadlining, timeouts, etc. The
// DownloadWithContext may create sub-contexts for individual underlying
// requests.
//
// Additional functional options can be provided to configure the individual
// download. These options are copies of the Downloader instance Download is
// called from. Modifying the options will not impact the original Downloader
// instance. Use the WithDownloaderClientOptions helper function to pass in request
// options that will be applied to all API operations made with this downloader.
//
// The w io.WriterAt can be satisfied by an os.File to do multipart concurrent
// downloads, or in memory []byte wrapper using aws.WriteAtBuffer. In case you download
// files into memory do not forget to pre-allocate memory to avoid additional allocations
// and GC runs.
//
// Example:
//	// pre-allocate in memory buffer, where headObject type is *s3.HeadObjectOutput
//	buf := make([]byte, int(headObject.ContentLength))
//	// wrap with aws.WriteAtBuffer
//	w := s3manager.NewWriteAtBuffer(buf)
//	// download file into the memory
//	numBytesDownloaded, err := downloader.Download(ctx, w, &s3.GetObjectInput{
//		Bucket: aws.String(bucket),
//		Key:    aws.String(item),
//	})
//
// Specifying a Downloader.Concurrency of 1 will cause the Downloader to
// download the parts from S3 sequentially.
//
// It is safe to call this method concurrently across goroutines.
//
// GetObjectInputs with the "Range" value provided are not allowed and will return an error.
func (d Downloader) Download(ctx context.Context, w io.Writer, input *s3.GetObjectInput, options ...func(*Downloader)) (n int64, err error) {
	if err := validateSupportedARNType(aws.ToString(input.Bucket)); err != nil {
		return 0, err
	}

	impl := downloader{w: w, in: input, cfg: d, ctx: ctx}

	// Copy ClientOptions
	clientOptions := make([]func(*s3.Options), 0, len(impl.cfg.ClientOptions)+1)
	clientOptions = append(clientOptions, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, middleware.AddSDKAgentKey(middleware.FeatureMetadata, userAgentKey))
	})
	clientOptions = append(clientOptions, impl.cfg.ClientOptions...)
	impl.cfg.ClientOptions = clientOptions

	for _, option := range options {
		option(&impl.cfg)
	}

	impl.partBodyMaxRetries = d.PartBodyMaxRetries

	impl.totalBytes = -1
	if impl.cfg.Concurrency == 0 {
		impl.cfg.Concurrency = DefaultDownloadConcurrency
	}

	if impl.cfg.PartSize == 0 {
		impl.cfg.PartSize = DefaultDownloadPartSize
	}

	return impl.download()
}

// downloader is the implementation structure used internally by Downloader.
type downloader struct {
	ctx context.Context
	cfg Downloader

	in *s3.GetObjectInput
	w  io.Writer

	wg sync.WaitGroup
	m  sync.RWMutex

	pos        int64
	totalBytes int64
	// total bytes written to internal buffers
	written int64
	// flushed/written to 'w' writer
	flushed int64
	err     error

	partBodyMaxRetries int
}

func (d *downloader) writeOutChunkIfNeeded(chunk *dlchunk) (wroteOut bool) {
	if chunk.start < d.flushed {
		// we received an unexpected old chunk
		log.Printf("received unexpected old chunk starting at %d, after flushing %d", chunk.start, d.flushed)
		return false
	}

	log.Println("trying to write out chunk", chunk.start, d.flushed)

	// if this chunk starts where we last flushed, then we are good to flush it
	if chunk.start == d.flushed {
		log.Println("flushing chunk", chunk.start)
		n, err := io.Copy(d.w, chunk.buf)
		log.Println("flushed", chunk.start)
		if err != nil {
			d.setErr(err)
		}
		d.incrFlushed(n)

		// replace/reset recycled buffer after finishing writing it
		d.cfg.BufPool.Put(chunk.buf)
		return true
	}

	return false
}

// download performs the implementation of the object download across ranged
// GETs.
func (d *downloader) download() (n int64, err error) {
	// If range is specified fall back to single download of that range
	// this enables the functionality of ranged gets with the downloader but
	// at the cost of no multipart downloads.
	if rng := aws.ToString(d.in.Range); len(rng) > 0 {
		return 0, errors.New("the Range option is not allowed in this downloader")
	}

	// add a write thread/queue
	toWrite := make(chan *dlchunk, d.cfg.MaxBufferedParts)

	writeWg := sync.WaitGroup{}
	writeWg.Add(1)

	go func() {
		defer writeWg.Done()

		var pendingWrites []*dlchunk

		for {
			chunk := <-toWrite
			if chunk == nil {
				// channel empty
				for _, c := range pendingWrites {
					// replace/reset recycled buffer since we are cleaning up
					d.cfg.BufPool.Put(chunk.buf)
				}
				return
			}

			if d.getErr() != nil {
				// clear out channel on error
				continue
			}

			if didWrite := d.writeOutChunkIfNeeded(chunk); !didWrite {
				// we didn't write this chunk yet, so add it to the queue of pending writes
				pendingWrites = append(pendingWrites, chunk)
				// TODO: would be potentially faster with a min heap but I'm lazy and there are only going to
				// be small number of items in here (should be less than or equal to MaxBufferedParts)
				sort.SliceStable(pendingWrites, func(i, j int) bool {
					return pendingWrites[i].start < pendingWrites[j].start
				})
				continue
			}

			// we wrote out this chunk, so lets see if this unblocked any pending writes we have
			var newPendingWrites []*dlchunk
			didNotWrite := false
			for _, chunk := range pendingWrites {
				if didNotWrite {
					newPendingWrites = append(newPendingWrites, chunk)
					continue
				}
				if didWrite := d.writeOutChunkIfNeeded(chunk); !didWrite {
					didNotWrite = true
					newPendingWrites = append(newPendingWrites, chunk)
				}
			}
			pendingWrites = newPendingWrites
		}
	}()

	// Spin off first worker to check additional header information
	d.getChunk(toWrite)

	if total := d.getTotalBytes(); total >= 0 {
		// Spin up workers
		ch := make(chan dlchunk, d.cfg.Concurrency)

		for i := 0; i < d.cfg.Concurrency; i++ {
			d.wg.Add(1)
			go d.downloadPart(ch, toWrite)
		}

		// Assign work
		for d.getErr() == nil {
			if d.pos >= total {
				break // We're finished queuing chunks
			}

			// Queue the next range of bytes to read.
			ch <- dlchunk{w: d.w, start: d.pos, size: d.cfg.PartSize}
			d.pos += d.cfg.PartSize
		}

		// Wait for completion
		close(ch)
		d.wg.Wait()
	} else {
		// Checking if we read anything new
		for d.err == nil {
			d.getChunk(toWrite)
		}

		// We expect a 416 error letting us know we are done downloading the
		// total bytes. Since we do not know the content's length, this will
		// keep grabbing chunks of data until the range of bytes specified in
		// the request is out of range of the content. Once, this happens, a
		// 416 should occur.
		var responseError interface {
			HTTPStatusCode() int
		}
		if errors.As(d.err, &responseError) {
			if responseError.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable {
				d.err = nil
			}
		}
	}

	close(toWrite)
	writeWg.Wait()

	// Return error
	return d.written, d.err
}

// downloadPart is an individual goroutine worker reading from the ch channel
// and performing a GetObject request on the data with a given byte range.
//
// If this is the first worker, this operation also resolves the total number
// of bytes to be read so that the worker manager knows when it is finished.
func (d *downloader) downloadPart(ch chan dlchunk, toWrite chan *dlchunk) {
	defer d.wg.Done()

	allowedBuffered := d.cfg.PartSize * int64(d.cfg.MaxBufferedParts)

	for {
		chunk, ok := <-ch
		if !ok {
			break
		}

		for {
			if d.getErr() != nil {
				break
			}
			// if we are attempting to download a part that is too far ahead, then wait a bit
			// and check again before proceeding
			flushed := d.getFlushed()
			if flushed+allowedBuffered < chunk.start {
				time.Sleep(time.Millisecond * 1)
			}
		}

		if d.getErr() != nil {
			// Drain the channel if there is an error, to prevent deadlocking
			// of download producer.
			continue
		}

		if err := d.downloadChunk(chunk, toWrite); err != nil {
			d.setErr(err)
		}
	}
}

// getChunk grabs a chunk of data from the body.
// Not thread safe. Should only used when grabbing data on a single thread.
func (d *downloader) getChunk(toWrite chan *dlchunk) {
	if d.getErr() != nil {
		return
	}

	chunk := dlchunk{w: d.w, start: d.pos, size: d.cfg.PartSize}
	d.pos += d.cfg.PartSize

	if err := d.downloadChunk(chunk, toWrite); err != nil {
		d.setErr(err)
	}
}

// downloadChunk downloads the chunk from s3
func (d *downloader) downloadChunk(chunk dlchunk, toWrite chan *dlchunk) error {
	in := &s3.GetObjectInput{}
	Copy(in, d.in)

	// Get the next byte range of data
	in.Range = aws.String(chunk.ByteRange())

	var n int64
	var err error
	for retry := 0; retry <= d.partBodyMaxRetries; retry++ {
		// get reset recycled buffer
		chunk.buf = d.cfg.BufPool.Get()

		n, err = d.tryDownloadChunk(in, &chunk)

		if err == nil {
			break
		}

		// replace/reset recycled buffer on error
		d.cfg.BufPool.Put(chunk.buf)

		// Check if the returned error is an errReadingBody.
		// If err is errReadingBody this indicates that an error
		// occurred while copying the http response body.
		// If this occurs we unwrap the err to set the underlying error
		// and attempt any remaining retries.
		if bodyErr, ok := err.(*errReadingBody); ok {
			err = bodyErr.Unwrap()
		} else {
			return err
		}

		chunk.cur = 0

		log.Printf("object part body download interrupted %s, err, %v, retrying attempt %d", aws.ToString(in.Key), err, retry)
	}

	d.incrWritten(n)

	if err == nil {
		toWrite <- &chunk
	}

	return err
}

func (d *downloader) tryDownloadChunk(in *s3.GetObjectInput, w io.Writer) (int64, error) {
	resp, err := d.cfg.S3.GetObject(d.ctx, in, d.cfg.ClientOptions...)
	if err != nil {
		return 0, err
	}
	d.setTotalBytes(resp) // Set total if not yet set.

	n, err := io.Copy(w, resp.Body)
	resp.Body.Close()
	if err != nil {
		return n, &errReadingBody{err: err}
	}

	return n, nil
}

// getTotalBytes is a thread-safe getter for retrieving the total byte status.
func (d *downloader) getTotalBytes() int64 {
	d.m.RLock()
	defer d.m.RUnlock()

	return d.totalBytes
}

// setTotalBytes is a thread-safe setter for setting the total byte status.
// Will extract the object's total bytes from the Content-Range if the file
// will be chunked, or Content-Length. Content-Length is used when the response
// does not include a Content-Range. Meaning the object was not chunked. This
// occurs when the full file fits within the PartSize directive.
func (d *downloader) setTotalBytes(resp *s3.GetObjectOutput) {
	d.m.Lock()
	defer d.m.Unlock()

	if d.totalBytes >= 0 {
		return
	}

	if resp.ContentRange == nil {
		// ContentRange is nil when the full file contents is provided, and
		// is not chunked. Use ContentLength instead.
		if resp.ContentLength > 0 {
			d.totalBytes = resp.ContentLength
			return
		}
	} else {
		parts := strings.Split(*resp.ContentRange, "/")

		total := int64(-1)
		var err error
		// Checking for whether or not a numbered total exists
		// If one does not exist, we will assume the total to be -1, undefined,
		// and sequentially download each chunk until hitting a 416 error
		totalStr := parts[len(parts)-1]
		if totalStr != "*" {
			total, err = strconv.ParseInt(totalStr, 10, 64)
			if err != nil {
				d.err = err
				return
			}
		}

		d.totalBytes = total
	}
}

func (d *downloader) incrWritten(n int64) {
	d.m.Lock()
	defer d.m.Unlock()

	d.written += n
}

func (d *downloader) incrFlushed(n int64) {
	d.m.Lock()
	defer d.m.Unlock()

	d.flushed += n
}

func (d *downloader) getFlushed() int64 {
	d.m.RLock()
	defer d.m.RUnlock()

	return d.flushed
}

// getErr is a thread-safe getter for the error object
func (d *downloader) getErr() error {
	d.m.RLock()
	defer d.m.RUnlock()

	return d.err
}

// setErr is a thread-safe setter for the error object
func (d *downloader) setErr(e error) {
	d.m.Lock()
	defer d.m.Unlock()

	d.err = e
}

// dlchunk represents a single chunk of data to write by the worker routine.
type dlchunk struct {
	w     io.Writer
	start int64
	size  int64
	cur   int64
	buf   *bytes.Buffer
}

// Write wraps io.WriterAt for the dlchunk, writing from the dlchunk's start
// position to its end (or EOF).
func (c *dlchunk) Write(p []byte) (n int, err error) {
	if c.cur >= c.size {
		return 0, io.EOF
	}

	// n, err = c.w.WriteAt(p, c.start+c.cur)
	n, err = c.buf.Write(p)
	c.cur += int64(n)
	return
}

// ByteRange returns a HTTP Byte-Range header value that should be used by the
// client to request the chunk's range.
func (c *dlchunk) ByteRange() string {
	return fmt.Sprintf("bytes=%d-%d", c.start, c.start+c.size-1)
}
