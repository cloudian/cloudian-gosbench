package driver

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rs/zerolog/log"
)

// WorkItem is an interface for general work operations
// They can be read,write,list,delete or a stopper
type WorkItem interface {
	Prepare() error
	Do() error
	Clean() error
}

// ReadOperation stands for a read operation
type ReadOperation struct {
	TestName                 string
	Bucket                   string
	ObjectName               string
	ObjectSize               uint64
	WorksOnPreexistingObject bool
	MPUEnabled               bool
	PartSize                 uint64
	MPUConcurrency           int
}

// WriteOperation stands for a write operation
type WriteOperation struct {
	TestName       string
	Bucket         string
	ObjectName     string
	ObjectSize     uint64
	MPUEnabled     bool
	PartSize       uint64
	MPUConcurrency int
}

// ListOperation stands for a list operation
type ListOperation struct {
	TestName                 string
	Bucket                   string
	ObjectName               string
	ObjectSize               uint64
	WorksOnPreexistingObject bool
	MPUEnabled               bool
	PartSize                 uint64
	MPUConcurrency           int
	UseV2                    bool
}

// DeleteOperation stands for a delete operation
type DeleteOperation struct {
	TestName                 string
	Bucket                   string
	ObjectName               string
	ObjectSize               uint64
	WorksOnPreexistingObject bool
	MPUEnabled               bool
	PartSize                 uint64
	MPUConcurrency           int
}

// Stopper marks the end of a workqueue when using
// maxOps as testCase end criterium
type Stopper struct{}

// KV is a simple key-value struct
type KV struct {
	Key   string
	Value float64
}

// Workqueue contains the Queue and the valid operation's
// values to determine which operation should be done next
// in order to satisfy the set ratios.
type Workqueue struct {
	OperationValues []KV
	Queue           *[]WorkItem
}

// GetNextOperation evaluates the operation values and returns which
// operation should happen next
func GetNextOperation(Queue *Workqueue) string {
	sort.Slice(Queue.OperationValues, func(i, j int) bool {
		return Queue.OperationValues[i].Value < Queue.OperationValues[j].Value
	})
	return Queue.OperationValues[0].Key
}

var WorkContext context.Context

// WorkCancel is the function to stop the execution of jobs
var WorkCancel context.CancelFunc

// IncreaseOperationValue increases the given operation's value by the set amount
func IncreaseOperationValue(operation string, value float64, Queue *Workqueue) error {
	for i := range Queue.OperationValues {
		if Queue.OperationValues[i].Key == operation {
			Queue.OperationValues[i].Value += value
			return nil
		}
	}
	return fmt.Errorf("could not find requested operation %s", operation)
}

// Prepare prepares the execution of the ReadOperation
func (op *ReadOperation) Prepare() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Bool("Preexisting?", op.WorksOnPreexistingObject).Msg("Preparing ReadOperation")
	if op.WorksOnPreexistingObject {
		return nil
	}
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = manager.DefaultDownloadPartSize
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = manager.DefaultDownloadConcurrency
		}
		uploader := manager.NewUploader(HousekeepingSvc, func(d *manager.Uploader) {
			d.PartSize = int64(op.PartSize)
			d.Concurrency = op.MPUConcurrency
		})

		return putObjectMPU(uploader, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket)
	} else {
		return putObject(HousekeepingSvc, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket, int64(op.ObjectSize))
	}
}

// Prepare prepares the execution of the WriteOperation
func (op *WriteOperation) Prepare() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Msg("Preparing WriteOperation")
	return nil
}

// Prepare prepares the execution of the ListOperation
func (op *ListOperation) Prepare() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Msg("Preparing ListOperation")
	if op.WorksOnPreexistingObject {
		return nil
	}
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = uint64(manager.DefaultUploadPartSize)
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = manager.DefaultUploadConcurrency
		}
		uploader := manager.NewUploader(HousekeepingSvc, func(d *manager.Uploader) {
			d.PartSize = int64(op.PartSize)
			d.Concurrency = op.MPUConcurrency
		})
		return putObjectMPU(uploader, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket)
	} else {
		return putObject(HousekeepingSvc, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket, int64(op.ObjectSize))
	}
}

// Prepare prepares the execution of the DeleteOperation
func (op *DeleteOperation) Prepare() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Bool("Preexisting?", op.WorksOnPreexistingObject).Msg("Preparing DeleteOperation")
	if op.WorksOnPreexistingObject {
		return nil
	}
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = uint64(manager.DefaultUploadPartSize)
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = manager.DefaultUploadConcurrency
		}
		uploader := manager.NewUploader(HousekeepingSvc, func(d *manager.Uploader) {
			d.PartSize = int64(op.PartSize)
			d.Concurrency = op.MPUConcurrency
		})
		return putObjectMPU(uploader, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket)
	} else {
		return putObject(HousekeepingSvc, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket, int64(op.ObjectSize))
	}
}

// Prepare does nothing here
func (op *Stopper) Prepare() error {
	return nil
}

// Do executes the actual work of the ReadOperation
func (op *ReadOperation) Do() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Bool("Preexisting?", op.WorksOnPreexistingObject).Msg("Doing ReadOperation")
	var err error
	var duration time.Duration
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = manager.DefaultDownloadPartSize
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = manager.DefaultDownloadConcurrency
		}
		downloader := manager.NewDownloader(svc, func(d *manager.Downloader) {
			d.PartSize = int64(op.PartSize)
			d.Concurrency = op.MPUConcurrency
		})
		start := time.Now()
		err = getObjectMPU(*downloader, op.ObjectName, op.Bucket)
		duration = time.Since(start)
	} else {
		start := time.Now()
		err = getObject(svc, op.ObjectName, op.Bucket)
		duration = time.Since(start)
	}
	if err != nil {
		log.Error().Err(err).Msgf("could not read object body for %s in bucket %s", op.ObjectName, op.Bucket)
	}
	promRTLatency.WithLabelValues(op.TestName, "GET").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "GET").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "GET").Inc()
	}
	promDownloadedBytes.WithLabelValues(op.TestName, "GET").Add(float64(op.ObjectSize))
	return err
}

// Do executes the actual work of the WriteOperation
func (op *WriteOperation) Do() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Msg("Doing WriteOperation")
	var err error
	var duration time.Duration
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = uint64(s3manager.DefaultUploadPartSize)
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = s3manager.DefaultUploadConcurrency
		}
		uploader := manager.NewUploader(svc, func(d *manager.Uploader) {
			d.PartSize = int64(op.PartSize)
			d.Concurrency = op.MPUConcurrency
		})
		start := time.Now()
		err = putObjectMPU(uploader, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket)
		duration = time.Since(start)
	} else {
		start := time.Now()
		err = putObject(svc, op.ObjectName, bytes.NewReader(generateRandomBytes(op.ObjectSize)), op.Bucket, int64(op.ObjectSize))
		duration = time.Since(start)
	}
	if err != nil {
		log.Error().Err(err).Str("object", op.ObjectName).Str("bucket", op.Bucket).Msg("Failed to upload object,")
		promFailedOps.WithLabelValues(op.TestName, "PUT").Inc()
		return err
	} else {
		log.Trace().Str("bucket", op.Bucket).Str("key", op.ObjectName).Msg("Upload successful")
	}
	promRTLatency.WithLabelValues(op.TestName, "PUT").Observe(float64(duration.Milliseconds()))
	promFinishedOps.WithLabelValues(op.TestName, "PUT").Inc()
	promUploadedBytes.WithLabelValues(op.TestName, "PUT").Add(float64(op.ObjectSize))
	return err
}

// Do executes the actual work of the ListOperation
func (op *ListOperation) Do() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Msg("Doing ListOperation")
	start := time.Now()
	var err error
	if op.UseV2 {
		_, err = ListObjectsV2(svc, op.ObjectName, op.Bucket)
	} else {
		_, err = ListObjects(svc, op.ObjectName, op.Bucket)
	}
	duration := time.Since(start)
	if err != nil {
		log.Error().Err(err).Msgf("Could not find prefix %s in bucket %s when querying properties", op.ObjectName, op.Bucket)
	}
	promRTLatency.WithLabelValues(op.TestName, "LIST").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "LIST").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "LIST").Inc()
	}
	return err
}

// Do executes the actual work of the DeleteOperation
func (op *DeleteOperation) Do() error {
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Msg("Doing DeleteOperation")
	start := time.Now()
	err := deleteObject(svc, op.ObjectName, op.Bucket)
	duration := time.Since(start)
	if err != nil {
		// Cast err to awserr.Error to handle specific error codes.
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.Error().Err(aerr).Msgf("Could not find object %s in bucket %s for deletion", op.ObjectName, op.Bucket)
		}
	}
	promRTLatency.WithLabelValues(op.TestName, "DELETE").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "DELETE").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "DELETE").Inc()
	}
	return err
}

// Do does nothing here
func (op *Stopper) Do() error {
	return nil
}

// Clean removes the objects and buckets left from the previous ReadOperation
func (op *ReadOperation) Clean() error {
	if op.WorksOnPreexistingObject {
		return nil
	}
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Bool("Preexisting?", op.WorksOnPreexistingObject).Msg("Cleaning up ReadOperation")
	return deleteObject(HousekeepingSvc, op.ObjectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous WriteOperation
func (op *WriteOperation) Clean() error {
	return deleteObject(HousekeepingSvc, op.ObjectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous ListOperation
func (op *ListOperation) Clean() error {
	if op.WorksOnPreexistingObject {
		return nil
	}
	log.Trace().Str("bucket", op.Bucket).Str("object", op.ObjectName).Bool("Preexisting?", op.WorksOnPreexistingObject).Msg("Cleaning up ListOperation")
	return deleteObject(HousekeepingSvc, op.ObjectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous DeleteOperation
func (op *DeleteOperation) Clean() error {
	return nil
}

// Clean does nothing here
func (op *Stopper) Clean() error {
	return nil
}

// DoWork processes the workitems in the workChannel until
// either the time runs out or a stopper is found
func DoWork(workChannel chan WorkItem, notifyChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-notifyChan:
			log.Debug().Msg("Runtime over - Got timeout from work context")
			return
		case work := <-workChannel:
			switch work.(type) {
			case *Stopper:
				log.Debug().Msg("Found the end of the work Queue - stopping")
				return
			}
			err := work.Do()
			if err != nil {
				log.Error().Err(err).Msg("Issues when performing work - ignoring")
			}
		}
	}
}

func generateRandomBytes(size uint64) []byte {
	now := time.Now()
	random := make([]byte, size)
	n, err := rand.Read(random)
	if err != nil {
		log.Fatal().Err(err).Msg("error generating random bytes")
	}
	log.Trace().Msgf("Generated %d random bytes in %v", n, time.Since(now))
	return random
}
