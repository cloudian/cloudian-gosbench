package driver

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

var RandomData []byte

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
	TestName       string
	Bucket         string
	ObjectName     string
	ObjectSize     uint64
	MPUEnabled     bool
	PartSize       uint64
	MPUConcurrency int
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

func init() {
	WorkContext = context.Background()
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
func (op ReadOperation) Prepare() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).WithField("Preexisting?", op.WorksOnPreexistingObject).Trace("Preparing ReadOperation")
	if op.WorksOnPreexistingObject {
		return nil
	}
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = s3manager.DefaultDownloadPartSize
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = s3manager.DefaultDownloadConcurrency
		}
		return putObjectMPU(HousekeepingSvc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, op.PartSize, op.MPUConcurrency)
	} else {
		return putObject(HousekeepingSvc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, int64(op.ObjectSize))
	}
}

// Prepare prepares the execution of the WriteOperation
func (op WriteOperation) Prepare() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Trace("Preparing WriteOperation")
	return nil
}

// Prepare prepares the execution of the ListOperation
func (op ListOperation) Prepare() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Trace("Preparing ListOperation")

	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = uint64(s3manager.DefaultUploadPartSize)
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = s3manager.DefaultUploadConcurrency
		}
		return putObjectMPU(HousekeepingSvc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, op.PartSize, op.MPUConcurrency)
	} else {
		return putObject(HousekeepingSvc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, int64(op.ObjectSize))
	}
}

// Prepare prepares the execution of the DeleteOperation
func (op DeleteOperation) Prepare() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).WithField("Preexisting?", op.WorksOnPreexistingObject).Trace("Preparing DeleteOperation")
	if op.WorksOnPreexistingObject {
		return nil
	}
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = uint64(s3manager.DefaultUploadPartSize)
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = s3manager.DefaultUploadConcurrency
		}
		return putObjectMPU(HousekeepingSvc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, op.PartSize, op.MPUConcurrency)
	} else {
		return putObject(HousekeepingSvc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, int64(op.ObjectSize))
	}
}

// Prepare does nothing here
func (op Stopper) Prepare() error {
	return nil
}

// Do executes the actual work of the ReadOperation
func (op ReadOperation) Do() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).WithField("Preexisting?", op.WorksOnPreexistingObject).Trace("Doing ReadOperation")
	var err error
	var duration time.Duration
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = s3manager.DefaultDownloadPartSize
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = s3manager.DefaultDownloadConcurrency
		}
		start := time.Now()
		err = getObjectMPU(svc, op.ObjectName, op.Bucket, op.PartSize, op.MPUConcurrency)
		duration = time.Since(start)
	} else {
		start := time.Now()
		err = getObject(svc, op.ObjectName, op.Bucket)
		duration = time.Since(start)
	}
	if err != nil {
		log.WithError(err).Errorf("could not read object body for %s in bucket %s", op.ObjectName, op.Bucket)
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
func (op WriteOperation) Do() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Trace("Doing WriteOperation")
	var err error
	var duration time.Duration
	if op.MPUEnabled {
		if op.PartSize == 0 {
			op.PartSize = uint64(s3manager.DefaultUploadPartSize)
		}
		if op.MPUConcurrency == 0 {
			op.MPUConcurrency = s3manager.DefaultUploadConcurrency
		}
		start := time.Now()
		err = putObjectMPU(svc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, op.PartSize, op.MPUConcurrency)
		duration = time.Since(start)
	} else {
		start := time.Now()
		err = putObject(svc, op.ObjectName, bytes.NewReader(RandomData[:op.ObjectSize]), op.Bucket, int64(op.ObjectSize))
		duration = time.Since(start)
	}
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			log.WithError(aerr).Errorf("Upload canceled due to timeout")
		} else {
			log.WithError(err).WithField("object", op.ObjectName).WithField("bucket", op.Bucket).Errorf("Failed to upload object,")
		}
		return err
	} else {
		log.WithField("bucket", op.Bucket).WithField("key", op.ObjectName).Tracef("Upload successful")
	}
	promRTLatency.WithLabelValues(op.TestName, "PUT").Observe(float64(duration.Milliseconds()))
	if err != nil {
		promFailedOps.WithLabelValues(op.TestName, "PUT").Inc()
	} else {
		promFinishedOps.WithLabelValues(op.TestName, "PUT").Inc()
	}
	promUploadedBytes.WithLabelValues(op.TestName, "PUT").Add(float64(op.ObjectSize))
	return err
}

// Do executes the actual work of the ListOperation
func (op ListOperation) Do() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Trace("Doing ListOperation")
	start := time.Now()
	_, err := ListObjects(svc, op.ObjectName, op.Bucket)
	duration := time.Since(start)
	if err != nil {
		// Cast err to awserr.Error to handle specific error codes.
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.WithError(aerr).Errorf("Could not find prefix %s in bucket %s when querying properties", op.ObjectName, op.Bucket)
		}
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
func (op DeleteOperation) Do() error {
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).Trace("Doing DeleteOperation")
	start := time.Now()
	err := deleteObject(svc, op.ObjectName, op.Bucket)
	duration := time.Since(start)
	if err != nil {
		// Cast err to awserr.Error to handle specific error codes.
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.WithError(aerr).Errorf("Could not find object %s in bucket %s for deletion", op.ObjectName, op.Bucket)
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
func (op Stopper) Do() error {
	return nil
}

// Clean removes the objects and buckets left from the previous ReadOperation
func (op ReadOperation) Clean() error {
	if op.WorksOnPreexistingObject {
		return nil
	}
	log.WithField("bucket", op.Bucket).WithField("object", op.ObjectName).WithField("Preexisting?", op.WorksOnPreexistingObject).Trace("Cleaning up ReadOperation")
	return deleteObject(HousekeepingSvc, op.ObjectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous WriteOperation
func (op WriteOperation) Clean() error {
	return deleteObject(HousekeepingSvc, op.ObjectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous ListOperation
func (op ListOperation) Clean() error {
	return deleteObject(HousekeepingSvc, op.ObjectName, op.Bucket)
}

// Clean removes the objects and buckets left from the previous DeleteOperation
func (op DeleteOperation) Clean() error {
	return nil
}

// Clean does nothing here
func (op Stopper) Clean() error {
	return nil
}

// DoWork processes the workitems in the workChannel until
// either the time runs out or a stopper is found
func DoWork(workChannel chan WorkItem, doneChannel chan bool) {
	for {
		select {
		case <-WorkContext.Done():
			log.Debugf("Runtime over - Got timeout from work context")
			doneChannel <- true
			return
		case work := <-workChannel:
			switch work.(type) {
			case Stopper:
				log.Debug("Found the end of the work Queue - stopping")
				doneChannel <- true
				return
			}
			err := work.Do()
			if err != nil {
				log.WithError(err).Error("Issues when performing work - ignoring")
			}
		}
	}
}

func GenerateRandomBytes(size uint64) []byte {
	now := time.Now()
	random := make([]byte, size)
	n, err := rand.Read(random)
	if err != nil {
		log.WithError(err).Fatal("I had issues getting my random bytes initialized")
	}
	log.Tracef("Generated %d random bytes in %v", n, time.Since(now))
	return random
}
