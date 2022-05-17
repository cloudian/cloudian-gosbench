package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/cloudian/cloudian-gosbench/internal/gosbench/common"
	"github.com/cloudian/cloudian-gosbench/internal/gosbench/driver"

	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

var config common.DriverConf
var prometheusPort int
var debug, trace bool

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var serverAddress string
	flag.StringVar(&serverAddress, "s", "", "Gosbench Server IP and Port in the form '192.168.1.1:2000'")
	var logFile string
	flag.StringVar(&logFile, "l", "", "file location to write logs output")
	flag.IntVar(&prometheusPort, "p", 9995, "Port on which the Prometheus Exporter will be available. Default: 9995")
	flag.BoolVar(&debug, "d", false, "enable debug log output")
	flag.BoolVar(&trace, "t", false, "enable trace log output")
	flag.Parse()
	if serverAddress == "" {
		log.Fatal("-s is a mandatory parameter - please specify the server IP and Port")
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	} else if trace {
		log.SetLevel(log.TraceLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if logFile != "" {
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			defer file.Close()
			log.SetOutput(file)
		} else {
			log.SetOutput(os.Stdout)
			log.Warn("failed to log to file, using default stdout")
		}
	} else {
		log.SetOutput(os.Stdout)
	}

	for {
		err := connectToServer(serverAddress)
		if err != nil {
			log.WithError(err).Error("Issues with server connection")
			time.Sleep(time.Second)
		}
	}
}

func connectToServer(serverAddress string) error {
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		// return errors.New("Could not establish connection to server yet")
		return err
	}
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	_ = encoder.Encode("ready for work")

	var response common.DriverMessage
	Workqueue := &driver.Workqueue{
		Queue: &[]driver.WorkItem{},
	}
	for {
		err := decoder.Decode(&response)
		if err != nil {
			log.WithField("message", response).WithError(err).Error("Server responded unusually - reconnecting")
			conn.Close()
			return errors.New("issue when receiving work from server")
		}
		log.Tracef("Response: %+v", response)
		switch response.Message {
		case "init":
			config = *response.Config
			log.WithField("DriverID", config.DriverID).Info("Got config from server - starting preparations now")

			driver.RandomData = driver.GenerateRandomBytes(config.Test.Objects.SizeMax)
			driver.InitS3(*config.S3Config)
			driver.InitPrometheus(prometheusPort)
			fillWorkqueue(config.Test, Workqueue, config.DriverID, config.Test.DriversShareBuckets)

			for _, work := range *Workqueue.Queue {
				err = work.Prepare()
				if err != nil {
					log.WithError(err).Error("Error during work preparation - ignoring")
				}
			}
			log.Info("Preparations finished - waiting on server to start work")
			_ = encoder.Encode(common.DriverMessage{Message: "preparations done"})
		case "start work":
			if config == (common.DriverConf{}) || len(*Workqueue.Queue) == 0 {
				log.Fatal("Was instructed to start work - but the preparation step is incomplete - reconnecting")
				return nil
			}
			log.Infof("Starting to work on %s", config.Test.Name)
			duration := PerfTest(config.Test, Workqueue, config.DriverID)
			benchResults := driver.GetCurrentPromValues(config.Test)
			benchResults.Duration = duration
			benchResults.Bandwidth = benchResults.Bytes / duration.Seconds()
			benchResults.OpsPerSecond = benchResults.Operations / duration.Seconds()
			log.Infof("PROM VALUES %s, %s, %s, %d, %.2f, %.2f, %.2f, %.2f ops/s, %.2f MB, %.2f MB/s, %.2f ms, %.2f ms, %.2f%%, %.2f s, %s",
				benchResults.Host, benchResults.TestName, benchResults.OperationName, benchResults.Workers, benchResults.ObjectSize,
				benchResults.Operations, benchResults.FailedOperations, benchResults.OpsPerSecond, benchResults.Bytes/(1024*1024),
				benchResults.Bandwidth/(1024*1024), benchResults.RTLatencyAvg, benchResults.TTFBLatencyAvg, benchResults.SuccessRatio*100, benchResults.Duration.Seconds(),
				benchResults.Options)
			_ = encoder.Encode(common.DriverMessage{Message: "work done", BenchResult: benchResults})
			// Work is done - return to being a ready driver by reconnecting
			return nil
		case "shutdown":
			log.Info("Server told us to shut down - all work is done for today")
			os.Exit(0)
		}
	}
}

// PerfTest runs a performance test as configured in testConfig
func PerfTest(testConfig *common.TestCaseConfiguration, Workqueue *driver.Workqueue, driverID string) time.Duration {
	workChannel := make(chan driver.WorkItem, len(*Workqueue.Queue))
	doneChannel := make(chan bool)

	startTime := time.Now().UTC()
	driver.PromTestStart.WithLabelValues(testConfig.Name).Set(float64(startTime.UnixNano() / int64(1000000)))
	driver.CurrentTest = testConfig.Name

	if time.Duration(testConfig.Runtime).Seconds() != 0 {
		driver.WorkContext, driver.WorkCancel = context.WithCancel(context.Background())
	}

	for worker := 0; worker < testConfig.Workers; worker++ {
		go driver.DoWork(workChannel, doneChannel)
	}
	log.Infof("Started %d workers", testConfig.Workers)
	if time.Duration(testConfig.Runtime).Seconds() != 0 {
		workUntilTimeout(Workqueue, workChannel, time.Duration(testConfig.Runtime))
	} else {
		workUntilOps(Workqueue, workChannel, testConfig.OpsDeadline, testConfig.Workers)
	}
	// Wait for all the goroutines to finish
	for i := 0; i < testConfig.Workers; i++ {
		<-doneChannel
	}
	endTime := time.Now().UTC()
	driver.PromTestEnd.WithLabelValues(testConfig.Name).Set(float64(endTime.UnixNano() / int64(1000000)))
	log.Info("All clients finished")

	if testConfig.CleanAfter {
		log.Info("Housekeeping started")
		for _, work := range *Workqueue.Queue {
			err := work.Clean()
			if err != nil {
				log.WithError(err).Error("Error during cleanup - ignoring")
			}
		}

		bucketStopCount := testConfig.Buckets.NumberMax
		if testConfig.Buckets.NumberDistribution == "constant" {
			bucketStopCount = testConfig.Buckets.NumberMin + 1
		}

		for bucket := testConfig.Buckets.NumberMin; bucket < bucketStopCount; bucket++ {
			bucketCount := common.EvaluateDistribution(testConfig.Buckets.NumberMin, testConfig.Buckets.NumberMax,
				&testConfig.Buckets.NumberLast, 1, testConfig.Buckets.NumberDistribution)
			bucketName := fmt.Sprintf("%s-%s-%d", testConfig.BucketPrefix, driverID, bucketCount)
			if testConfig.DriversShareBuckets {
				bucketName = fmt.Sprintf("%s-%d", testConfig.BucketPrefix, bucket)
			}
			err := driver.DeleteBucket(driver.HousekeepingSvc, bucketName)
			if err != nil {
				log.WithError(err).Error("Error during bucket deleting - ignoring")
			}
		}
		log.Info("Housekeeping finished")
	}
	// Sleep to ensure Prometheus can still scrape the last information before we restart the driver
	time.Sleep(10 * time.Second)
	return endTime.Sub(startTime)
}

func workUntilTimeout(Workqueue *driver.Workqueue, workChannel chan driver.WorkItem, runtime time.Duration) {
	//driver.WorkContext, driver.WorkCancel = context.WithCancel(context.Background())
	log.Debugf("Work duration = %s", runtime.String())
	timer := time.NewTimer(runtime)
	for {
		for _, work := range *Workqueue.Queue {
			select {
			case <-timer.C:
				log.Debug("Reached Runtime end")
				driver.WorkCancel()
				return
			case workChannel <- work:
			}
		}
		// for _, work := range *Workqueue.Queue {
		// 	switch work.(type) {
		// 	case driver.DeleteOperation:
		// 		log.Debug("Re-Running Work preparation for delete job started")
		// 		err := work.Prepare()
		// 		if err != nil {
		// 			log.WithError(err).Error("Error during work preparation - ignoring")
		// 		}
		// 		log.Debug("Delete preparation re-run finished")
		// 	}
		// }
	}
}

func workUntilOps(Workqueue *driver.Workqueue, workChannel chan driver.WorkItem, maxOps uint64, numberOfWorker int) {
	currentOps := uint64(0)
	for {
		for _, work := range *Workqueue.Queue {
			if currentOps >= maxOps {
				log.Debug("Reached OpsDeadline ... waiting for workers to finish")
				for worker := 0; worker < numberOfWorker; worker++ {
					workChannel <- driver.Stopper{}
				}
				return
			}
			currentOps++
			workChannel <- work
		}
		// for _, work := range *Workqueue.Queue {
		// 	switch work.(type) {
		// 	case driver.DeleteOperation:
		// 		log.Debug("Re-Running Work preparation for delete job started")
		// 		err := work.Prepare()
		// 		if err != nil {
		// 			log.WithError(err).Error("Error during work preparation - ignoring")
		// 		}
		// 		log.Debug("Delete preparation re-run finished")
		// 	}
		// }
	}
}

func fillWorkqueue(testConfig *common.TestCaseConfiguration, Workqueue *driver.Workqueue, driverID string, shareBucketName bool) {

	if testConfig.ReadWeight > 0 {
		Workqueue.OperationValues = append(Workqueue.OperationValues, driver.KV{Key: "read"})
	}
	if testConfig.ExistingReadWeight > 0 {
		Workqueue.OperationValues = append(Workqueue.OperationValues, driver.KV{Key: "existing_read"})
	}
	if testConfig.WriteWeight > 0 {
		Workqueue.OperationValues = append(Workqueue.OperationValues, driver.KV{Key: "write"})
	}
	if testConfig.ListWeight > 0 {
		Workqueue.OperationValues = append(Workqueue.OperationValues, driver.KV{Key: "list"})
	}
	if testConfig.DeleteWeight > 0 {
		Workqueue.OperationValues = append(Workqueue.OperationValues, driver.KV{Key: "delete"})
	}
	if testConfig.ExistingDeleteWeight > 0 {
		Workqueue.OperationValues = append(Workqueue.OperationValues, driver.KV{Key: "existing_delete"})
	}

	bucketStopCount := testConfig.Buckets.NumberMax
	if testConfig.Buckets.NumberDistribution == "constant" {
		bucketStopCount = testConfig.Buckets.NumberMin + 1
	}

	for bucket := testConfig.Buckets.NumberMin; bucket < bucketStopCount; bucket++ {
		bucketCount := common.EvaluateDistribution(testConfig.Buckets.NumberMin, testConfig.Buckets.NumberMax,
			&testConfig.Buckets.NumberLast, 1, testConfig.Buckets.NumberDistribution)
		bucketName := fmt.Sprintf("%s-%s-%d", testConfig.BucketPrefix, driverID, bucketCount)
		if shareBucketName {
			bucketName = fmt.Sprintf("%s-%d", testConfig.BucketPrefix, bucket)
		}
		err := driver.CreateBucket(driver.HousekeepingSvc, bucketName)
		if err != nil {
			log.WithError(err).WithField("bucket", bucketName).Error("Error when creating bucket")
		}

		objectDriverPrefix := fmt.Sprintf("%s-%s", testConfig.ObjectPrefix, driverID)

		var PreExistingReadObjects []*s3.Object
		var PreExistingReadObjectCount uint64
		var PreExistingReadObjectIndex uint64
		if testConfig.ExistingReadWeight > 0 {
			PreExistingReadObjects, err = driver.GetExistingReadObjectList(driver.HousekeepingSvc, objectDriverPrefix, bucketName)
			PreExistingReadObjectCount = uint64(len(PreExistingReadObjects))
			log.Debugf("Found %d objects in bucket %s with prefix %s to read", PreExistingReadObjectCount, bucketName, objectDriverPrefix)
			if err != nil {
				log.WithError(err).Fatalf("Problems when listing contents of bucket %s to read", bucketName)
			}
		}

		var PreExistingDeleteObjects []*s3.Object
		var PreExistingDeleteObjectCount uint64
		var PreExistingDeleteObjectIndex uint64
		if testConfig.ExistingDeleteWeight > 0 {
			PreExistingDeleteObjects, err = driver.GetExistingReadObjectList(driver.HousekeepingSvc, objectDriverPrefix, bucketName)
			PreExistingDeleteObjectCount = uint64(len(PreExistingDeleteObjects))
			log.Debugf("Found %d objects in bucket %s with prefix %s to delete", PreExistingDeleteObjectCount, bucketName, objectDriverPrefix)
			if err != nil {
				log.WithError(err).Fatalf("Problems when listing contents of bucket %s to delete", bucketName)
			}
		}

		objectStopCount := testConfig.Objects.NumberMax
		if testConfig.Objects.NumberDistribution == "constant" {
			objectStopCount = testConfig.Objects.NumberMin + 1
		}

		for object := testConfig.Objects.NumberMin; object < objectStopCount; object++ {
			objectCount := common.EvaluateDistribution(testConfig.Objects.NumberMin, testConfig.Objects.NumberMax,
				&testConfig.Objects.NumberLast, 1, testConfig.Objects.NumberDistribution)
			objectSize := common.EvaluateDistribution(testConfig.Objects.SizeMin, testConfig.Objects.SizeMax,
				&testConfig.Objects.SizeLast, 1, testConfig.Objects.SizeDistribution)

			nextOp := driver.GetNextOperation(Workqueue)
			switch nextOp {
			case "read":
				err := driver.IncreaseOperationValue(nextOp, 1/float64(testConfig.ReadWeight), Workqueue)
				if err != nil {
					log.WithError(err).Error("Could not increase operational Value - ignoring")
				}
				new := driver.ReadOperation{
					TestName:                 testConfig.Name,
					Bucket:                   bucketName,
					ObjectName:               fmt.Sprintf("%s-%d", objectDriverPrefix, objectCount),
					ObjectSize:               objectSize,
					WorksOnPreexistingObject: false,
					MPUEnabled:               testConfig.Multipart.ReadMPUEnabled,
					PartSize:                 testConfig.Multipart.ReadPartSize,
					MPUConcurrency:           testConfig.Multipart.ReadConcurrency,
				}
				*Workqueue.Queue = append(*Workqueue.Queue, new)
			case "existing_read":
				err := driver.IncreaseOperationValue(nextOp, 1/float64(testConfig.ExistingReadWeight), Workqueue)
				if err != nil {
					log.WithError(err).Error("Could not increase operational Value - ignoring")
				}
				if PreExistingReadObjectIndex >= PreExistingReadObjectCount {
					PreExistingReadObjectIndex = 0
				}
				new := driver.ReadOperation{
					TestName:                 testConfig.Name,
					Bucket:                   bucketName,
					ObjectName:               *PreExistingReadObjects[PreExistingReadObjectIndex].Key,
					ObjectSize:               uint64(*PreExistingReadObjects[PreExistingReadObjectIndex].Size),
					WorksOnPreexistingObject: true,
					MPUEnabled:               testConfig.Multipart.ReadMPUEnabled,
					PartSize:                 testConfig.Multipart.ReadPartSize,
					MPUConcurrency:           testConfig.Multipart.ReadConcurrency,
				}
				PreExistingReadObjectIndex++
				*Workqueue.Queue = append(*Workqueue.Queue, new)
			case "write":
				err := driver.IncreaseOperationValue(nextOp, 1/float64(testConfig.WriteWeight), Workqueue)
				if err != nil {
					log.WithError(err).Error("Could not increase operational Value - ignoring")
				}
				new := driver.WriteOperation{
					TestName:       testConfig.Name,
					Bucket:         bucketName,
					ObjectName:     fmt.Sprintf("%s-%d", objectDriverPrefix, objectCount),
					ObjectSize:     objectSize,
					MPUEnabled:     testConfig.Multipart.WriteMPUEnabled,
					PartSize:       testConfig.Multipart.WritePartSize,
					MPUConcurrency: testConfig.Multipart.WriteConcurrency,
				}
				*Workqueue.Queue = append(*Workqueue.Queue, new)
			case "list":
				err := driver.IncreaseOperationValue(nextOp, 1/float64(testConfig.ListWeight), Workqueue)
				if err != nil {
					log.WithError(err).Error("Could not increase operational Value - ignoring")
				}
				new := driver.ListOperation{
					TestName:       testConfig.Name,
					Bucket:         bucketName,
					ObjectName:     fmt.Sprintf("%s-%d", objectDriverPrefix, objectCount),
					ObjectSize:     objectSize,
					MPUEnabled:     testConfig.Multipart.WriteMPUEnabled,
					PartSize:       testConfig.Multipart.WritePartSize,
					MPUConcurrency: testConfig.Multipart.WriteConcurrency,
				}
				*Workqueue.Queue = append(*Workqueue.Queue, new)
			case "delete":
				err := driver.IncreaseOperationValue(nextOp, 1/float64(testConfig.DeleteWeight), Workqueue)
				if err != nil {
					log.WithError(err).Error("Could not increase operational Value - ignoring")
				}
				new := driver.DeleteOperation{
					TestName:                 testConfig.Name,
					Bucket:                   bucketName,
					ObjectName:               fmt.Sprintf("%s-%d", objectDriverPrefix, objectCount),
					ObjectSize:               objectSize,
					WorksOnPreexistingObject: false,
					MPUEnabled:               testConfig.Multipart.WriteMPUEnabled,
					PartSize:                 testConfig.Multipart.WritePartSize,
					MPUConcurrency:           testConfig.Multipart.WriteConcurrency,
				}
				*Workqueue.Queue = append(*Workqueue.Queue, new)
			case "existing_delete":
				err := driver.IncreaseOperationValue(nextOp, 1/float64(testConfig.ExistingReadWeight), Workqueue)
				if err != nil {
					log.WithError(err).Error("Could not increase operational Value - ignoring")
				}
				if PreExistingDeleteObjectIndex < PreExistingDeleteObjectCount {
					new := driver.DeleteOperation{
						TestName:                 testConfig.Name,
						Bucket:                   bucketName,
						ObjectName:               *PreExistingDeleteObjects[PreExistingDeleteObjectIndex].Key,
						ObjectSize:               uint64(*PreExistingDeleteObjects[PreExistingDeleteObjectIndex].Size),
						WorksOnPreexistingObject: true,
						MPUEnabled:               testConfig.Multipart.ReadMPUEnabled,
						PartSize:                 testConfig.Multipart.ReadPartSize,
						MPUConcurrency:           testConfig.Multipart.ReadConcurrency,
					}
					PreExistingDeleteObjectIndex++
					*Workqueue.Queue = append(*Workqueue.Queue, new)
				}
			}
		}
	}
}
