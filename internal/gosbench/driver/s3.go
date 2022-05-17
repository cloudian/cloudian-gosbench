package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/cloudian/cloudian-gosbench/internal/gosbench/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"

	//"go.opencensus.io/plugin/ochttp"
	//"go.opencensus.io/stats/view"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var svc, HousekeepingSvc *s3.S3
var ctx context.Context
var hc *http.Client
var promInitialized bool

func InitPrometheus(prometheusPort int) {
	if !promInitialized {
		// if err := view.Register([]*view.View{
		// 	ochttp.ClientSentBytesDistribution,
		// 	ochttp.ClientReceivedBytesDistribution,
		// 	ochttp.ClientRoundtripLatencyDistribution,
		// 	ochttp.ClientCompletedCount,
		// }...); err != nil {
		// 	log.WithError(err).Fatalf("Failed to register HTTP client views:")
		// }
		// view.RegisterExporter(pe)
		go func() {
			// mux := http.NewServeMux()
			// mux.Handle("/metrics", pe)
			http.Handle("/metrics", promhttp.Handler())
			// http://localhost:9995/metrics
			log.Infof("Starting Prometheus Exporter on port %d", prometheusPort)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", prometheusPort), nil); err != nil {
				log.WithError(err).Fatalf("Failed to run Prometheus /metrics endpoint:")
			}
		}()
		promInitialized = true
	} else {
		log.Debug("Prometheus exporter is already initialized")
	}
}

// InitS3 initialises the S3 session
// Also starts the Prometheus exporter on Port 8888
func InitS3(config common.S3Configuration) {
	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.

	var tr *http.Transport
	if config.ProxyHost != "" {
		proxyUrl, err := url.Parse(config.ProxyHost)
		if err != nil {
			log.WithError(err).Fatalf("Unable to configure proxy:")
		}
		tr = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: config.SkipSSLVerify},
			Proxy:           http.ProxyURL(proxyUrl),
			DialContext: (&net.Dialer{
				KeepAlive: 15 * time.Second,
				DualStack: true,
				Timeout:   5 * time.Second,
			}).DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		}
	} else {
		tr = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: config.SkipSSLVerify},
			DialContext: (&net.Dialer{
				KeepAlive: 15 * time.Second,
				DualStack: true,
				Timeout:   5 * time.Second,
			}).DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		}
	}
	tr2 := &GBTransport{Base: tr}
	hc = &http.Client{
		Transport: tr2,
	}

	sess := session.Must(session.NewSession(&aws.Config{
		HTTPClient: hc,
		// TODO Also set the remaining S3 connection details...
		Region:                            &config.Region,
		Credentials:                       credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		Endpoint:                          &config.Endpoint,
		S3ForcePathStyle:                  aws.Bool(true),
		S3DisableContentMD5Validation:     aws.Bool(true),
		DisableComputeChecksums:           aws.Bool(true),
		S3Disable100Continue:              aws.Bool(true),
		EC2MetadataDisableTimeoutOverride: aws.Bool(true),
	}))
	// Use this Session to do things that are hidden from the performance monitoring
	housekeepingSess := session.Must(session.NewSession(&aws.Config{
		HTTPClient: &http.Client{Transport: tr},
		// TODO Also set the remaining S3 connection details...
		Region:                            &config.Region,
		Credentials:                       credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		Endpoint:                          &config.Endpoint,
		S3ForcePathStyle:                  aws.Bool(true),
		S3DisableContentMD5Validation:     aws.Bool(true),
		DisableComputeChecksums:           aws.Bool(true),
		S3Disable100Continue:              aws.Bool(true),
		EC2MetadataDisableTimeoutOverride: aws.Bool(true),
	}))

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	svc = s3.New(sess)
	// Use this service to do things that are hidden from the performance monitoring
	HousekeepingSvc = s3.New(housekeepingSess)

	// TODO Create a context with a timeout - we already use this context in all S3 calls
	// Usually this shouldn't be a problem ;)
	ctx = context.Background()
	log.Debug("S3 Init done")
}

func putObject(service *s3.S3, objectName string, objectContent io.ReadSeeker, bucket string, objectSize int64) error {

	_, err := service.PutObject(&s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &objectName,
		Body:          objectContent,
		ContentLength: &objectSize,
	})

	return err
}

func putObjectMPU(service *s3.S3, objectName string, objectContent io.ReadSeeker, bucket string, partSize uint64, concurrency int) error {
	// Create an uploader with S3 client and custom options
	uploader := s3manager.NewUploaderWithClient(service)

	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &objectName,
		Body:   objectContent,
	}, func(d *s3manager.Uploader) {
		d.PartSize = int64(partSize)
		d.Concurrency = concurrency
	})

	return err
}

func ListObjects(service *s3.S3, prefix string, bucket string) (*s3.ListObjectsOutput, error) {
	result, err := service.ListObjects(&s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	return result, err
}

func getObjectMPU(service *s3.S3, objectName string, bucket string, partSize uint64, concurrency int) error {
	// Create a downloader with the session and custom options
	downloader := s3manager.NewDownloaderWithClient(service)
	_, err := downloader.DownloadWithContext(ctx, nullWriter{}, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
	}, func(d *s3manager.Downloader) {
		d.PartSize = int64(partSize)
		d.Concurrency = concurrency
	})
	return err
}

func getObject(service *s3.S3, objectName string, bucket string) error {

	obj, err := service.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
	})

	if err == nil {
		_, err = io.Copy(nullWriter{}, obj.Body)
	}

	return err

}

func deleteObject(service *s3.S3, objectName string, bucket string) error {
	// _, err := service.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
	// 	Bucket: &bucket,
	// 	Delete: &s3.Delete{
	// 		Objects: []*s3.ObjectIdentifier{{Key: &objectName}},
	// 	},
	// })

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
	})

	return err
}

func CreateBucket(service *s3.S3, bucket string) error {
	// TODO do not err when the bucket is already there...
	_, err := service.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		aerr, _ := err.(awserr.Error)
		// Ignore error if bucket already exists
		if aerr.Code() == s3.ErrCodeBucketAlreadyExists {
			return nil
		}
		log.WithField("Message", aerr.Message()).WithField("Code", aerr.Code()).Info("Issues when creating bucket")
	}
	return err
}

func DeleteBucket(service *s3.S3, bucket string) error {
	// First delete all objects in the bucket
	iter := s3manager.NewDeleteListIterator(service, &s3.ListObjectsInput{
		Bucket: &bucket,
	})

	if err := s3manager.NewBatchDeleteWithClient(service).Delete(aws.BackgroundContext(), iter); err != nil {
		return err
	}
	// Then delete the (now empty) bucket itself
	_, err := service.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	return err
}

func GetExistingReadObjectList(service *s3.S3, prefix string, bucket string) ([]*s3.Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	var bucketContents []*s3.Object
	isTruncated := true
	for isTruncated {
		result, err := svc.ListObjectsV2(input)
		if err != nil {
			return nil, err
		}
		bucketContents = append(bucketContents, result.Contents...)
		input.ContinuationToken = result.NextContinuationToken
		isTruncated = *result.IsTruncated
	}
	return bucketContents, nil
}

type nullWriter struct{}

func (nullWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (nullWriter) WriteAt(p []byte, off int64) (int, error) {
	return len(p), nil
}
