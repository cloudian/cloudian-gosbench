package driver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/cloudian/cloudian-gosbench/internal/gosbench/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/rs/zerolog/log"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
)

var svc, HousekeepingSvc *s3.Client
var ctx context.Context
var promInitialized bool

func InitPrometheus(prometheusPort int) {
	if !promInitialized {
		if err := view.Register([]*view.View{
			ochttp.ClientSentBytesDistribution,
			ochttp.ClientReceivedBytesDistribution,
			ochttp.ClientRoundtripLatencyDistribution,
			ochttp.ClientCompletedCount,
		}...); err != nil {
			log.Fatal().Err(err).Msg("Failed to register HTTP client views")
		}
		view.RegisterExporter(pe)
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", pe)
			//http.Handle("/metrics", promhttp.Handler())
			// http://localhost:9295/metrics
			log.Info().Msgf("Starting Prometheus Exporter on port %d", prometheusPort)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", prometheusPort), mux); err != nil {
				log.Fatal().Err(err).Msgf("Failed to run Prometheus /metrics endpoint: %s", fmt.Sprintf(":%d", prometheusPort))
			}
		}()
		promInitialized = true
	} else {
		log.Debug().Msg("Prometheus exporter is already initialized")
	}
}

// InitS3 initialises the S3 session
// Also starts the Prometheus exporter on Port 8888
func InitS3(s3Config common.S3Configuration) {
	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: s3Config.SkipSSLVerify},
		DialContext: (&net.Dialer{
			KeepAlive: 15 * time.Second,
			DualStack: true,
			Timeout:   5 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
	}

	if s3Config.ProxyHost != "" {
		proxyUrl, err := url.Parse(s3Config.ProxyHost)
		if err != nil {
			log.Fatal().Err(err).Msgf("Unable to configure proxy: %s", s3Config.ProxyHost)
		}
		tr.Proxy = http.ProxyURL(proxyUrl)
	}

	//Setting up the S3 client for the worker items
	workerHTTPClient := &http.Client{
		Transport: &GBTransport{Base: tr},
	}

	workerCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithHTTPClient(workerHTTPClient),
		config.WithRegion(s3Config.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s3Config.AccessKey, s3Config.SecretKey, "")),
	)

	if err != nil {
		log.Fatal().Err(err).Msg("error loading AWS config")
	}

	svc = s3.NewFromConfig(workerCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Config.Endpoint)
	})

	// Setting up the housekeeping S3 client
	housekeepingHTTPClient := &http.Client{
		Transport: &HKTransport{Base: tr},
	}
	hkCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithHTTPClient(housekeepingHTTPClient),
		config.WithRegion(s3Config.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s3Config.AccessKey, s3Config.SecretKey, "")),
	)

	if err != nil {
		log.Fatal().Err(err).Msg("error loading AWS config")
	}
	HousekeepingSvc = s3.NewFromConfig(hkCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Config.Endpoint)
	})

	// TODO Create a context with a timeout - we already use this context in all S3 calls
	// Usually this shouldn't be a problem ;)
	ctx = context.Background()
	log.Debug().Msg("S3 Init done")
}

func putObject(service *s3.Client, objectName string, objectContent io.ReadSeeker, bucket string, objectSize int64) error {
	_, err := service.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &objectName,
		Body:          objectContent,
		ContentLength: &objectSize,
	})

	return err
}

func putObjectMPU(uploader *manager.Uploader, objectName string, objectContent io.ReadSeeker, bucket string) error {
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
		Body:   objectContent,
	})

	return err
}

func ListObjects(service *s3.Client, prefix string, bucket string) ([]types.Object, error) {
	var bucketContents []types.Object
	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: &prefix,
	}

	// Use a loop to handle pagination
	for {
		result, err := service.ListObjects(ctx, input)
		if err != nil {
			return nil, err
		}

		bucketContents = append(bucketContents, result.Contents...)

		// Check if there are more objects to fetch
		if result.IsTruncated != nil && *result.IsTruncated {
			input.Marker = result.NextMarker
		} else {
			break
		}
	}
	return bucketContents, nil
}

func ListObjectsV2(service *s3.Client, prefix string, bucket string) ([]types.Object, error) {
	var bucketContents []types.Object
	p := s3.NewListObjectsV2Paginator(service, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		// Next Page takes a new context for each page retrieval. This is where
		// you could add timeouts or deadlines.
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		bucketContents = append(bucketContents, page.Contents...)
	}
	return bucketContents, nil
}

func getObjectMPU(downloader manager.Downloader, objectName string, bucket string) error {
	// Create a downloader with the session and custom options
	_, err := downloader.Download(ctx, DiscardWriterAt{}, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
	})
	return err
}

func getObject(service *s3.Client, objectName string, bucket string) error {

	obj, err := service.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
	})

	if err == nil {
		_, err = io.Copy(DiscardWriterAt{}, obj.Body)
	}

	defer obj.Body.Close()

	return err

}

func deleteObject(service *s3.Client, objectName string, bucket string) error {
	_, err := service.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &objectName,
	})

	return err
}

func CreateBucket(service *s3.Client, bucket string) error {
	// Do not err when the bucket is already there...
	_, err := service.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		var bne *types.BucketAlreadyExists
		// Ignore error if bucket already exists
		if errors.As(err, &bne) {
			return nil
		}
		log.Info().Err(err).Msg("Issues when creating bucket")
	}
	return err
}

func DeleteBucket(service *s3.Client, bucket string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	var bucketContents []types.Object
	isTruncated := true
	for isTruncated {
		result, err := service.ListObjectsV2(ctx, input)
		if err != nil {
			return err
		}
		bucketContents = append(bucketContents, result.Contents...)
		input.ContinuationToken = result.NextContinuationToken
		isTruncated = *result.IsTruncated
	}

	if len(bucketContents) > 0 {
		var objectsToDelete []types.ObjectIdentifier
		for _, item := range bucketContents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key: item.Key,
			})
		}

		deleteObjectsInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		}

		_, err := svc.DeleteObjects(ctx, deleteObjectsInput)
		if err != nil {
			return err
		}
	}

	// Then delete the (now empty) bucket itself
	_, err := service.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	return err
}

type DiscardWriterAt struct{}

func (DiscardWriterAt) Write(p []byte) (int, error) {
	return len(p), nil
}

func (DiscardWriterAt) WriteAt(p []byte, off int64) (int, error) {
	return len(p), nil
}
