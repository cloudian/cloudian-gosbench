package driver

import (
	"fmt"
	"os"
	"strings"

	"github.com/cloudian/cloudian-gosbench/internal/gosbench/common"

	"contrib.go.opencensus.io/exporter/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
	promModel "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog/log"
)

var pe *prometheus.Exporter
var promRegistry = prom.NewRegistry()
var PromTestStart = prom.NewGaugeVec(
	prom.GaugeOpts{
		Name:      "test_start",
		Namespace: "gosbench",
		Help:      "Determines the start time of a job for Grafana annotations",
	}, []string{"testName"})
var PromTestEnd = prom.NewGaugeVec(
	prom.GaugeOpts{
		Name:      "test_end",
		Namespace: "gosbench",
		Help:      "Determines the end time of a job for Grafana annotations",
	}, []string{"testName"})
var promFinishedOps = prom.NewCounterVec(
	prom.CounterOpts{
		Name:      "finished_ops",
		Namespace: "gosbench",
		Help:      "Finished S3 operations",
	}, []string{"testName", "method"})
var promFailedOps = prom.NewCounterVec(
	prom.CounterOpts{
		Name:      "failed_ops",
		Namespace: "gosbench",
		Help:      "Failed S3 operations",
	}, []string{"testName", "method"})
var promRTLatency = prom.NewHistogramVec(
	prom.HistogramOpts{
		Name:      "rt_ops_latency",
		Namespace: "gosbench",
		Help:      "Histogram latency RT of S3 operations",
		Buckets:   prom.ExponentialBuckets(2, 2, 12),
	}, []string{"testName", "method"})
var promTTFBLatency = prom.NewHistogramVec(
	prom.HistogramOpts{
		Name:      "ttfb_ops_latency",
		Namespace: "gosbench",
		Help:      "Histogram latency of TTFB S3 operations",
		Buckets:   prom.ExponentialBuckets(2, 2, 12),
	}, []string{"testName", "method"})
var promUploadedBytes = prom.NewCounterVec(
	prom.CounterOpts{
		Name:      "uploaded_bytes",
		Namespace: "gosbench",
		Help:      "Uploaded bytes to S3 store",
	}, []string{"testName", "method"})
var promDownloadedBytes = prom.NewCounterVec(
	prom.CounterOpts{
		Name:      "downloaded_bytes",
		Namespace: "gosbench",
		Help:      "Downloaded bytes from S3 store",
	}, []string{"testName", "method"})

func init() {
	// Then create the prometheus stat exporter
	var err error
	pe, err = prometheus.NewExporter(prometheus.Options{
		Namespace: "gosbench",
		ConstLabels: map[string]string{
			"version": "0.0.1",
		},
		Registry: promRegistry,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create the Prometheus exporter")
	}

	if err = promRegistry.Register(PromTestStart); err != nil {
		log.Error().Err(err).Msg("Issues when adding test_start gauge to Prometheus registry")
	}
	if err = promRegistry.Register(PromTestEnd); err != nil {
		log.Error().Err(err).Msg("Issues when adding test_end gauge to Prometheus registry")
	}
	if err = promRegistry.Register(promFinishedOps); err != nil {
		log.Error().Err(err).Msg("Issues when adding finished_ops gauge to Prometheus registry")
	}
	if err = promRegistry.Register(promFailedOps); err != nil {
		log.Error().Err(err).Msg("Issues when adding failed_ops gauge to Prometheus registry")
	}
	if err = promRegistry.Register(promRTLatency); err != nil {
		log.Error().Err(err).Msg("Issues when adding rt_ops_latency gauge to Prometheus registry")
	}
	if err = promRegistry.Register(promTTFBLatency); err != nil {
		log.Error().Err(err).Msg("Issues when adding ttfb_ops_latency gauge to Prometheus registry")
	}
	if err = promRegistry.Register(promUploadedBytes); err != nil {
		log.Error().Err(err).Msg("Issues when adding uploaded_bytes gauge to Prometheus registry")
	}
	if err = promRegistry.Register(promDownloadedBytes); err != nil {
		log.Error().Err(err).Msg("Issues when adding downloaded_bytes gauge to Prometheus registry")
	}
}

func GetCurrentPromValues(testConfig *common.TestCaseConfiguration) common.BenchmarkResult {
	testName := testConfig.Name
	host, err := os.Hostname()
	if err != nil {
		host = "Unknown-Err"
	}
	benchResult := common.BenchmarkResult{
		Host:     host,
		TestName: testName,
		OperationName: getOperationName(testConfig.ReadWeight, testConfig.ExistingReadWeight,
			testConfig.WriteWeight, testConfig.ListWeight, testConfig.ExistingListWeight, testConfig.DeleteWeight, testConfig.ExistingDeleteWeight),
		Workers: testConfig.Workers,
		Options: getTestOptionString(testConfig),
	}
	result, err := promRegistry.Gather()
	if err != nil {
		log.Error().Err(err).Msg("ERROR during PROM VALUE gathering")
	}
	resultmap := map[string][]*promModel.Metric{}
	for _, metric := range result {
		resultmap[*metric.Name] = metric.Metric
	}
	benchResult.Operations = sumCounterForTest(resultmap["gosbench_finished_ops"], testName)
	benchResult.FailedOperations = sumCounterForTest(resultmap["gosbench_failed_ops"], testName)
	benchResult.SuccessRatio = benchResult.Operations / (benchResult.Operations + benchResult.FailedOperations)
	benchResult.Bytes = sumCounterForTest(resultmap["gosbench_uploaded_bytes"], testName) + sumCounterForTest(resultmap["gosbench_downloaded_bytes"], testName)
	benchResult.ObjectSize = benchResult.Bytes / (benchResult.Operations + benchResult.FailedOperations)
	benchResult.RTLatencyAvg = averageHistogramForTest(resultmap["gosbench_rt_ops_latency"], testName)
	benchResult.TTFBLatencyAvg = averageHistogramForTest(resultmap["gosbench_ttfb_ops_latency"], testName)
	return benchResult
}

func sumCounterForTest(metrics []*promModel.Metric, testName string) float64 {
	sum := float64(0)
	for _, metric := range metrics {
		for _, label := range metric.Label {
			if *label.Name == "testName" && *label.Value == testName {
				sum += *metric.Counter.Value
			}
		}
	}
	return sum
}

func averageHistogramForTest(metrics []*promModel.Metric, testName string) float64 {
	sum := float64(0)
	count := float64(0)
	for _, metric := range metrics {
		for _, label := range metric.Label {
			if *label.Name == "testName" && *label.Value == testName {
				sum += *metric.Histogram.SampleSum
				count += float64(*metric.Histogram.SampleCount)
			}
		}
	}
	return sum / count
}

func getOperationName(readWt int, existingReadWt int, writeWt int, listWt int, existingListWt int, deleteWt int, existingDeleteWt int) string {
	kvList := make([]KV, 0)
	if readWt > 0 {
		kvList = append(kvList, KV{"read", float64(readWt)})
	}
	if existingReadWt > 0 {
		kvList = append(kvList, KV{"existingRead", float64(existingReadWt)})
	}
	if writeWt > 0 {
		kvList = append(kvList, KV{"write", float64(writeWt)})
	}
	if listWt > 0 {
		kvList = append(kvList, KV{"list", float64(listWt)})
	}
	if existingListWt > 0 {
		kvList = append(kvList, KV{"existingList", float64(existingListWt)})
	}
	if deleteWt > 0 {
		kvList = append(kvList, KV{"delete", float64(deleteWt)})
	}
	if existingDeleteWt > 0 {
		kvList = append(kvList, KV{"existingDelete", float64(existingDeleteWt)})
	}

	if len(kvList) == 0 {
		return "Unknown"
	}

	if len(kvList) == 1 {
		return kvList[0].Key
	}

	var op strings.Builder
	for _, kv := range kvList {
		fmt.Fprintf(&op, "%s(%d)-", kv.Key, int(kv.Value))
	}
	return strings.TrimRight(op.String(), "-")
}

func getTestOptionString(testConfig *common.TestCaseConfiguration) string {
	var options strings.Builder
	fmt.Fprintf(&options, "object_size_min=%d~", testConfig.Objects.SizeMin)
	fmt.Fprintf(&options, "object_size_max=%d~", testConfig.Objects.SizeMax)
	fmt.Fprintf(&options, "object_size_distribution=%s~", testConfig.Objects.SizeDistribution)
	fmt.Fprintf(&options, "multipart_write_enabled=%t~", testConfig.Multipart.WriteMPUEnabled)
	fmt.Fprintf(&options, "multipart_write_part_size=%d~", testConfig.Multipart.WritePartSize)
	fmt.Fprintf(&options, "multipart_write_unit=%s~", testConfig.Multipart.WriteUnit)
	fmt.Fprintf(&options, "multipart_read_enabled=%t~", testConfig.Multipart.ReadMPUEnabled)
	fmt.Fprintf(&options, "multipart_read_part_size=%d~", testConfig.Multipart.ReadPartSize)
	fmt.Fprintf(&options, "multipart_read_unit=%s~", testConfig.Multipart.ReadUnit)
	return strings.TrimRight(options.String(), "~")
}
