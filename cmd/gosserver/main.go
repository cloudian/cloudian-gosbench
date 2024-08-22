package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cloudian/cloudian-gosbench/internal/gosbench/common"

	"gopkg.in/yaml.v2"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})

	flag.StringVar(&configFileLocation, "c", "", "Config file describing test run")
	flag.StringVar(&s3FileLocation, "s", "", "S3 configuration information")
	flag.StringVar(&testResultsOutputDir, "r", "", "Directory to write the results file")
	flag.IntVar(&serverPort, "p", 2000, "Port on which the server will be available for clients. Default: 2000")
	flag.BoolVar(&debug, "d", false, "enable debug log output")
	flag.BoolVar(&trace, "t", false, "enable trace log output")
	flag.Parse()
	// Only demand this flag if we are not running go test
	if configFileLocation == "" && flag.Lookup("test.v") == nil {
		log.Fatal().Msg("-c is a mandatory parameter - please specify the config file")
	}

	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else if trace {
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

var configFileLocation string
var s3FileLocation string
var testResultsOutputDir string
var serverPort int
var readyDrivers chan *net.Conn
var done bool = false
var debug, trace bool
var listener net.Listener

func loadS3ConfigFromFile(s3FileContent []byte) []*common.S3Configuration {
	var s3Config []*common.S3Configuration
	var err error

	if strings.HasSuffix(s3FileLocation, ".yaml") {
		err = yaml.Unmarshal(s3FileContent, &s3Config)
		if err != nil {
			log.Fatal().Err(err).Msgf("Error unmarshaling yaml s3 config file: %s", s3FileLocation)
		}
	} else if strings.HasSuffix(s3FileLocation, ".json") {
		err = json.Unmarshal(s3FileContent, &s3Config)
		if err != nil {
			log.Fatal().Err(err).Msgf("Error unmarshalling json S3 config file: %s", s3FileLocation)
		}
	} else {
		log.Fatal().Err(err).Msg("S3 configuration file must be a yaml or json formatted file")
	}
	return s3Config
}

func loadConfigFromFile(configFileContent []byte) *common.Workloadconf {
	var workload common.Workloadconf
	var err error

	if strings.HasSuffix(configFileLocation, ".yaml") {
		err = yaml.Unmarshal(configFileContent, &workload)
		if err != nil {
			log.Fatal().Err(err).Msgf("Error unmarshaling yaml config file: %s", configFileLocation)
		}
	} else if strings.HasSuffix(configFileLocation, ".json") {
		err = json.Unmarshal(configFileContent, &workload)
		if err != nil {
			log.Fatal().Err(err).Msgf("Error unmarshaling json config file: %s", configFileLocation)
		}
	} else {
		log.Fatal().Msg("Configuration file must be a yaml or json formatted file")
	}
	return &workload
}

func main() {

	configFileContent, err := os.ReadFile(configFileLocation)
	if err != nil {
		log.Fatal().Err(err).Msgf("Error reading workload config file: %s", configFileLocation)
	}

	workload := loadConfigFromFile(configFileContent)

	s3FileContent, err := os.ReadFile(s3FileLocation)
	if err != nil {
		log.Fatal().Err(err).Msgf("Error reading S3config file: %s", s3FileLocation)
	}

	s3Config := loadS3ConfigFromFile(s3FileContent)

	config := common.Testconf{
		S3Config:      s3Config,
		GrafanaConfig: workload.GrafanaConfig,
		Tests:         workload.Tests,
	}

	common.CheckConfig(&config)

	readyDrivers = make(chan *net.Conn)
	defer close(readyDrivers)

	// Listen on TCP port 2000 on all available unicast and
	// anycast IP addresses of the local system.
	listener, err = net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		log.Fatal().Err(err).Msg("Could not open port!")
	}
	log.Info().Msg("Ready to accept connections")
	go scheduleTests(config)
	for {
		// Wait for a connection.
		conn, err := listener.Accept()
		if done {
			break
		}
		if err != nil {
			log.Fatal().Err(err).Msg("Issue when waiting for connection of clients")
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c *net.Conn) {
			log.Info().Msgf("%s connected to us ", (*c).RemoteAddr())
			decoder := json.NewDecoder(*c)
			var message string
			err := decoder.Decode(&message)
			if err != nil {
				log.Error().Str("message", message).Err(err).Msg("Could not decode message, closing connection")
				_ = (*c).Close()
				return
			}
			if message == "ready for work" {
				log.Debug().Msg("We have a new driver!")
				readyDrivers <- c
				return
			}
		}(&conn)

	}

	log.Info().Msg("Shutting down server")
}

func scheduleTests(config common.Testconf) {

	var maxDrivers int = 0

	for testNumber, test := range config.Tests {

		log.Debug().Msgf("Starting test %s", test.Name)

		doneChannel := make(chan bool, test.Drivers)
		resultChannel := make(chan common.BenchmarkResult, test.Drivers)
		continueDrivers := make(chan bool, test.Drivers)

		maxDrivers = int(math.Max(float64(test.Drivers), float64(maxDrivers)))

		for driver := 0; driver < test.Drivers; driver++ {
			driverConfig := &common.DriverConf{
				Test:     test,
				S3Config: config.S3Config[driver%len(config.S3Config)],
				DriverID: fmt.Sprintf("d%d", driver),
			}
			driverConnection := <-readyDrivers
			log.Info().Any("Driver", (*driverConnection).RemoteAddr()).Msgf("We found driver %d / %d for test %d", driver+1, test.Drivers, testNumber)
			go executeTestOnDriver(driverConnection, driverConfig, doneChannel, continueDrivers, resultChannel)
		}
		for driver := 0; driver < test.Drivers; driver++ {
			// Will halt until all driverss are done with preparations
			<-doneChannel
		}
		// Add sleep after prep phase so that drives can relax
		time.Sleep(5 * time.Second)
		log.Info().Str("test", test.Name).Msg("All drivers have finished preparations - starting performance test")
		startTime := time.Now().UTC()
		for driver := 0; driver < test.Drivers; driver++ {
			continueDrivers <- true
		}
		var benchResults []common.BenchmarkResult
		for driver := 0; driver < test.Drivers; driver++ {
			// Will halt until all drivers are done with their work
			<-doneChannel
			benchResults = append(benchResults, <-resultChannel)
		}
		stopTime := time.Now().UTC()
		log.Info().Str("test", test.Name).Msg("All drivers have finished the performance test - continuing with next test")
		log.Info().Str("test", test.Name).Msgf("GRAFANA: ?from=%d&to=%d", startTime.UnixNano()/int64(1000000), stopTime.UnixNano()/int64(1000000))
		benchResult := sumBenchmarkResults(benchResults)
		benchResult.StartTime = startTime
		benchResult.StopTime = stopTime
		log.Info().Str("test", test.Name).
			Str("Operation Name", benchResult.OperationName).
			Int("Drivers", benchResult.Workers).
			Float64("Object Size", benchResult.ObjectSize).
			Float64("Completed Operations", benchResult.Operations).
			Float64("Failed Operations", benchResult.FailedOperations).
			Float64("Ops Per Second", benchResult.OpsPerSecond).
			Float64("Total Bytes", benchResult.Bytes).
			Float64("Average BW in Byte/s", benchResult.Bandwidth).
			Float64("Average RT latency in ms", benchResult.RTLatencyAvg).
			Float64("Average TTFB latency in ms", benchResult.TTFBLatencyAvg).
			Float64("Success Ratio", benchResult.SuccessRatio).
			Time("Start Time", benchResult.StartTime).
			Time("Stop Time", benchResult.StopTime).
			Str("Test runtime on server", benchResult.Duration.String()).
			Msg("PERF RESULTS")
		writeResultToCSV(benchResult)
		writeResultToConsole(benchResults, benchResult)
		close(doneChannel)
		close(continueDrivers)
		close(resultChannel)
	}
	log.Info().Msg("All performance tests finished")
	for driver := 0; driver < maxDrivers; driver++ {
		driverConnection := <-readyDrivers
		shutdownDriver(driverConnection)
	}
	done = true
	listener.Close()
}

func executeTestOnDriver(conn *net.Conn, config *common.DriverConf, doneChannel chan bool, continueDrivers chan bool, resultChannel chan common.BenchmarkResult) {
	encoder := json.NewEncoder(*conn)
	decoder := json.NewDecoder(*conn)
	_ = encoder.Encode(common.DriverMessage{Message: "init", Config: config})

	var response common.DriverMessage
	for {
		err := decoder.Decode(&response)
		if err != nil {
			log.Error().Str("driver", config.DriverID).Any("message", response).Err(err).Msg("Driver responded unusually - dropping")
			_ = (*conn).Close()
			return
		}
		log.Trace().Msgf("Response: %+v", response)
		switch response.Message {
		case "preparations done":
			doneChannel <- true
			<-continueDrivers
			_ = encoder.Encode(common.DriverMessage{Message: "start work"})
		case "work done":
			doneChannel <- true
			resultChannel <- response.BenchResult
			_ = (*conn).Close()
			return
		}
	}
}

func shutdownDriver(conn *net.Conn) {
	encoder := json.NewEncoder(*conn)
	log.Info().Any("Driver", (*conn).RemoteAddr()).Msg("Shutting down driver")
	_ = encoder.Encode(common.DriverMessage{Message: "shutdown"})
}

func sumBenchmarkResults(results []common.BenchmarkResult) common.BenchmarkResult {
	sum := common.BenchmarkResult{}
	bandwidthAverages := float64(0)
	rtLatencyAverages := float64(0)
	ttfbLatencyAverages := float64(0)
	objectSizeAverages := float64(0)
	for _, result := range results {
		sum.Bytes += result.Bytes
		sum.Operations += result.Operations
		sum.FailedOperations += result.FailedOperations
		rtLatencyAverages += result.RTLatencyAvg
		ttfbLatencyAverages += result.TTFBLatencyAvg
		bandwidthAverages += result.Bandwidth
		objectSizeAverages += result.ObjectSize
		sum.OpsPerSecond += result.OpsPerSecond
		sum.Workers += result.Workers
		if result.Duration > sum.Duration {
			sum.Duration = result.Duration
		}
	}
	sum.SuccessRatio = sum.Operations / (sum.Operations + sum.FailedOperations)
	sum.RTLatencyAvg = rtLatencyAverages / float64(len(results))
	sum.TTFBLatencyAvg = ttfbLatencyAverages / float64(len(results))
	sum.ObjectSize = objectSizeAverages / float64(len(results))
	sum.TestName = results[0].TestName
	sum.OperationName = results[0].OperationName
	sum.Options = results[0].Options
	sum.Bandwidth = bandwidthAverages
	return sum
}

func writeResultToCSV(benchResult common.BenchmarkResult) {
	file, created, err := getCSVFileHandle()
	if err != nil {
		log.Error().Err(err).Msg("Could not get a file handle for the CSV results")
		return
	}
	defer file.Close()

	csvwriter := csv.NewWriter(file)

	if created {
		err = csvwriter.Write([]string{
			"TestName",
			"Operation Name",
			"Workers",
			"Object Size",
			"Completed Operations",
			"Failed Operations",
			"Ops/Second",
			"Total Bytes",
			"Bandwidth in Bytes/s",
			"Average RT Latency in ms",
			"Success Ratio",
			"Start Time",
			"Stop Time",
			"Test duration seen by server in seconds",
			"Test Options",
			"Average TTFB Latency in ms",
		})
		if err != nil {
			log.Error().Err(err).Msg("Failed writing line to results csv")
			return
		}
	}

	err = csvwriter.Write([]string{
		benchResult.TestName,
		benchResult.OperationName,
		fmt.Sprintf("%d", benchResult.Workers),
		fmt.Sprintf("%.0f", benchResult.ObjectSize),
		fmt.Sprintf("%.0f", benchResult.Operations),
		fmt.Sprintf("%.0f", benchResult.FailedOperations),
		fmt.Sprintf("%f", benchResult.OpsPerSecond),
		fmt.Sprintf("%.0f", benchResult.Bytes),
		fmt.Sprintf("%f", benchResult.Bandwidth),
		fmt.Sprintf("%f", benchResult.RTLatencyAvg),
		fmt.Sprintf("%.2f", benchResult.SuccessRatio),
		fmt.Sprintf("%d", benchResult.StartTime.Unix()),
		fmt.Sprintf("%d", benchResult.StopTime.Unix()),
		fmt.Sprintf("%f", benchResult.Duration.Seconds()),
		benchResult.Options,
		fmt.Sprintf("%f", benchResult.TTFBLatencyAvg), // Added in 1.2.0
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed writing line to results csv")
		return
	}

	csvwriter.Flush()

}

func getCSVFileHandle() (*os.File, bool, error) {
	if testResultsOutputDir != "" {
		pathInfo, err := os.Stat(testResultsOutputDir)
		if err != nil {
			log.Error().Msg("Unable to stat() results output directory, writing to current working directory")
			testResultsOutputDir = ""
		}
		if !pathInfo.IsDir() {
			log.Error().Msg("Results output loaction is not a directory, writing to current working directory")
			testResultsOutputDir = ""
		}
	}
	filePath := filepath.Join(testResultsOutputDir, "gosbench_results.csv")
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0755)
	if err == nil {
		return file, false, nil
	}

	file, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0755)
	if err == nil {
		return file, true, nil
	}

	return nil, false, fmt.Errorf("could not find previous CSV for appending and could not write new CSV file to location %s", filePath)

}

func writeResultToConsole(driverResult []common.BenchmarkResult, summedResults common.BenchmarkResult) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "HOST\tTEST NAME\tOP NAME\tWORKERS\tOBJECT SIZE\tCOMPLETED OPS\tFAILED OPS\tOPS PER SECOND\tTOTAL MB\tBANDWIDTH (MB)\tRT LATENCY\tTTFB LATENCY\tSUCCESS RATIO\tDURATION\t")
	for _, result := range driverResult {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%.0f\t%.0f\t%.0f\t%.2f ops/sec\t%.2f MB\t%.2f MB/s\t%.2f ms\t%.2f ms\t%.2f%%\t%.2f s\t\n",
			result.Host, result.TestName, result.OperationName, result.Workers, result.ObjectSize, result.Operations,
			result.FailedOperations, result.OpsPerSecond, result.Bytes/(1024*1024), result.Bandwidth/(1024*1024),
			result.RTLatencyAvg, result.TTFBLatencyAvg, result.SuccessRatio*100, result.Duration.Seconds())
	}
	fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%.0f\t%.0f\t%.0f\t%.2f ops/sec\t%.2f MB\t%.2f MB/s\t%.2f ms\t%.2f ms\t%.2f%%\t%.2f s\t\n",
		"Totals", summedResults.TestName, summedResults.OperationName, summedResults.Workers, summedResults.ObjectSize,
		summedResults.Operations, summedResults.FailedOperations, summedResults.OpsPerSecond, summedResults.Bytes/(1024*1024),
		summedResults.Bandwidth/(1024*1024), summedResults.RTLatencyAvg, summedResults.TTFBLatencyAvg, summedResults.SuccessRatio*100, summedResults.Duration.Seconds())

	w.Flush()
}
