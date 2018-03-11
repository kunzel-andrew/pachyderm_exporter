package exporter

import (
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	stateSuccess = "success"
	stateFailure = "failure"
	stateKilled  = "killed"

	datumStateProcessed = "processed"
	datumStateFailed    = "failed"
	datumStateSkipped   = "skipped"
)

type Exporter struct {
	pachClient        PachydermClient
	queryTimeout      time.Duration
	mutex             sync.Mutex
	runningJobs       map[string]*pps.JobInfo
	startingJobs      map[string]*pps.JobInfo
	lastTailJobID     string
	lastSuccessfulJob map[string]int64
	m                 *metrics
}

type PachydermClient interface {
	ListJobStream(ctx context.Context, in *pps.ListJobRequest, opts ...grpc.CallOption) (pps.API_ListJobStreamClient, error)
	ListPipeline() ([]*pps.PipelineInfo, error)
	WithCtx(ctx context.Context) PachydermClient
}

type metrics struct {
	scrapes           prometheus.Counter
	up                prometheus.Gauge
	pipelines         *prometheus.GaugeVec
	jobsCompleted     *prometheus.CounterVec
	jobsFailed        *prometheus.CounterVec
	jobsRunning       *prometheus.GaugeVec
	jobsStarting      *prometheus.GaugeVec
	datums            *prometheus.CounterVec
	lastSuccessfulJob *prometheus.GaugeVec
	uploaded          *prometheus.CounterVec
	downloaded        *prometheus.CounterVec
	uploadTime        *prometheus.CounterVec
	downloadTime      *prometheus.CounterVec
	processTime       *prometheus.CounterVec
}

func New(c PachydermClient, queryTimeout time.Duration) *Exporter {
	return &Exporter{
		pachClient:   c,
		queryTimeout: queryTimeout,
		m: &metrics{
			up: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "pachyderm_up",
				Help: "Was the last pachyderm scrape successful",
			}),
			scrapes: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "pachyderm_exporter_scrapes_total",
				Help: "Total pachyderm scrapes",
			}),
			pipelines: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: "pachyderm_pipeline_states",
				Help: "State of each pipeline. Set to 1 if pipeline is in the given state.",
			}, []string{"state", "pipeline"}),
			jobsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_jobs_completed_total",
				Help: "Total number of jobs that pachyderm has completed, by state and pipeline",
			}, []string{"state", "pipeline"}),
			jobsRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: "pachyderm_jobs_running",
				Help: "Number of pachyderm jobs in the RUNNING state",
			}, []string{"pipeline"}),
			jobsStarting: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: "pachyderm_jobs_starting",
				Help: "Number of pachyderm jobs in the STARTING state",
			}, []string{"pipeline"}),
			lastSuccessfulJob: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: "pachyderm_last_successful_job",
				Help: "When the last successful job ran, as a Unix timestamp in seconds",
			}, []string{"pipeline"}),
			datums: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_datums_total",
				Help: "Total number of datums that pachyderm has processed",
			}, []string{"state", "pipeline"}),
			uploaded: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_uploaded_bytes_total",
				Help: "Total amount of bytes uploaded across all runs of the pipeline",
			}, []string{"pipeline"}),
			downloaded: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_downloaded_bytes_total",
				Help: "Total amount of bytes downloaded across all runs of the pipeline",
			}, []string{"pipeline"}),
			uploadTime: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_upload_time_seconds_total",
				Help: "Total amount of time spent in uploading across all runs of the pipeline",
			}, []string{"pipeline"}),
			downloadTime: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_download_time_seconds_total",
				Help: "Total amount of time spent in uploading across all runs of the pipeline",
			}, []string{"pipeline"}),
			processTime: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_process_time_seconds_total",
				Help: "Total amount of time spent in processing across all runs of the pipeline",
			}, []string{"pipeline"}),
		},
		runningJobs:       make(map[string]*pps.JobInfo),
		startingJobs:      make(map[string]*pps.JobInfo),
		lastSuccessfulJob: make(map[string]int64),
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.m.up.Describe(ch)
	e.m.scrapes.Describe(ch)
	e.m.pipelines.Describe(ch)
	e.m.jobsCompleted.Describe(ch)
	e.m.jobsRunning.Describe(ch)
	e.m.jobsStarting.Describe(ch)
	e.m.datums.Describe(ch)
	e.m.lastSuccessfulJob.Describe(ch)
	e.m.downloaded.Describe(ch)
	e.m.uploaded.Describe(ch)
	e.m.downloadTime.Describe(ch)
	e.m.uploadTime.Describe(ch)
	e.m.processTime.Describe(ch)
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if err := e.scrape(); err != nil {
		e.m.up.Set(0)
		log.Println("scrape failed: ", err)
	} else {
		e.m.up.Set(1)
	}

	e.m.up.Collect(ch)
	e.m.scrapes.Collect(ch)
	e.m.pipelines.Collect(ch)
	e.m.jobsCompleted.Collect(ch)
	e.m.datums.Collect(ch)
	e.m.downloaded.Collect(ch)
	e.m.uploaded.Collect(ch)
	e.m.downloadTime.Collect(ch)
	e.m.uploadTime.Collect(ch)
	e.m.processTime.Collect(ch)
	e.collectRunningJobs(ch)
	e.collectStartingJobs(ch)
	e.collectLastSuccess(ch)
}

func (e *Exporter) scrape() error {
	e.m.scrapes.Inc()
	if err := e.scrapePipelines(); err != nil {
		return err
	}
	if err := e.scrapeJobs(); err != nil {
		return err
	}
	return nil
}

func (e *Exporter) scrapePipelines() error {
	ctx, cancel := context.WithTimeout(context.Background(), e.queryTimeout)
	defer cancel()
	pipelines, err := e.pachClient.WithCtx(ctx).ListPipeline()
	if err != nil {
		return fmt.Errorf("couldn't list pipelines: %s", err.Error())
	}
	gauge := e.m.pipelines
	gauge.Reset()
	for _, pipeline := range pipelines {
		name := pipeline.Pipeline.Name
		state := strings.ToLower(strings.TrimPrefix(pipeline.State.String(), "PIPELINE_"))
		gauge.WithLabelValues(state, name).Set(1)
	}
	return nil
}

func (e *Exporter) scrapeJobs() error {
	ctx, cancel := context.WithTimeout(context.Background(), e.queryTimeout)
	defer cancel()
	stream, err := e.pachClient.ListJobStream(ctx, &pps.ListJobRequest{})
	if err != nil {
		return fmt.Errorf("couldn't list jobs: %s", err.Error())
	}
	defer stream.CloseSend()

	reachedLastTail := false
	tailJobID := ""
	defer func() {
		if tailJobID != "" {
			e.lastTailJobID = tailJobID
		}
	}()

	notSeen := mergeJobMap(e.startingJobs, e.runningJobs)

	// Loop terminates if:
	// - We've found the previous tail and we aren't looking for any more previously STARTING/RUNNING jobs
	// OR
	// - The server stream ended
	for !(reachedLastTail && len(notSeen) == 0) {
		job, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("error reading stream: %s", err.Error())
			}
			break
		}

		jobID := job.Job.ID
		state := job.State

		// Save the latest job ID as the tail pointer
		if tailJobID == "" {
			tailJobID = jobID
		}
		// Track if we reached the previous tail
		if !reachedLastTail && jobID == e.lastTailJobID {
			reachedLastTail = true
		}
		// Track jobs that we still need to see in the logs
		if _, ok := notSeen[jobID]; ok {
			delete(notSeen, jobID)
		}

		if prev, ok := e.runningJobs[jobID]; ok {
			// Update state and datum count of a job that was previously RUNNING
			e.trackJobDiff(prev, job)
			if state != pps.JobState_JOB_RUNNING {
				delete(e.runningJobs, jobID)
			}
			if isCompletedState(state) {
				e.trackCompletion(job)
			}
		} else if _, ok := e.startingJobs[jobID]; ok {
			// Update state and datum count of a job that was previously STARTING
			e.trackJobDiff(nil, job)
			if state == pps.JobState_JOB_RUNNING {
				delete(e.startingJobs, jobID)
				e.runningJobs[jobID] = job
			} else if isCompletedState(state) {
				delete(e.startingJobs, jobID)
				e.trackCompletion(job)
			}
		} else if !reachedLastTail {
			// Update state and datum count for jobs that we haven't seen before
			e.trackJobDiff(nil, job)
			switch job.State {
			case pps.JobState_JOB_SUCCESS, pps.JobState_JOB_FAILURE, pps.JobState_JOB_KILLED:
				e.trackCompletion(job)
			case pps.JobState_JOB_RUNNING:
				if _, ok := e.runningJobs[jobID]; !ok {
					e.runningJobs[jobID] = job
				}
			case pps.JobState_JOB_STARTING:
				if _, ok := e.startingJobs[jobID]; !ok {
					e.startingJobs[jobID] = job
				}
			}
		}
	}
	return nil
}

func isCompletedState(state pps.JobState) bool {
	return state == pps.JobState_JOB_SUCCESS || state == pps.JobState_JOB_FAILURE || state == pps.JobState_JOB_KILLED
}

func (e *Exporter) trackCompletion(job *pps.JobInfo) {
	state := job.State
	pipeline := job.Pipeline.Name
	switch state {
	case pps.JobState_JOB_SUCCESS:
		e.m.jobsCompleted.WithLabelValues(stateSuccess, pipeline).Inc()
		// Track latest successful run of a pipeline as seconds since the Unix epoch
		if last, _ := e.lastSuccessfulJob[pipeline]; last < job.Finished.GetSeconds() {
			e.lastSuccessfulJob[pipeline] = job.Finished.GetSeconds()
		}
	case pps.JobState_JOB_FAILURE:
		e.m.jobsCompleted.WithLabelValues(stateFailure, pipeline).Inc()
	case pps.JobState_JOB_KILLED:
		e.m.jobsCompleted.WithLabelValues(stateKilled, pipeline).Inc()
	}
}

func (e *Exporter) trackJobDiff(prev *pps.JobInfo, curr *pps.JobInfo) {
	datums := e.m.datums
	pipeline := curr.Pipeline.Name
	currStats := curr.GetStats()
	if prev == nil {
		incCounter(datums.WithLabelValues(datumStateProcessed, pipeline), curr.DataProcessed)
		incCounter(datums.WithLabelValues(datumStateSkipped, pipeline), curr.DataSkipped)
		incCounter(datums.WithLabelValues(datumStateFailed, pipeline), curr.DataFailed)
		incCounter(e.m.downloaded.WithLabelValues(pipeline), int64(currStats.DownloadBytes))
		incCounter(e.m.uploaded.WithLabelValues(pipeline), int64(currStats.UploadBytes))
		incCounter(e.m.downloadTime.WithLabelValues(pipeline), currStats.DownloadTime.GetSeconds())
		incCounter(e.m.uploadTime.WithLabelValues(pipeline), currStats.UploadTime.GetSeconds())
		incCounter(e.m.processTime.WithLabelValues(pipeline), currStats.ProcessTime.GetSeconds())
	} else {
		prevStats := prev.GetStats()
		incCounter(datums.WithLabelValues(datumStateProcessed, pipeline), curr.DataProcessed-prev.DataProcessed)
		incCounter(datums.WithLabelValues(datumStateSkipped, pipeline), curr.DataSkipped-prev.DataSkipped)
		incCounter(datums.WithLabelValues(datumStateFailed, pipeline), curr.DataFailed-prev.DataFailed)
		incCounter(e.m.downloaded.WithLabelValues(pipeline), int64(currStats.DownloadBytes-prevStats.DownloadBytes))
		incCounter(e.m.uploaded.WithLabelValues(pipeline), int64(currStats.UploadBytes-prevStats.DownloadBytes))
		incCounter(e.m.downloadTime.WithLabelValues(pipeline), currStats.DownloadTime.GetSeconds()-prevStats.DownloadTime.GetSeconds())
		incCounter(e.m.uploadTime.WithLabelValues(pipeline), currStats.UploadTime.GetSeconds()-prevStats.UploadTime.GetSeconds())
		incCounter(e.m.processTime.WithLabelValues(pipeline), currStats.ProcessTime.GetSeconds()-prevStats.ProcessTime.GetSeconds())
	}
}

func incCounter(c prometheus.Counter, v int64) {
	if v <= 0 {
		// Pachyderm can produce negative counts for data processed... Don't know why
		return
	}
	c.Add(float64(v))
}

func (e *Exporter) collectRunningJobs(ch chan<- prometheus.Metric) {
	e.m.jobsRunning.Reset()

	jobs := byPipeline(e.runningJobs)
	for pipeline, count := range jobs {
		e.m.jobsRunning.WithLabelValues(pipeline).Set(float64(count))
	}
	e.m.jobsRunning.Collect(ch)
}

func (e *Exporter) collectStartingJobs(ch chan<- prometheus.Metric) {
	e.m.jobsStarting.Reset()

	jobs := byPipeline(e.startingJobs)
	for pipeline, count := range jobs {
		e.m.jobsStarting.WithLabelValues(pipeline).Set(float64(count))
	}
	e.m.jobsStarting.Collect(ch)
}

func (e *Exporter) collectLastSuccess(ch chan<- prometheus.Metric) {
	e.m.lastSuccessfulJob.Reset()

	for pipeline, timestamp := range e.lastSuccessfulJob {
		e.m.lastSuccessfulJob.WithLabelValues(pipeline).Set(float64(timestamp))
	}
	e.m.lastSuccessfulJob.Collect(ch)
}

func byPipeline(byJobID map[string]*pps.JobInfo) map[string]int {
	out := make(map[string]int)
	for _, job := range byJobID {
		pipeline := job.Pipeline.Name
		if _, ok := out[pipeline]; !ok {
			out[pipeline] = 1
		} else {
			out[pipeline]++
		}
	}
	return out
}

func mergeJobMap(a, b map[string]*pps.JobInfo) map[string]*pps.JobInfo {
	dst := make(map[string]*pps.JobInfo, len(a)+len(b))
	for k, v := range a {
		dst[k] = v
	}
	for k, v := range b {
		dst[k] = v
	}
	return dst
}

// PachydermClientWrapper modifies the signature of APIClient.WithCtx so that it can be used in the PachydermClient
// interface.
type PachydermClientWrapper struct {
	*client.APIClient
}

func (w *PachydermClientWrapper) WithCtx(ctx context.Context) PachydermClient {
	return &PachydermClientWrapper{w.APIClient.WithCtx(ctx)}
}
