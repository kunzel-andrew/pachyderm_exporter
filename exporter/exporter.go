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
	stateDeleted = "deleted"

	datumStateProcessed = "processed"
	datumStateFailed    = "failed"
	datumStateSkipped   = "skipped"
)

type Exporter struct {
	pachClient   PachydermClient
	queryTimeout time.Duration
	mutex        sync.Mutex
	// List of pipelines on pachyderm, and their states
	pipelines map[string]pps.PipelineState
	// Map of jobID to job metadata
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
	pipelines         *prometheus.Desc
	jobsCompleted     *prometheus.CounterVec
	jobsRunning       *prometheus.Desc
	jobsStarting      *prometheus.Desc
	datums            *prometheus.CounterVec
	lastSuccessfulJob *prometheus.Desc
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
			pipelines: prometheus.NewDesc(
				"pachyderm_pipeline_states",
				"State of each pipeline. Set to 1 if pipeline is in the given state.",
				[]string{"state", "pipeline"}, nil,
			),
			jobsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "pachyderm_jobs_completed_total",
				Help: "Total number of jobs that pachyderm has completed, by state and pipeline",
			}, []string{"state", "pipeline"}),
			jobsRunning: prometheus.NewDesc(
				"pachyderm_jobs_running",
				"Number of pachyderm jobs in the RUNNING state",
				[]string{"pipeline"}, nil,
			),
			jobsStarting: prometheus.NewDesc(
				"pachyderm_jobs_starting",
				"Number of pachyderm jobs in the STARTING state",
				[]string{"pipeline"}, nil,
			),
			lastSuccessfulJob: prometheus.NewDesc(
				"pachyderm_last_successful_job",
				"When the last successful job ran, as a Unix timestamp in seconds",
				[]string{"pipeline"}, nil,
			),
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
	e.m.jobsCompleted.Describe(ch)
	e.m.datums.Describe(ch)
	e.m.downloaded.Describe(ch)
	e.m.uploaded.Describe(ch)
	e.m.downloadTime.Describe(ch)
	e.m.uploadTime.Describe(ch)
	e.m.processTime.Describe(ch)
	ch <- e.m.pipelines
	ch <- e.m.jobsRunning
	ch <- e.m.jobsStarting
	ch <- e.m.lastSuccessfulJob
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
	e.m.jobsCompleted.Collect(ch)
	e.m.datums.Collect(ch)
	e.m.downloaded.Collect(ch)
	e.m.uploaded.Collect(ch)
	e.m.downloadTime.Collect(ch)
	e.m.uploadTime.Collect(ch)
	e.m.processTime.Collect(ch)
	e.collectPipelineStates(ch)
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
	// reset and populate e.pipelines
	e.pipelines = make(map[string]pps.PipelineState, len(pipelines))
	for _, pipeline := range pipelines {
		name := pipeline.Pipeline.Name
		e.pipelines[name] = pipeline.State
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
			e.runningJobs[jobID] = job
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

	// Any job we haven't seen has probably been deleted, clear it out
	for jobID, job := range notSeen {
		log.Printf("Job %s was not seen, clearing it out", jobID)
		switch job.State {
		case pps.JobState_JOB_RUNNING:
			delete(e.runningJobs, jobID)
		case pps.JobState_JOB_STARTING:
			delete(e.startingJobs, jobID)
		}
		e.m.jobsCompleted.WithLabelValues(stateDeleted, job.Pipeline.Name).Inc()
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

func (e *Exporter) collectPipelineStates(ch chan<- prometheus.Metric) {
	for pipeline, state := range e.pipelines {
		for _, possibleState := range pps.PipelineState_name {
			stateLabel := strings.ToLower(strings.TrimPrefix(possibleState, "PIPELINE_"))
			// Populate a 0 or 1 for each pipeline in every state
			if possibleState == state.String() {
				ch <- prometheus.MustNewConstMetric(e.m.pipelines, prometheus.GaugeValue, 1, stateLabel, pipeline)
			} else {
				ch <- prometheus.MustNewConstMetric(e.m.pipelines, prometheus.GaugeValue, 0, stateLabel, pipeline)
			}
		}
	}
}

func (e *Exporter) collectRunningJobs(ch chan<- prometheus.Metric) {
	jobs := byPipeline(e.runningJobs)

	for pipeline := range e.pipelines {
		ch <- prometheus.MustNewConstMetric(e.m.jobsRunning, prometheus.GaugeValue, float64(jobs[pipeline]), pipeline)
	}
}

func (e *Exporter) collectStartingJobs(ch chan<- prometheus.Metric) {
	jobs := byPipeline(e.startingJobs)

	for pipeline := range e.pipelines {
		ch <- prometheus.MustNewConstMetric(e.m.jobsStarting, prometheus.GaugeValue, float64(jobs[pipeline]), pipeline)
	}
}

func (e *Exporter) collectLastSuccess(ch chan<- prometheus.Metric) {
	for pipeline := range e.pipelines {
		ch <- prometheus.MustNewConstMetric(e.m.lastSuccessfulJob, prometheus.GaugeValue, float64(e.lastSuccessfulJob[pipeline]), pipeline)
	}
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
