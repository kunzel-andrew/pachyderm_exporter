package exporter

import (
	"testing"

	"io"

	"time"

	"errors"

	"github.com/button/pachyderm_exporter/promtest"
	proto "github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	t1 = 1111
	t2 = 2222
	t3 = 3333
	t4 = 4444
	t5 = 5555
)

func TestScrape(t *testing.T) {
	client := &fakePachyderm{
		jobs: []*pps.JobInfo{
			job("reduce", "5", pps.JobState_JOB_STARTING, 0, 0),
			job("reduce", "4", pps.JobState_JOB_RUNNING, 5, 0),
			job("reduce", "3", pps.JobState_JOB_SUCCESS, 5, t3),
			job("map", "2", pps.JobState_JOB_SUCCESS, 10, t2),
			job("map", "1", pps.JobState_JOB_SUCCESS, 10, t1),
		},
		pipelines: []*pps.PipelineInfo{
			pipeline("map", "1", pps.PipelineState_PIPELINE_RUNNING),
			pipeline("reduce", "2", pps.PipelineState_PIPELINE_STARTING),
			pipeline("goofy", "3", pps.PipelineState_PIPELINE_FAILURE),
		},
	}
	exporter := New(client, time.Minute)
	reg := promtest.NewTestRegistry(t)
	reg.MustRegister(exporter)
	snapshot, err := reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 2)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "reduce"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "reduce"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "reduce"}, 1)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t2)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "reduce"}, t3)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 20)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "reduce"}, 10)

	snapshot.AssertGauge("pachyderm_pipeline_states", map[string]string{"state": "running", "pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_pipeline_states", map[string]string{"state": "starting", "pipeline": "reduce"}, 1)
	snapshot.AssertGauge("pachyderm_pipeline_states", map[string]string{"state": "failure", "pipeline": "goofy"}, 1)

	// Reduce job 4 finishes successfully and processes more datums
	// Reduce job 5 fails
	jobWithSkipsAndFails := job("map", "6", pps.JobState_JOB_SUCCESS, 10, t5)
	jobWithSkipsAndFails.DataSkipped = 7
	jobWithSkipsAndFails.DataFailed = 2
	client.jobs = []*pps.JobInfo{
		jobWithSkipsAndFails,
		job("reduce", "5", pps.JobState_JOB_FAILURE, 5, t4),
		job("reduce", "4", pps.JobState_JOB_SUCCESS, 10, t4),
		job("reduce", "3", pps.JobState_JOB_SUCCESS, 5, t3),
		job("map", "2", pps.JobState_JOB_SUCCESS, 10, t2),
		job("map", "1", pps.JobState_JOB_SUCCESS, 10, t1),
	}
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 2)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 3)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "reduce"}, 2)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateFailure, "pipeline": "map"}, 0)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateFailure, "pipeline": "reduce"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "reduce"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "reduce"}, 0)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t5)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "reduce"}, t4)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 30)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateSkipped, "pipeline": "map"}, 7)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateFailed, "pipeline": "map"}, 2)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "reduce"}, 20)

	// The exporter only read up to job ID "4" before closing the stream
	if client.lastStream.idx != 3 {
		t.Errorf("Expected 3 items read from stream, but was %v", client.lastStream.idx)
	}
}

func TestScrapeErrorAndRecover(t *testing.T) {
	client := &fakePachyderm{
		jobs: []*pps.JobInfo{
			job("map", "2", pps.JobState_JOB_RUNNING, 10, 0),
			job("map", "1", pps.JobState_JOB_SUCCESS, 10, t1),
		},
		pipelines: []*pps.PipelineInfo{
			pipeline("map", "1", pps.PipelineState_PIPELINE_RUNNING),
		},
	}
	exporter := New(client, time.Minute)
	reg := promtest.NewTestRegistry(t)
	reg.MustRegister(exporter)
	snapshot, err := reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t1)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 20)

	client.errCreateStream = true
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.AssertGauge("pachyderm_up", nil, 0)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 2)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t1)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 20)

	client.errCreateStream = false
	client.jobs = []*pps.JobInfo{
		job("map", "4", pps.JobState_JOB_FAILURE, 3, t4),
		job("map", "3", pps.JobState_JOB_STARTING, 0, 0),
		job("map", "2", pps.JobState_JOB_SUCCESS, 10, t2),
		job("map", "1", pps.JobState_JOB_SUCCESS, 10, t1),
	}
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 3)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 2)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateFailure, "pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t2)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 23)
}

func TestScrapeErrorReadStreamItemAndRecover(t *testing.T) {
	client := &fakePachyderm{
		jobs: []*pps.JobInfo{
			job("map", "2", pps.JobState_JOB_RUNNING, 10, 0),
			job("map", "1", pps.JobState_JOB_SUCCESS, 10, t1),
		},
		pipelines: []*pps.PipelineInfo{
			pipeline("map", "1", pps.PipelineState_PIPELINE_RUNNING),
		},
	}
	exporter := New(client, time.Minute)
	reg := promtest.NewTestRegistry(t)
	reg.MustRegister(exporter)
	snapshot, err := reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t1)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 20)

	// If stream breaks mid-way, expected behavior is to stop processing the rest of the lines, but export what has been read so far.
	client.errReadStream = true
	client.jobs = []*pps.JobInfo{
		job("map", "3", pps.JobState_JOB_STARTING, 0, t3),
	}
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.AssertGauge("pachyderm_up", nil, 0)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 2)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateFailure, "pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t1)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 20)

	// On next successful scrape, we should get back to a good state.
	client.errReadStream = false
	client.jobs = []*pps.JobInfo{
		job("map", "4", pps.JobState_JOB_FAILURE, 3, t4),
		job("map", "3", pps.JobState_JOB_STARTING, 0, 0),
		job("map", "2", pps.JobState_JOB_SUCCESS, 10, t2),
		job("map", "1", pps.JobState_JOB_SUCCESS, 10, t1),
	}
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_exporter_scrapes_total", nil, 3)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 2)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateFailure, "pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t2)
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 23)
}

func TestJobCounts(t *testing.T) {
	client := &fakePachyderm{
		jobs: []*pps.JobInfo{
			job("map", "1", pps.JobState_JOB_RUNNING, 10, t1),
		},
		pipelines: []*pps.PipelineInfo{
			pipeline("map", "1", pps.PipelineState_PIPELINE_RUNNING),
		},
	}
	exporter := New(client, time.Minute)
	reg := promtest.NewTestRegistry(t)
	reg.MustRegister(exporter)
	snapshot, err := reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 10)

	// More items processed, count should go up by 5
	client.jobs = []*pps.JobInfo{
		job("map", "1", pps.JobState_JOB_RUNNING, 15, t1),
	}
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 15)

	// No changes, count should stay the same
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	snapshot.AssertCount("pachyderm_datums_total", map[string]string{"state": datumStateProcessed, "pipeline": "map"}, 15)
}

func TestDeletedJobs(t *testing.T) {
	client := &fakePachyderm{
		jobs: []*pps.JobInfo{
			job("map", "3", pps.JobState_JOB_STARTING, 0, 0),
			job("map", "2", pps.JobState_JOB_RUNNING, 5, 0),
			job("map", "1", pps.JobState_JOB_SUCCESS, 5, t1),
		},
		pipelines: []*pps.PipelineInfo{
			pipeline("map", "1", pps.PipelineState_PIPELINE_RUNNING),
		},
	}
	exporter := New(client, time.Minute)
	reg := promtest.NewTestRegistry(t)
	reg.MustRegister(exporter)
	snapshot, err := reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateFailure, "pipeline": "map"}, 0)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateKilled, "pipeline": "map"}, 0)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateDeleted, "pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t1)

	// Job 2 was deleted. It should be listed as completed & "deleted".
	client.jobs = []*pps.JobInfo{
		job("map", "3", pps.JobState_JOB_RUNNING, 0, 0),
		job("map", "1", pps.JobState_JOB_SUCCESS, 5, t1),
	}
	snapshot, err = reg.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	snapshot.AssertGauge("pachyderm_up", nil, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateSuccess, "pipeline": "map"}, 1)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateFailure, "pipeline": "map"}, 0)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateKilled, "pipeline": "map"}, 0)
	snapshot.AssertCount("pachyderm_jobs_completed_total", map[string]string{"state": stateDeleted, "pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_running", map[string]string{"pipeline": "map"}, 1)
	snapshot.AssertGauge("pachyderm_jobs_starting", map[string]string{"pipeline": "map"}, 0)
	snapshot.AssertGauge("pachyderm_last_successful_job", map[string]string{"pipeline": "map"}, t1)

}

func job(pipeline, id string, state pps.JobState, datumsProcessed int64, timeCompleted int64) *pps.JobInfo {
	var finished *proto.Timestamp
	if timeCompleted != 0 {
		finished = &proto.Timestamp{
			Seconds: timeCompleted,
		}
	}
	return &pps.JobInfo{
		Pipeline: &pps.Pipeline{
			Name: pipeline,
		},
		Job: &pps.Job{
			ID: id,
		},
		State:         state,
		DataProcessed: datumsProcessed,
		Finished:      finished,
		Stats:         &pps.ProcessStats{},
	}
}

func pipeline(name, id string, state pps.PipelineState) *pps.PipelineInfo {
	return &pps.PipelineInfo{
		ID: id,
		Pipeline: &pps.Pipeline{
			Name: name,
		},
		State: state,
	}
}

type fakePachyderm struct {
	jobs       []*pps.JobInfo
	pipelines  []*pps.PipelineInfo
	idx        int
	lastStream *stream
	// Return error on creating stream
	errCreateStream bool
	// Return non-EOF error after reading to end of provided jobs
	errReadStream bool
}

func (f *fakePachyderm) ListJobStream(ctx context.Context, in *pps.ListJobRequest, opts ...grpc.CallOption) (pps.API_ListJobStreamClient, error) {
	if f.errCreateStream {
		return nil, errors.New("bad stream")
	}
	f.lastStream = &stream{
		jobs:          f.jobs,
		idx:           0,
		errReadStream: f.errReadStream,
	}
	return f.lastStream, nil
}

func (f *fakePachyderm) ListPipeline() ([]*pps.PipelineInfo, error) {
	return f.pipelines, nil
}

func (f *fakePachyderm) WithCtx(ctx context.Context) PachydermClient {
	return f
}

type stream struct {
	jobs          []*pps.JobInfo
	idx           int
	errReadStream bool
}

func (s *stream) Recv() (*pps.JobInfo, error) {
	if len(s.jobs) > s.idx {
		job := s.jobs[s.idx]
		s.idx++
		return job, nil
	}
	if s.errReadStream {
		return nil, errors.New("error reading stream")
	}
	return nil, io.EOF
}

func (s *stream) CloseSend() error {
	return nil
}

func (s *stream) SendMsg(m interface{}) error {
	panic("not implemented")
}

func (s *stream) RecvMsg(m interface{}) error {
	panic("not implemented")
}

func (s *stream) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (s *stream) Trailer() metadata.MD {
	panic("not implemented")
}

func (s *stream) Context() context.Context {
	panic("not implemented")
}
