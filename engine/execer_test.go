package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drone/drone-go/drone"
	"github.com/drone/runner-go/pipeline"
	"github.com/drone/runner-go/pipeline/runtime"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

type executionEngineStub struct {
	setup   func(context.Context, runtime.Spec) error
	run     func(context.Context, runtime.Spec, runtime.Step, io.Writer) (*runtime.State, error)
	destroy func(context.Context, runtime.Spec) error
}

func (e executionEngineStub) Setup(ctx context.Context, s runtime.Spec) error {
	if e.setup != nil {
		return e.setup(ctx, s)
	}
	return nil
}
func (e executionEngineStub) Run(ctx context.Context, s runtime.Spec, step runtime.Step, w io.Writer) (*runtime.State, error) {
	if e.run != nil {
		return e.run(ctx, s, step, w)
	}
	return &runtime.State{Exited: true}, nil
}
func (e executionEngineStub) Destroy(ctx context.Context, s runtime.Spec) error {
	return e.destroy(ctx, s)
}

type executionReporterStub struct {
	stage func(context.Context, *pipeline.State) error
	step  func(context.Context, *pipeline.State, string) error
}

func (r executionReporterStub) ReportStage(ctx context.Context, s *pipeline.State) error {
	if r.stage != nil {
		return r.stage(ctx, s)
	}
	return nil
}
func (r executionReporterStub) ReportStep(ctx context.Context, s *pipeline.State, name string) error {
	if r.step != nil {
		return r.step(ctx, s, name)
	}
	return nil
}

func executionState(spec *Spec) *pipeline.State {
	s := &pipeline.State{Build: &drone.Build{Status: drone.StatusRunning}, Repo: &drone.Repo{}, System: &drone.System{}, Stage: &drone.Stage{Status: drone.StatusRunning}}
	for _, step := range spec.Steps {
		if step.RunPolicy != runtime.RunNever {
			s.Stage.Steps = append(s.Stage.Steps, &drone.Step{Name: step.Name, Status: drone.StatusPending, ErrIgnore: step.ErrPolicy == runtime.ErrIgnore})
		}
	}
	return s
}

func awaitExecution(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func TestCleanupWhileStepReportBlocked(t *testing.T) {
	entered, release, cleaned, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() { close(release); awaitExecution(t, returned, "executor did not return after report release") }()
	engine := executionEngineStub{destroy: func(ctx context.Context, s runtime.Spec) error {
		if ctx.Err() != nil {
			t.Error("cleanup inherited cancellation")
		}
		close(cleaned)
		return nil
	}}
	reporter := executionReporterStub{step: func(context.Context, *pipeline.State, string) error {
		close(entered)
		<-release
		return context.Canceled
	}}
	spec := &Spec{Steps: []*Step{{Name: "step", RunPolicy: runtime.RunOnSuccess}}}
	exec := NewExecer(reporter, pipeline.NopStreamer(), pipeline.NopUploader(), engine, 1)
	go func() { defer close(returned); exec.Exec(ctx, spec, executionState(spec)) }()
	awaitExecution(t, entered, "step report was not reached")
	cancel()
	awaitExecution(t, cleaned, "cancellation did not start cleanup while reporting was blocked")
	select {
	case <-returned:
		t.Fatal("report gate unexpectedly released")
	default:
	}
}

func TestSetupUsesTaskContext(t *testing.T) {
	entered, cleaned, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	engine := executionEngineStub{
		setup: func(got context.Context, _ runtime.Spec) error {
			if got != ctx {
				t.Error("Setup did not receive the task context")
			}
			close(entered)
			select {
			case <-got.Done():
				return got.Err()
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		destroy: func(context.Context, runtime.Spec) error { close(cleaned); return nil },
	}
	exec := NewExecer(pipeline.NopReporter(), pipeline.NopStreamer(), pipeline.NopUploader(), engine, 1)
	spec := &Spec{}
	go func() { defer close(returned); exec.Exec(ctx, spec, executionState(spec)) }()
	awaitExecution(t, entered, "Setup was not reached")
	awaitExecution(t, cleaned, "deadline did not trigger cleanup")
	awaitExecution(t, returned, "Setup did not respond to deadline")
}

func TestCleanupPrecedesStageReport(t *testing.T) {
	entered, release, cleaned, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() { close(release); awaitExecution(t, returned, "executor did not return") }()
	engine := executionEngineStub{destroy: func(context.Context, runtime.Spec) error { close(cleaned); return nil }}
	reporter := executionReporterStub{stage: func(context.Context, *pipeline.State) error { close(entered); <-release; return nil }}
	spec := &Spec{}
	exec := NewExecer(reporter, pipeline.NopStreamer(), pipeline.NopUploader(), engine, 1)
	go func() { defer close(returned); exec.Exec(context.Background(), spec, executionState(spec)) }()
	awaitExecution(t, entered, "stage report was not reached")
	awaitExecution(t, cleaned, "stage report blocked cleanup")
}

type executionStreamerStub struct{ close func(string) error }
type executionWriterStub struct{ close func() error }

func (s executionStreamerStub) Stream(_ context.Context, _ *pipeline.State, name string) io.WriteCloser {
	return executionWriterStub{close: func() error { return s.close(name) }}
}
func (w executionWriterStub) Write(p []byte) (int, error) { return len(p), nil }
func (w executionWriterStub) Close() error                { return w.close() }

func TestCleanupWhileFinalLogUploadBlocked(t *testing.T) {
	entered, release, cleaned, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	engine := executionEngineStub{destroy: func(context.Context, runtime.Spec) error { close(cleaned); return nil }}
	streamer := executionStreamerStub{close: func(string) error { close(entered); <-release; return nil }}
	spec := &Spec{Steps: []*Step{{Name: "step", RunPolicy: runtime.RunOnSuccess}}}
	state := executionState(spec)
	defer func() {
		close(release)
		awaitExecution(t, returned, "executor did not return")
		if state.Stage.Status != drone.StatusPassing {
			t.Errorf("normal completion changed to %s", state.Stage.Status)
		}
	}()
	exec := NewExecer(pipeline.NopReporter(), streamer, pipeline.NopUploader(), engine, 1)
	go func() { defer close(returned); exec.Exec(context.Background(), spec, state) }()
	awaitExecution(t, entered, "log upload was not reached")
	awaitExecution(t, cleaned, "final log upload blocked cleanup")
}

func TestCleanupWaitsForDependentRunAlwaysStep(t *testing.T) {
	entered, release, cleaned, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() { close(release); awaitExecution(t, returned, "executor did not return") }()
	engine := executionEngineStub{
		run: func(_ context.Context, _ runtime.Spec, step runtime.Step, _ io.Writer) (*runtime.State, error) {
			if step.GetName() == "first" {
				return &runtime.State{Exited: true, ExitCode: 1}, nil
			}
			close(entered)
			<-release
			return &runtime.State{Exited: true}, nil
		},
		destroy: func(context.Context, runtime.Spec) error { close(cleaned); return nil },
	}
	spec := &Spec{Steps: []*Step{
		{Name: "first", RunPolicy: runtime.RunOnSuccess},
		{Name: "always", RunPolicy: runtime.RunAlways, DependsOn: []string{"first"}},
	}}
	state := executionState(spec)
	exec := NewExecer(pipeline.NopReporter(), pipeline.NopStreamer(), pipeline.NopUploader(), engine, 1)
	go func() { defer close(returned); exec.Exec(context.Background(), spec, state) }()
	awaitExecution(t, entered, "dependent RunAlways step did not execute")
	select {
	case <-cleaned:
		t.Fatal("first step completion deleted resources needed by RunAlways")
	default:
	}
}

func TestCancellationRemovesPodWhileServerBlocked(t *testing.T) {
	client := cleanupClient()
	kube := New(client, time.Second, 0)
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() { close(release); awaitExecution(t, returned, "executor did not return") }()
	reporter := executionReporterStub{step: func(context.Context, *pipeline.State, string) error {
		close(entered)
		<-release
		return context.Canceled
	}}
	spec := &Spec{PodSpec: PodSpec{Name: "cancel-report", Namespace: "test"}, Steps: []*Step{{Name: "step", RunPolicy: runtime.RunOnSuccess}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := NewExecer(reporter, pipeline.NopStreamer(), pipeline.NopUploader(), kube, 1)
	go func() { defer close(returned); exec.Exec(ctx, spec, executionState(spec)) }()
	awaitExecution(t, entered, "step report was not reached")
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), spec.PodSpec.Name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	cancel()
	awaitExecution(t, spec.lifecycle().done, "Pod cleanup waited for blocked Server report")
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), spec.PodSpec.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Pod was not removed: %v", err)
	}
}

func TestSharedStepLimitAndCancellationIsolation(t *testing.T) {
	firstStarted, firstRelease, secondSetup, firstCleaned, secondCleaned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	firstReturned, secondReturned := make(chan struct{}), make(chan struct{})
	var secondRuns int32
	defer func() { close(firstRelease); awaitExecution(t, firstReturned, "first task did not return") }()
	engine := executionEngineStub{
		setup: func(_ context.Context, s runtime.Spec) error {
			if s.(*Spec).PodSpec.Name == "second" {
				close(secondSetup)
			}
			return nil
		},
		run: func(_ context.Context, s runtime.Spec, _ runtime.Step, _ io.Writer) (*runtime.State, error) {
			if s.(*Spec).PodSpec.Name == "first" {
				close(firstStarted)
				<-firstRelease
			} else {
				atomic.AddInt32(&secondRuns, 1)
			}
			return &runtime.State{Exited: true}, nil
		},
		destroy: func(_ context.Context, s runtime.Spec) error {
			if s.(*Spec).PodSpec.Name == "first" {
				close(firstCleaned)
			} else {
				close(secondCleaned)
			}
			return nil
		},
	}
	exec := NewExecer(pipeline.NopReporter(), pipeline.NopStreamer(), pipeline.NopUploader(), engine, 1)
	first := &Spec{PodSpec: PodSpec{Name: "first"}, Steps: []*Step{{Name: "step", RunPolicy: runtime.RunOnSuccess}}}
	second := &Spec{PodSpec: PodSpec{Name: "second"}, Steps: []*Step{{Name: "step", RunPolicy: runtime.RunOnSuccess}}}
	go func() { defer close(firstReturned); exec.Exec(context.Background(), first, executionState(first)) }()
	awaitExecution(t, firstStarted, "first task did not start")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { defer close(secondReturned); exec.Exec(ctx, second, executionState(second)) }()
	awaitExecution(t, secondSetup, "second task did not reach Setup")
	cancel()
	awaitExecution(t, secondCleaned, "waiting second task was not cleaned")
	awaitExecution(t, secondReturned, "second task did not return")
	if atomic.LoadInt32(&secondRuns) != 0 {
		t.Fatal("step limit was not shared across tasks")
	}
	select {
	case <-firstCleaned:
		t.Fatal("canceling the second task cleaned the first")
	default:
	}
}

func TestSetupFailuresDoNotInterruptOtherTasks(t *testing.T) {
	const failures = 20
	client := cleanupClient()
	rejected := make(map[string]string, failures)
	for i := 0; i < failures; i++ {
		resource := "secrets"
		if i%2 == 1 {
			resource = "pods"
		}
		rejected[fmt.Sprintf("failed-%02d", i)] = resource
	}
	client.PrependReactor("create", "*", func(a ktesting.Action) (bool, kuberuntime.Object, error) {
		name := a.(ktesting.CreateAction).GetObject().(metav1.Object).GetName()
		if rejected[name] == a.GetResource().Resource {
			return true, nil, apierrors.NewForbidden(a.GetResource().GroupResource(), name, errors.New("injected setup rejection"))
		}
		return false, nil, nil
	})
	kube := New(client, time.Second, 0)
	started, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		awaitExecution(t, returned, "concurrent task did not return")
	}()
	var runs int32
	// Exercise the real Kubernetes Setup/Destroy and shared runner-go executor;
	// only container execution is replaced with a deterministic step barrier.
	engine := executionEngineStub{
		setup: kube.Setup, destroy: kube.Destroy,
		run: func(ctx context.Context, spec runtime.Spec, _ runtime.Step, _ io.Writer) (*runtime.State, error) {
			name := spec.(*Spec).PodSpec.Name
			if _, failed := rejected[name]; failed {
				t.Errorf("task %s executed a step after Setup failed", name)
			}
			atomic.AddInt32(&runs, 1)
			if name == "concurrent" {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &runtime.State{Exited: true}, nil
		},
	}
	type stageReport struct{ name, status, err string }
	reports := make(chan stageReport, failures+2)
	reporter := executionReporterStub{stage: func(_ context.Context, state *pipeline.State) error {
		state.Lock()
		defer state.Unlock()
		reports <- stageReport{state.Stage.Name, state.Stage.Status, state.Stage.Error}
		return nil
	}}
	exec := NewExecer(reporter, pipeline.NopStreamer(), pipeline.NopUploader(), engine, 1)
	newTask := func(name string) (*Spec, *pipeline.State) {
		spec := &Spec{PodSpec: PodSpec{Name: name, Namespace: "test"}, Steps: []*Step{{Name: "step", RunPolicy: runtime.RunOnSuccess}}}
		state := executionState(spec)
		state.Stage.Name = name
		return spec, state
	}
	concurrent, concurrentState := newTask("concurrent")
	var concurrentErr error
	go func() {
		defer close(returned)
		concurrentErr = exec.Exec(ctx, concurrent, concurrentState)
	}()
	awaitExecution(t, started, "concurrent task did not start")
	pod, err := client.CoreV1().Pods("test").Get(ctx, concurrent.PodSpec.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < failures; i++ {
		failed, state := newTask(fmt.Sprintf("failed-%02d", i))
		if err := exec.Exec(ctx, failed, state); err != nil {
			t.Fatalf("failed task could not finish reporting: %v", err)
		}
		select {
		case report := <-reports:
			if report.name != failed.PodSpec.Name || report.status != drone.StatusError || !strings.Contains(report.err, "injected setup rejection") {
				t.Fatalf("initialization failure was not reported: %+v", report)
			}
		default:
			t.Fatal("missing failure report")
		}
		select {
		case <-concurrent.lifecycle().stop:
			t.Fatal("failed task stopped the concurrent task")
		case <-returned:
			t.Fatal("concurrent task returned before its step was released")
		default:
		}
		current, err := client.CoreV1().Pods("test").Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil || current.UID != pod.UID || current.DeletionTimestamp != nil {
			t.Fatalf("failed task affected the concurrent Pod: %v", err)
		}
	}
	close(release)
	awaitExecution(t, returned, "concurrent task did not finish after failures")
	if concurrentErr != nil || concurrentState.Stage.Status != drone.StatusPassing {
		t.Fatalf("concurrent task failed: status=%s error=%v", concurrentState.Stage.Status, concurrentErr)
	}
	next, nextState := newTask("next")
	if err := exec.Exec(ctx, next, nextState); err != nil || nextState.Stage.Status != drone.StatusPassing {
		t.Fatalf("executor could not run the next task: status=%s error=%v", nextState.Stage.Status, err)
	}
	for _, name := range []string{"concurrent", "next"} {
		select {
		case report := <-reports:
			if report.name != name || report.status != drone.StatusPassing {
				t.Fatalf("missing successful task report for %s: %+v", name, report)
			}
		default:
			t.Fatalf("missing successful task report for %s", name)
		}
	}
	if atomic.LoadInt32(&runs) != 2 {
		t.Fatalf("expected only the concurrent and next steps to run, got %d", runs)
	}
	if pods, err := client.CoreV1().Pods("test").List(ctx, metav1.ListOptions{}); err != nil || len(pods.Items) != 0 {
		t.Fatalf("task Pods were not cleaned: %v", err)
	}
	if secrets, err := client.CoreV1().Secrets("test").List(ctx, metav1.ListOptions{}); err != nil || len(secrets.Items) != 0 {
		t.Fatalf("task Secrets were not cleaned: %v", err)
	}
}

func TestCleanupOnFastFailWhileOtherLogUploadBlocked(t *testing.T) {
	logEntered, logRelease, cleaned, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() { close(logRelease); awaitExecution(t, returned, "fast-fail execution did not return") }()
	engine := executionEngineStub{
		run: func(_ context.Context, _ runtime.Spec, step runtime.Step, _ io.Writer) (*runtime.State, error) {
			if step.GetName() == "fast" {
				<-logEntered
				return &runtime.State{Exited: true, ExitCode: 1}, nil
			}
			if step.GetName() == "after" {
				t.Error("step ran after fast-fail cancellation")
			}
			return &runtime.State{Exited: true}, nil
		},
		destroy: func(context.Context, runtime.Spec) error { close(cleaned); return nil },
	}
	streamer := executionStreamerStub{close: func(name string) error {
		if name == "upload" {
			close(logEntered)
			<-logRelease
		}
		return nil
	}}
	spec := &Spec{Steps: []*Step{
		{Name: "fast", ErrPolicy: runtime.ErrFailFast},
		{Name: "upload"},
		{Name: "after", DependsOn: []string{"fast", "upload"}},
	}}
	exec := NewExecer(pipeline.NopReporter(), streamer, pipeline.NopUploader(), engine, 0)
	go func() { defer close(returned); exec.Exec(context.Background(), spec, executionState(spec)) }()
	awaitExecution(t, logEntered, "parallel upload did not start")
	awaitExecution(t, cleaned, "internal fast-fail cancellation did not trigger cleanup")
}

func TestCleanupWhileSkippedStepReportBlocked(t *testing.T) {
	entered, release, cleaned, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() { close(release); awaitExecution(t, returned, "executor did not return") }()
	engine := executionEngineStub{
		run: func(_ context.Context, _ runtime.Spec, step runtime.Step, _ io.Writer) (*runtime.State, error) {
			if step.GetName() != "first" {
				t.Error("conditional step should be skipped")
			}
			return &runtime.State{Exited: true}, nil
		},
		destroy: func(context.Context, runtime.Spec) error { close(cleaned); return nil },
	}
	reporter := executionReporterStub{step: func(_ context.Context, _ *pipeline.State, name string) error {
		if name == "skipped" {
			close(entered)
			<-release
		}
		return nil
	}}
	spec := &Spec{Steps: []*Step{
		{Name: "first"},
		{Name: "skipped", RunPolicy: runtime.RunOnFailure, DependsOn: []string{"first"}},
	}}
	exec := NewExecer(reporter, pipeline.NopStreamer(), pipeline.NopUploader(), engine, 1)
	go func() { defer close(returned); exec.Exec(context.Background(), spec, executionState(spec)) }()
	awaitExecution(t, entered, "skipped step report was not reached")
	awaitExecution(t, cleaned, "skipped step report blocked cleanup")
}
