package engine

import (
	"context"
	"io"
	"sync"

	"github.com/drone/runner-go/logger"
	"github.com/drone/runner-go/pipeline"
	"github.com/drone/runner-go/pipeline/runtime"
)

// Execer keeps resource cleanup independent of runner-go's blocking reports and
// log uploads. The upstream executor still owns scheduling and result handling.
type Execer struct {
	base     *runtime.Execer
	engine   runtime.Engine
	sessions sync.Map // *pipeline.State -> *execution
}

// NewExecer retains one shared upstream executor, including its step semaphore.
func NewExecer(reporter pipeline.Reporter, streamer pipeline.Streamer, uploader pipeline.Uploader, engine runtime.Engine, threads int64) *Execer {
	e := &Execer{engine: engine}
	e.base = runtime.NewExecer(executionReporter{reporter, e}, streamer, uploader, executionEngine{engine}, threads)
	return e
}

func (e *Execer) Exec(ctx context.Context, spec runtime.Spec, state *pipeline.State) error {
	s := &execution{
		Spec: spec, ctx: ctx, engine: e.engine,
		pending:  make(map[string]runtime.RunPolicy),
		finished: make(chan struct{}), cleaned: make(chan struct{}),
	}
	for i := 0; i < spec.StepLen(); i++ {
		step := spec.StepAt(i)
		if step.IsDetached() || step.GetRunPolicy() == runtime.RunNever {
			continue
		}
		// RunAlways bypasses upstream's already-finished check.
		if step.GetRunPolicy() != runtime.RunAlways && state.Finished(step.GetName()) {
			continue
		}
		s.pending[step.GetName()] = step.GetRunPolicy()
	}
	e.sessions.Store(state, s)
	defer e.sessions.Delete(state)
	defer close(s.finished)
	s.watch(ctx)
	return e.base.Exec(ctx, s, state)
}

// execution wraps only the Spec identity passed to engine callbacks. It never
// changes steps, their policies, or the pipeline's reported state.
type execution struct {
	runtime.Spec
	ctx          context.Context
	engine       runtime.Engine
	mu           sync.Mutex
	pending      map[string]runtime.RunPolicy
	stopOnce     sync.Once
	runWatchOnce sync.Once
	finished     chan struct{}
	cleaned      chan struct{}
	cleanupErr   error
}

func (s *execution) watch(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			s.stop()
		case <-s.finished:
		}
	}()
}

func (s *execution) stop() {
	s.stopOnce.Do(func() {
		go func() {
			ctx := logger.WithContext(context.Background(), logger.FromContext(s.ctx))
			s.cleanupErr = s.engine.Destroy(ctx, s.Spec)
			close(s.cleaned)
		}()
	})
}

func (s *execution) finishStep(name string) {
	s.mu.Lock()
	_, tracked := s.pending[name]
	delete(s.pending, name)
	complete := tracked && len(s.pending) == 0
	s.mu.Unlock()
	if complete {
		s.stop()
	}
}

type executionEngine struct{ runtime.Engine }

func (e executionEngine) Setup(_ context.Context, spec runtime.Spec) error {
	s := spec.(*execution)
	if err := s.ctx.Err(); err != nil {
		return err
	}
	err := e.Engine.Setup(s.ctx, s.Spec)
	if err != nil {
		s.stop()
	}
	return err
}

func (e executionEngine) Run(ctx context.Context, spec runtime.Spec, step runtime.Step, output io.Writer) (*runtime.State, error) {
	s := spec.(*execution)
	// runner-go adds an internal cancellation context for fail-fast execution.
	s.runWatchOnce.Do(func() { s.watch(ctx) })
	defer s.finishStep(step.GetName())
	return e.Engine.Run(ctx, s.Spec, step, output)
}

func (e executionEngine) Destroy(_ context.Context, spec runtime.Spec) error {
	s := spec.(*execution)
	s.stop()
	<-s.cleaned
	return s.cleanupErr
}

type executionReporter struct {
	pipeline.Reporter
	exec *Execer
}

func (r executionReporter) ReportStage(ctx context.Context, state *pipeline.State) error {
	if value, ok := r.exec.sessions.Load(state); ok {
		value.(*execution).stop()
	}
	return r.Reporter.ReportStage(ctx, state)
}

func (r executionReporter) ReportStep(ctx context.Context, state *pipeline.State, name string) error {
	value, ok := r.exec.sessions.Load(state)
	if !ok {
		return r.Reporter.ReportStep(ctx, state, name)
	}
	s := value.(*execution)
	s.mu.Lock()
	policy, tracked := s.pending[name]
	s.mu.Unlock()
	// Skipped steps never reach Engine.Run. RunAlways may still execute despite
	// a previously finished state, so only its Run return settles its work.
	if tracked && policy != runtime.RunAlways && state.Finished(name) {
		s.finishStep(name)
	}
	err := r.Reporter.ReportStep(ctx, state, name)
	if err != nil {
		s.finishStep(name)
	}
	return err
}
