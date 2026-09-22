// Copyright 2019 Drone.IO Inc. All rights reserved.
// Use of this source code is governed by the Polyform License
// that can be found in the LICENSE file.

package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/drone-runners/drone-runner-kube/engine/launcher"
	"github.com/drone-runners/drone-runner-kube/engine/podwatcher"

	"github.com/drone/runner-go/logger"
	"github.com/drone/runner-go/pipeline/runtime"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
)

// Kubernetes implements a Kubernetes pipeline engine.
type Kubernetes struct {
	client    kubernetes.Interface
	watchers  *sync.Map
	launchers *sync.Map
	recovery  *recovery

	containerStartTimeout      time.Duration
	containerTimeToWaitForLogs time.Duration // HACK: this timeout delays fetching the logs to ensure there is enough time to stream the logs.
}

var errPodStopped = errors.New("pod has been stopped")

// New returns a new engine with the provided kubernetes client
func New(client kubernetes.Interface, containerStartTimeout, containerTimeToWaitForLogs time.Duration) runtime.Engine {
	if containerStartTimeout < time.Second {
		containerStartTimeout = time.Second
	}

	return &Kubernetes{
		client:    client,
		watchers:  &sync.Map{},
		launchers: &sync.Map{},

		containerStartTimeout:      containerStartTimeout,
		containerTimeToWaitForLogs: containerTimeToWaitForLogs,
	}
}

// Setup the pipeline environment.
func (k *Kubernetes) Setup(ctx context.Context, specv runtime.Spec) (err error) {
	spec := specv.(*Spec)
	c := spec.lifecycle()
	c.mu.Lock()
	c.setupDone = false
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.setupDone = true
		c.mu.Unlock()
		if err != nil {
			k.stopCleanup(spec)
		}
	}()

	// Setup can be canceled even when Destroy is called concurrently.
	setupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-c.stop:
			cancel()
		case <-setupCtx.Done():
		}
	}()
	if k.recovery != nil {
		if spec.Namespace == k.recovery.config.Namespace || spec.PodSpec.Namespace == k.recovery.config.Namespace {
			return fmt.Errorf("task resources cannot use the recovery management namespace")
		}
		if err = setupCtx.Err(); err != nil {
			return err
		}
		if err = k.recovery.checkTask(setupCtx, spec); err != nil {
			return err
		}
		if err = k.recovery.initialize(c); err != nil {
			return fmt.Errorf("initialize cleanup record: %w", err)
		}
	}
	if spec.Namespace != "" {
		if err = k.create(setupCtx, spec, "namespaces", toNamespace(spec.Namespace, spec.PodSpec.Labels)); err != nil {
			return err
		}
	}
	if spec.PullSecret != nil {
		if err = k.create(setupCtx, spec, "secrets", toDockerConfigSecret(spec)); err != nil {
			return err
		}
	}
	if err = k.create(setupCtx, spec, "secrets", toSecret(spec)); err != nil {
		return err
	}
	return k.create(setupCtx, spec, "pods", toPod(spec))
}

// Destroy stops execution and waits for confirmed removal. Persistent failures
// remain queued in memory and are retried while this runner is alive.
func (k *Kubernetes) Destroy(ctx context.Context, specv runtime.Spec) error {
	spec := specv.(*Spec)
	c := k.stopCleanup(spec)
	key := spec.PodSpec.Namespace + "/" + spec.PodSpec.Name
	k.launchers.Delete(key)
	k.watchers.Delete(key)
	timer := time.NewTimer(cleanupTimeout)
	defer timer.Stop()
	select {
	case <-c.done:
		return nil
	case <-timer.C:
		c.mu.Lock()
		defer c.mu.Unlock()
		return fmt.Errorf("cleanup pending for %s (task %s): %v", key, c.id, c.err)
	}
}

// Run runs the pipeline step.
func (k *Kubernetes) Run(ctx context.Context, specv runtime.Spec, stepv runtime.Step, output io.Writer) (state *runtime.State, err error) {
	spec := specv.(*Spec)
	step := stepv.(*Step)

	podId := spec.PodSpec.Name
	podNamespace := spec.PodSpec.Namespace
	stepName := step.Name
	containerId := step.ID
	containerImage := step.Image
	containerPlaceholder := step.Placeholder

	log := logger.FromContext(ctx).
		WithField("pod", podId).
		WithField("namespace", podNamespace).
		WithField("image", containerImage).
		WithField("placeholder", containerPlaceholder).
		WithField("container", containerId).
		WithField("step", stepName)

	c := spec.lifecycle()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errPodStopped
	}
	key := podNamespace + "/" + podId
	w, loaded := k.watchers.LoadOrStore(key, &podwatcher.PodWatcher{})
	watcher := w.(*podwatcher.PodWatcher)
	if !loaded {
		watchCtx, cancel := context.WithCancel(c.ctx)
		c.watcherCancel = cancel
		watcher.Start(watchCtx, &podwatcher.KubernetesWatcher{
			PodNamespace: podNamespace,
			PodName:      podId,
			KubeClient:   k.client,
			Period:       20 * time.Second,
		})

		log.Trace("PodWatcher started")
	}

	c.mu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-c.stop:
			cancel()
		case <-runCtx.Done():
		}
	}()
	ctx = runCtx

	err = watcher.AddContainer(step.ID, step.Placeholder, step.Image)
	if err != nil {
		return
	}

	log.Debug("Engine: Starting step")

	select {
	case err = <-k.startContainer(ctx, spec, step):
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stop:
		return nil, errPodStopped
	}
	if err != nil {
		return
	}

	chErrStart := make(chan error, 1)
	go func() {
		chErrStart <- watcher.WaitContainerStart(containerId)
	}()

	select {
	case err = <-chErrStart:
	case <-time.After(k.containerStartTimeout):
		err = podwatcher.StartTimeoutContainerError{Container: containerId, Image: containerImage}
		log.WithError(err).Error("Engine: Container start timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stop:
		return nil, errPodStopped
	}
	if err != nil {
		return
	}

	err = k.fetchLogs(ctx, spec, step, output)
	if err != nil {
		return
	}

	type containerResult struct {
		code int
		err  error
	}

	chErrStop := make(chan containerResult, 1)
	go func() {
		code, err := watcher.WaitContainerTerminated(containerId)
		chErrStop <- containerResult{code: code, err: err}
	}()

	select {
	case result := <-chErrStop:
		err = result.err
		if err != nil {
			return
		}

		state = &runtime.State{
			ExitCode:  result.code,
			Exited:    true,
			OOMKilled: false,
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stop:
		return nil, errPodStopped
	}

	return
}

func (k *Kubernetes) fetchLogs(ctx context.Context, spec *Spec, step *Step, output io.Writer) error {
	// HACK: this timeout delays fetching the logs to ensure there is enough time to stream the logs.
	// it does not delay the build speed.
	timer := time.NewTimer(k.containerTimeToWaitForLogs)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-spec.lifecycle().stop:
		return errPodStopped
	case <-timer.C:
	}
	opts := &v1.PodLogOptions{
		Follow:    true,
		Container: step.ID,
	}

	req := k.client.CoreV1().RESTClient().Get().
		Namespace(spec.PodSpec.Namespace).
		Name(spec.PodSpec.Name).
		Resource("pods").
		SubResource("log").
		VersionedParams(opts, scheme.ParameterCodec)

	readCloser, err := req.Stream(ctx)
	if err != nil {
		logger.FromContext(ctx).
			WithError(err).
			WithField("pod", spec.PodSpec.Name).
			WithField("namespace", spec.PodSpec.Namespace).
			WithField("container", step.ID).
			WithField("step", step.Name).
			Error("failed to stream logs")
		return err
	}
	defer readCloser.Close()

	return cancellableCopy(ctx, output, readCloser)
}

func (k *Kubernetes) startContainer(ctx context.Context, spec *Spec, step *Step) <-chan error {
	podName := spec.PodSpec.Name
	podNamespace := spec.PodSpec.Namespace
	containerName := step.ID
	containerImage := step.Image

	c := spec.lifecycle()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		result := make(chan error, 1)
		result <- errPodStopped
		return result
	}
	_l, loaded := k.launchers.LoadOrStore(podNamespace+"/"+podName, launcher.New(podName, podNamespace, k.client, &spec.podUpdateMutex))
	l := _l.(*launcher.Launcher)
	if !loaded {
		l.Start(c.ctx)
	}
	c.mu.Unlock()

	statusEnvs := make(map[string]string)
	for _, env := range statusesWhiteList {
		statusEnvs[env] = step.Envs[env]
	}

	return l.Launch(ctx, containerName, containerImage, statusEnvs)
}
