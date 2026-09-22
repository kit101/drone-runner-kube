package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/drone/runner-go/logger"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	cleanupLabel   = "io.drone.cleanup-task"
	requestTimeout = 5 * time.Second
	cleanupTimeout = 60 * time.Second
)

// A create is recorded before it is sent. An unknown result must survive a
// NotFound: the API server may still commit the original request later.
type cleanupResource struct {
	Kind            string
	Namespace       string
	Name            string
	UID             types.UID
	Unknown         bool
	Done            bool
	Attempt         string
	DeleteRequested bool
}

type cleanupState struct {
	mu             sync.Mutex
	persistMu      sync.Mutex
	recordRequired bool
	ctx            context.Context
	cancel         context.CancelFunc
	stop           chan struct{}
	done           chan struct{}
	id             string
	closed         bool
	setupDone      bool
	resources      []cleanupResource
	err            error
	watcherCancel  context.CancelFunc
}

func (s *Spec) lifecycle() *cleanupState {
	s.cleanupOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.cleanup = &cleanupState{ctx: ctx, cancel: cancel, id: string(uuid.NewUUID()), setupDone: true, stop: make(chan struct{}), done: make(chan struct{})}
	})
	return s.cleanup
}

func (k *Kubernetes) create(ctx context.Context, s *Spec, kind string, obj runtime.Object) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := s.lifecycle()
	meta := obj.(metav1.Object)
	labels := make(map[string]string)
	for key, value := range meta.GetLabels() {
		labels[key] = value
	}
	labels[cleanupLabel] = c.id
	attempt := string(uuid.NewUUID())
	labels[attemptLabel], labels[protocolLabel] = attempt, protocolVersion
	if k.recovery != nil {
		labels[poolLabel] = k.recovery.config.Pool
	}
	meta.SetLabels(labels)
	ns := s.PodSpec.Namespace
	if kind == "namespaces" {
		ns = ""
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errPodStopped
	}
	i := len(c.resources)
	c.resources = append(c.resources, cleanupResource{Kind: kind, Namespace: ns, Name: meta.GetName(), Unknown: true, Attempt: attempt})
	c.mu.Unlock()
	if k.recovery != nil {
		if err := k.recovery.sync(c); err != nil {
			// This request was definitely not sent, even if its intent write had
			// an ambiguous outcome. Do not create resources without durable intent.
			c.mu.Lock()
			c.resources[i].Unknown = false
			c.resources[i].Done = true
			c.mu.Unlock()
			return fmt.Errorf("persist create intent: %w", err)
		}
	}

	// Do not transparently replay a POST with an unknown outcome.
	requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var result metav1.Object
	var err error
	if rest := k.client.CoreV1().RESTClient(); rest != nil {
		var created runtime.Object
		created, err = rest.Post().NamespaceIfScoped(ns, ns != "").Resource(kind).
			VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).
			Body(obj).MaxRetries(0).Do(requestCtx).Get()
		if err == nil {
			result = created.(metav1.Object)
		}
	} else {
		// Typed clients without a REST transport are used by the Kubernetes fake.
		switch kind {
		case "pods":
			result, err = k.client.CoreV1().Pods(ns).Create(requestCtx, obj.(*v1.Pod), metav1.CreateOptions{})
		case "secrets":
			result, err = k.client.CoreV1().Secrets(ns).Create(requestCtx, obj.(*v1.Secret), metav1.CreateOptions{})
		case "namespaces":
			result, err = k.client.CoreV1().Namespaces().Create(requestCtx, obj.(*v1.Namespace), metav1.CreateOptions{})
		}
	}
	c.mu.Lock()
	if err == nil {
		c.resources[i].UID = result.GetUID()
		c.resources[i].Unknown = false
	} else if apierrors.IsAlreadyExists(err) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
		// A rejected create grants no ownership over an existing same-name object.
		c.resources[i].Unknown = false
		c.resources[i].Done = true
	}
	c.mu.Unlock()
	if k.recovery != nil {
		if saveErr := k.recovery.sync(c); saveErr != nil {
			return fmt.Errorf("persist create outcome: %w", saveErr)
		}
	}
	return err
}

// stopCleanup closes the creation gate before handing resources to the cleaner.
// Cancellation and cleanup use different contexts; a canceled task still needs
// to be able to issue DELETE requests.
func (k *Kubernetes) stopCleanup(s *Spec) *cleanupState {
	c := s.lifecycle()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.stop)
		c.cancel()
		if c.watcherCancel != nil {
			c.watcherCancel()
		}
		go k.cleanUntilDone(c)
	}
	return c
}

func (k *Kubernetes) cleanUntilDone(c *cleanupState) {
	delay := time.Second
	for attempt := 1; ; attempt++ {
		complete, err := k.cleanOnce(c)
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		if complete {
			close(c.done)
			return
		}
		if err != nil && (attempt == 1 || attempt%6 == 0) {
			logger.Default.WithField("cleanup.task", c.id).WithField("attempt", attempt).
				WithError(err).Error("resource cleanup pending; will retry")
		}
		time.Sleep(delay)
		if delay < 5*time.Second {
			delay *= 2
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}
	}
}

func (k *Kubernetes) cleanOnce(c *cleanupState) (bool, error) {
	// Best effort handoff: failure to save the stop intent must not block
	// removal of resources already known to belong to this task.
	if k.recovery != nil {
		_ = k.recovery.sync(c)
	}
	c.mu.Lock()
	resources := append([]cleanupResource(nil), c.resources...)
	setupDone := c.setupDone
	c.mu.Unlock()
	var lastErr error
	complete := setupDone
	// Always attempt Pod removal before auxiliary resources. A failed secret
	// request cannot delay the first Pod deletion request.
	for _, kind := range []string{"pods", "secrets", "namespaces"} {
		for i, resource := range resources {
			if resource.Kind != kind || resource.Done {
				continue
			}
			if kind == "namespaces" && !complete {
				continue
			}
			done, uid, err := k.cleanResourceContext(context.Background(), c.id, resource, func(uid types.UID) error {
				c.mu.Lock()
				c.resources[i].UID, c.resources[i].Unknown, c.resources[i].DeleteRequested = uid, false, true
				c.mu.Unlock()
				if k.recovery != nil {
					_ = k.recovery.sync(c)
				}
				return nil
			})
			c.mu.Lock()
			if uid != "" {
				c.resources[i].UID = uid
				c.resources[i].Unknown = false
			}
			if done {
				c.resources[i].Done = true
			}
			c.mu.Unlock()
			if !done {
				complete = false
			}
			if err != nil {
				lastErr = err
			}
		}
	}
	if k.recovery != nil {
		if err := k.recovery.sync(c); err != nil && !(complete && apierrors.IsNotFound(err)) {
			return false, err
		}
		if complete {
			if err := k.recovery.finish(c); err != nil {
				return false, err
			}
		}
	}
	return complete, lastErr
}

func (k *Kubernetes) cleanResource(task string, r cleanupResource) (bool, types.UID, error) {
	return k.cleanResourceContext(context.Background(), task, r, nil)
}

func (k *Kubernetes) cleanResourceContext(ctx context.Context, task string, r cleanupResource, beforeDelete func(types.UID) error) (bool, types.UID, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	readCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	var obj metav1.Object
	var err error
	switch r.Kind {
	case "pods":
		obj, err = k.client.CoreV1().Pods(r.Namespace).Get(readCtx, r.Name, metav1.GetOptions{})
	case "secrets":
		obj, err = k.client.CoreV1().Secrets(r.Namespace).Get(readCtx, r.Name, metav1.GetOptions{})
	case "namespaces":
		obj, err = k.client.CoreV1().Namespaces().Get(readCtx, r.Name, metav1.GetOptions{})
	}
	cancel()
	if apierrors.IsNotFound(err) {
		if r.Unknown {
			return false, "", fmt.Errorf("%s %s/%s: create outcome unknown", r.Kind, r.Namespace, r.Name)
		}
		if r.Kind == "pods" && r.UID != "" && !r.DeleteRequested {
			return false, "", fmt.Errorf("pod %s/%s disappeared before managed deletion; execution stop unconfirmed", r.Namespace, r.Name)
		}
		return true, "", nil
	}
	if err != nil {
		return false, "", err
	}
	uid := obj.GetUID()
	if uid == "" || (r.UID != "" && uid != r.UID) || obj.GetLabels()[cleanupLabel] != task {
		return false, "", fmt.Errorf("%s %s/%s: ownership or UID mismatch; retained", r.Kind, r.Namespace, r.Name)
	}
	if r.Attempt != "" && (obj.GetLabels()[attemptLabel] != r.Attempt || obj.GetLabels()[protocolLabel] != protocolVersion) {
		return false, "", fmt.Errorf("%s %s/%s: create attempt mismatch; retained", r.Kind, r.Namespace, r.Name)
	}
	if k.recovery != nil && obj.GetLabels()[poolLabel] != k.recovery.config.Pool {
		return false, "", fmt.Errorf("cleanup pool mismatch; retained")
	}
	if obj.GetDeletionTimestamp() != nil {
		if time.Since(obj.GetDeletionTimestamp().Time) > cleanupTimeout {
			return false, uid, fmt.Errorf("%s %s/%s: deletion remains pending", r.Kind, r.Namespace, r.Name)
		}
		return false, uid, nil
	}
	opts := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
	if r.Kind == "pods" {
		grace := int64(30)
		opts.GracePeriodSeconds = &grace
	}
	if beforeDelete != nil {
		if err := beforeDelete(uid); err != nil {
			return false, uid, err
		}
	}
	if err := ctx.Err(); err != nil {
		return false, uid, err
	}
	deleteCtx, deleteCancel := context.WithTimeout(ctx, requestTimeout)
	defer deleteCancel()
	switch r.Kind {
	case "pods":
		err = k.client.CoreV1().Pods(r.Namespace).Delete(deleteCtx, r.Name, opts)
	case "secrets":
		err = k.client.CoreV1().Secrets(r.Namespace).Delete(deleteCtx, r.Name, opts)
	case "namespaces":
		err = k.client.CoreV1().Namespaces().Delete(deleteCtx, r.Name, opts)
	}
	// DELETE accepted is not proof of removal. Confirm with a fresh GET.
	if apierrors.IsNotFound(err) {
		err = nil
	}
	return false, uid, err
}
