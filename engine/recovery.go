package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/drone/runner-go/logger"
	"github.com/drone/runner-go/pipeline/runtime"
	authorization "k8s.io/api/authorization/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// RecoveryConfig enables recovery only for explicitly identified in-cluster
// runners. Namespace is a dedicated management namespace, outside task lifetime.
type RecoveryConfig struct {
	Namespace     string
	Pool          string
	PodNamespace  string
	PodName       string
	PodUID        string
	ContainerName string
}

type recovery struct {
	client kubernetes.Interface
	engine *Kubernetes
	config RecoveryConfig
	owner  executorIdentity
}

// EnableRecovery must be called before accepting tasks. Failure leaves recovery
// disabled and must abort daemon startup; callers must never silently downgrade.
func EnableRecovery(ctx context.Context, engine runtime.Engine, config RecoveryConfig) error {
	k := engine.(*Kubernetes)
	r, err := newRecovery(ctx, k, config)
	if err != nil {
		return err
	}
	k.recovery = r
	go r.run(ctx)
	return nil
}

func newRecovery(ctx context.Context, k *Kubernetes, config RecoveryConfig) (*recovery, error) {
	if len(validation.IsDNS1123Label(config.Pool)) != 0 || len(config.Pool) > 40 || config.Namespace == "" || config.PodNamespace == "" || config.PodName == "" || config.PodUID == "" || config.ContainerName == "" {
		return nil, fmt.Errorf("recovery requires a pool (DNS label, at most 40 characters), management namespace and full runner Pod/container identity")
	}
	r := &recovery{client: k.client, engine: k, config: config}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ns, err := r.client.CoreV1().Namespaces().Get(ctx, config.Namespace, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if ns.DeletionTimestamp != nil {
		return nil, fmt.Errorf("recovery namespace is terminating")
	}
	sessionStarted := time.Now().UnixNano()
	for r.owner.ContainerID == "" {
		pod, err := r.client.CoreV1().Pods(config.PodNamespace).Get(ctx, config.PodName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if pod.UID != types.UID(config.PodUID) || pod.DeletionTimestamp != nil {
			return nil, fmt.Errorf("runner Pod identity does not match")
		}
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == config.ContainerName && container.State.Running != nil && container.ContainerID != "" {
				r.owner = executorIdentity{Namespace: config.PodNamespace, Pod: config.PodName, PodUID: pod.UID,
					Container: container.Name, ContainerID: container.ContainerID, Session: string(uuid.NewUUID()), SessionStarted: sessionStarted}
			}
		}
		if r.owner.ContainerID == "" {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("running runner container identity unavailable: %w", ctx.Err())
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	// Management and executor identity permissions are checked at startup.
	checks := []struct {
		namespace, group, resource string
		verbs                      []string
	}{
		{config.Namespace, "", "configmaps", []string{"get", "list", "create", "update", "delete"}},
		{config.Namespace, "coordination.k8s.io", "leases", []string{"get", "create", "update"}},
		{config.PodNamespace, "", "pods", []string{"get"}},
	}
	for _, check := range checks {
		for _, verb := range check.verbs {
			if err := r.checkAccess(ctx, check.namespace, check.group, check.resource, verb); err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}

func (r *recovery) checkAccess(ctx context.Context, namespace, group, resource, verb string) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	attrs := &authorization.ResourceAttributes{Namespace: namespace, Group: group, Resource: resource, Verb: verb}
	if parts := strings.SplitN(resource, "/", 2); len(parts) == 2 {
		attrs.Resource, attrs.Subresource = parts[0], parts[1]
	}
	result, err := r.client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorization.SelfSubjectAccessReview{
		Spec: authorization.SelfSubjectAccessReviewSpec{ResourceAttributes: attrs},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if !result.Status.Allowed {
		return fmt.Errorf("recovery requires %s %s in namespace %q", verb, resource, namespace)
	}
	return nil
}

func (r *recovery) checkTask(ctx context.Context, s *Spec) error {
	for resource, verbs := range map[string][]string{
		"pods":     {"get", "list", "watch", "create", "update", "delete"},
		"pods/log": {"get"}, "secrets": {"get", "create", "delete"},
	} {
		for _, verb := range verbs {
			if err := r.checkAccess(ctx, s.PodSpec.Namespace, "", resource, verb); err != nil {
				return err
			}
		}
	}
	if s.Namespace != "" {
		for _, verb := range []string{"get", "create", "delete"} {
			if err := r.checkAccess(ctx, "", "", "namespaces", verb); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *recovery) run(ctx context.Context) {
	for ctx.Err() == nil {
		// Lease limits duplicate work. Cancellation, resource UID preconditions
		// and record CAS still guard every individual operation.
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.LeaseLock{
				LeaseMeta: metav1.ObjectMeta{Name: "drone-cleanup-" + r.config.Pool, Namespace: r.config.Namespace},
				Client:    r.client.CoordinationV1(), LockConfig: resourcelock.ResourceLockConfig{Identity: r.owner.Session},
			},
			LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(leaseCtx context.Context) {
					ticker := time.NewTicker(5 * time.Second)
					defer ticker.Stop()
					for {
						if err := r.reconcile(leaseCtx); err != nil && leaseCtx.Err() == nil {
							logger.Default.WithError(err).Error("resource recovery scan failed")
						}
						select {
						case <-leaseCtx.Done():
							return
						case <-ticker.C:
						}
					}
				},
				OnStoppedLeading: func() { logger.Default.Warn("resource recovery processing lease lost; stopping new actions") },
			},
		})
	}
}

func (r *recovery) executorTerminated(ctx context.Context, owner executorIdentity) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	pod, err := r.client.CoreV1().Pods(owner.Namespace).Get(ctx, owner.Pod, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if pod.UID != owner.PodUID {
		return false, fmt.Errorf("executor Pod UID changed; termination unconfirmed")
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != owner.Container {
			continue
		}
		for _, terminated := range []*v1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
			if terminated != nil && terminated.ContainerID == owner.ContainerID && terminated.FinishedAt.UnixNano() >= owner.SessionStarted {
				return true, nil
			}
		}
		if cs.ContainerID == owner.ContainerID && cs.State.Running != nil {
			return false, nil
		}
	}
	return false, fmt.Errorf("matching executor termination evidence is unavailable")
}

func (r *recovery) reconcile(ctx context.Context) error {
	readCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	list, err := r.client.CoreV1().ConfigMaps(r.config.Namespace).List(readCtx, metav1.ListOptions{
		LabelSelector: protocolLabel + "=" + protocolVersion + "," + poolLabel + "=" + r.config.Pool,
	})
	cancel()
	if err != nil {
		return err
	}
	for i := range list.Items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.reconcileRecord(ctx, &list.Items[i]); err != nil && ctx.Err() == nil {
			logger.Default.WithField("record", list.Items[i].Name).WithError(err).Warn("resource recovery pending; record retained")
		}
	}
	return nil
}

func (r *recovery) reconcileRecord(ctx context.Context, cm *v1.ConfigMap) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rec, err := r.decode(cm)
	if err != nil {
		return err
	}
	if !rec.Closed {
		terminated, err := r.executorTerminated(ctx, rec.Owner)
		if err != nil {
			return err
		}
		if !terminated {
			// A running matching container is normal, not an orphan. Never infer
			// death from age, a missing local task, or another process session.
			return nil
		}
		rec.Closed, rec.Reason = true, "executor-terminated"
	}
	cm, rec, err = r.save(ctx, rec)
	if err != nil {
		return err
	}
	var pending error
	for _, kind := range []string{"pods", "secrets", "namespaces"} {
		for i := range rec.Resources {
			res := rec.Resources[i]
			if res.Kind != kind || res.Done {
				continue
			}
			if kind == "namespaces" {
				childrenDone := true
				for _, child := range rec.Resources {
					if child.Kind != "namespaces" && !child.Done {
						childrenDone = false
					}
				}
				if !childrenDone {
					continue
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			done, uid, err := r.engine.cleanResourceContext(ctx, rec.Task, res, func(uid types.UID) error {
				rec.Resources[i].UID, rec.Resources[i].Unknown, rec.Resources[i].DeleteRequested = uid, false, true
				var saveErr error
				cm, rec, saveErr = r.save(ctx, rec)
				return saveErr
			})
			if uid != "" {
				rec.Resources[i].UID, rec.Resources[i].Unknown = uid, false
			}
			rec.Resources[i].Done = done
			if err != nil {
				pending = err
			}
		}
	}
	rec.LastError = ""
	if pending != nil {
		rec.LastError = recoveryError(pending)
	}
	cm, rec, err = r.save(ctx, rec)
	if err != nil {
		return err
	}
	if recordComplete(rec) {
		// A live owner must observe the completed facts before removing its
		// record. Otherwise its in-memory cleaner could lose the UID of a late
		// create already deleted by this replica. A dead owner needs no ack.
		if rec.Reason == "executor-terminated" {
			return r.remove(ctx, cm, rec)
		}
		dead, err := r.executorTerminated(ctx, rec.Owner)
		if err != nil {
			return err
		}
		if dead {
			return r.remove(ctx, cm, rec)
		}
	}
	return pending
}
