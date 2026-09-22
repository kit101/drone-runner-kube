package engine

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	protocolLabel   = "io.drone.cleanup-version"
	poolLabel       = "io.drone.cleanup-pool"
	attemptLabel    = "io.drone.cleanup-attempt"
	protocolVersion = "1"
)

type executorIdentity struct {
	Namespace      string
	Pod            string
	PodUID         types.UID
	Container      string
	ContainerID    string
	Session        string
	SessionStarted int64
}

// This deliberately contains no Spec, script, environment, or secret contents.
type cleanupRecord struct {
	Version   string
	Pool      string
	Task      string
	Owner     executorIdentity
	Closed    bool
	Reason    string
	Resources []cleanupResource
	LastError string
}

func recordName(task string) string { return "drone-cleanup-" + task }

func (r *recovery) snapshot(c *cleanupState) cleanupRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec := cleanupRecord{Version: protocolVersion, Pool: r.config.Pool, Task: c.id, Owner: r.owner,
		Closed: c.closed && c.setupDone, Resources: append([]cleanupResource(nil), c.resources...)}
	if rec.Closed {
		rec.Reason = "task-finished"
	}
	if c.err != nil {
		rec.LastError = recoveryError(c.err)
	}
	return rec
}

func recoveryError(err error) string {
	if err == nil {
		return ""
	}
	// Admission errors may contain user data. Persist only an error category.
	if reason := apierrors.ReasonForError(err); reason != metav1.StatusReasonUnknown && reason != "" {
		return string(reason)
	}
	return "cleanup incomplete; inspect runner logs"
}

func (r *recovery) initialize(c *cleanupState) error {
	rec := r.snapshot(c)
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.recordRequired = true
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err = r.client.CoreV1().ConfigMaps(r.config.Namespace).Create(ctx, &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: recordName(c.id), Labels: map[string]string{protocolLabel: protocolVersion, poolLabel: rec.Pool, cleanupLabel: c.id}},
		Data:       map[string]string{"record": string(data)},
	}, metav1.CreateOptions{})
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) || apierrors.IsNotFound(err) {
		// No resource create has been attempted and this record was rejected.
		c.mu.Lock()
		c.recordRequired = false
		c.mu.Unlock()
	}
	return err
}

func (r *recovery) decode(cm *v1.ConfigMap) (cleanupRecord, error) {
	var rec cleanupRecord
	if err := json.Unmarshal([]byte(cm.Data["record"]), &rec); err != nil {
		return rec, err
	}
	if rec.Version != protocolVersion || cm.Labels[protocolLabel] != protocolVersion ||
		rec.Pool != r.config.Pool || cm.Labels[poolLabel] != rec.Pool || rec.Task == "" ||
		cm.Name != recordName(rec.Task) || cm.Labels[cleanupLabel] != rec.Task ||
		rec.Owner.PodUID == "" || rec.Owner.Pod == "" || rec.Owner.Namespace == "" ||
		rec.Owner.Container == "" || rec.Owner.ContainerID == "" || rec.Owner.Session == "" || rec.Owner.SessionStarted <= 0 {
		return rec, fmt.Errorf("invalid cleanup record identity: %s", cm.Name)
	}
	seen := map[string]bool{}
	for _, res := range rec.Resources {
		if res.Name == "" || res.Attempt == "" || seen[res.Attempt] ||
			(res.Kind != "pods" && res.Kind != "secrets" && res.Kind != "namespaces") ||
			(res.Kind != "namespaces" && res.Namespace == "") ||
			(res.Kind == "namespaces" && (res.Namespace != "" || res.Name == r.config.Namespace)) ||
			(!res.Done && !res.Unknown && res.UID == "") || (res.Done && res.Unknown) {
			return rec, fmt.Errorf("invalid cleanup resource in %s", cm.Name)
		}
		seen[res.Attempt] = true
	}
	return rec, nil
}

// Merge only monotonic facts. A stale writer cannot reopen a closed task or
// forget an observed UID/completed deletion. Kubernetes resourceVersion guards
// the subsequent update; conflicts are re-read on the next cleanup iteration.
func mergeRecord(current, incoming cleanupRecord) (cleanupRecord, error) {
	if current.Task != incoming.Task || current.Owner != incoming.Owner || current.Pool != incoming.Pool || current.Version != incoming.Version {
		return current, fmt.Errorf("cleanup record owner changed")
	}
	for _, next := range incoming.Resources {
		found := false
		for i, prev := range current.Resources {
			if prev.Attempt != next.Attempt {
				continue
			}
			found = true
			if prev.Kind != next.Kind || prev.Namespace != next.Namespace || prev.Name != next.Name || (prev.UID != "" && next.UID != "" && prev.UID != next.UID) {
				return current, fmt.Errorf("cleanup resource identity changed")
			}
			if next.UID != "" {
				current.Resources[i].UID = next.UID
			}
			current.Resources[i].Unknown = prev.Unknown && next.Unknown
			current.Resources[i].Done = prev.Done || next.Done
			current.Resources[i].DeleteRequested = prev.DeleteRequested || next.DeleteRequested
		}
		if !found {
			if current.Closed {
				return current, fmt.Errorf("cannot add a create attempt to a closed task")
			}
			current.Resources = append(current.Resources, next)
		}
	}
	current.Closed = current.Closed || incoming.Closed
	if incoming.Reason != "" && current.Reason != "executor-terminated" {
		current.Reason = incoming.Reason
	}
	current.LastError = incoming.LastError
	return current, nil
}

func (r *recovery) save(ctx context.Context, rec cleanupRecord) (*v1.ConfigMap, cleanupRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, rec, err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	cm, err := r.client.CoreV1().ConfigMaps(r.config.Namespace).Get(ctx, recordName(rec.Task), metav1.GetOptions{})
	if err != nil {
		return nil, rec, err
	}
	current, err := r.decode(cm)
	if err != nil {
		return nil, rec, err
	}
	merged, err := mergeRecord(current, rec)
	if err != nil {
		return nil, rec, err
	}
	data, err := json.Marshal(merged)
	if err != nil {
		return nil, rec, err
	}
	cm.Data["record"] = string(data)
	cm, err = r.client.CoreV1().ConfigMaps(r.config.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return cm, merged, err
}

func (r *recovery) sync(c *cleanupState) error {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()
	c.mu.Lock()
	required := c.recordRequired
	c.mu.Unlock()
	if !required {
		return nil
	}
	rec := r.snapshot(c)
	_, saved, err := r.save(context.Background(), rec)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	local := rec
	local.Resources = c.resources
	merged, err := mergeRecord(local, saved)
	if err != nil {
		return err
	}
	c.resources = merged.Resources
	return nil
}

func recordComplete(rec cleanupRecord) bool {
	if !rec.Closed {
		return false
	}
	for _, res := range rec.Resources {
		if !res.Done || res.Unknown {
			return false
		}
	}
	return true
}

func (r *recovery) remove(ctx context.Context, cm *v1.ConfigMap, rec cleanupRecord) error {
	if !recordComplete(rec) {
		return fmt.Errorf("cleanup record still pending")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	err := r.client.CoreV1().ConfigMaps(r.config.Namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion},
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *recovery) finish(c *cleanupState) error {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()
	c.mu.Lock()
	required := c.recordRequired
	c.mu.Unlock()
	if !required {
		return nil
	}
	cm, saved, err := r.save(context.Background(), r.snapshot(c))
	// A concurrent cleaner may already have finalized this closed task.
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.remove(context.Background(), cm, saved)
}
