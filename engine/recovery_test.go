package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authorization "k8s.io/api/authorization/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func recoveryFixture(t *testing.T) (*Kubernetes, *recovery, *cleanupFake) {
	t.Helper()
	c := cleanupClient()
	c.PrependReactor("create", "selfsubjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authorization.SelfSubjectAccessReview{Status: authorization.SubjectAccessReviewStatus{Allowed: true}}, nil
	})
	c.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
		a.(ktesting.CreateAction).GetObject().(metav1.Object).SetResourceVersion("1")
		return false, nil, nil
	})
	// The client-go fake does not enforce optimistic locking by itself.
	c.PrependReactor("update", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
		next := a.(ktesting.UpdateAction).GetObject().(*v1.ConfigMap)
		old, err := c.Tracker().Get(v1.SchemeGroupVersion.WithResource("configmaps"), a.GetNamespace(), next.Name)
		if err != nil {
			return true, nil, err
		}
		prev := old.(*v1.ConfigMap)
		if next.ResourceVersion != prev.ResourceVersion {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, next.Name, errors.New("stale version"))
		}
		n, _ := strconv.Atoi(prev.ResourceVersion)
		next.ResourceVersion = strconv.Itoa(n + 1)
		return false, nil, nil
	})
	ctx := context.Background()
	_, err := c.CoreV1().Namespaces().Create(ctx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "management"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CoreV1().Pods("runners").Create(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "runner"},
		Status:     v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{Name: "runner", ContainerID: "containerd://old", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	k := New(c, time.Second, 0).(*Kubernetes)
	r, err := newRecovery(ctx, k, RecoveryConfig{Namespace: "management", Pool: "pool", PodNamespace: "runners", PodName: "runner", PodUID: "uid-runner", ContainerName: "runner"})
	if err != nil {
		t.Fatal(err)
	}
	k.recovery = r
	return k, r, c
}

func readCleanupRecord(t *testing.T, r *recovery, task string) (*v1.ConfigMap, cleanupRecord) {
	t.Helper()
	cm, err := r.client.CoreV1().ConfigMaps(r.config.Namespace).Get(context.Background(), recordName(task), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := r.decode(cm)
	if err != nil {
		t.Fatal(err)
	}
	return cm, rec
}

func TestRecoveryIntentPrecedesCreateAndExcludesSecrets(t *testing.T) {
	k, r, c := recoveryFixture(t)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}, Secrets: map[string]*Secret{"password": {Name: "password", Data: "must-not-persist"}}}
	c.PrependReactor("create", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
		if a.GetResource().Resource != "pods" && a.GetResource().Resource != "secrets" {
			return false, nil, nil
		}
		obj := a.(ktesting.CreateAction).GetObject().(metav1.Object)
		saved, err := c.Tracker().Get(v1.SchemeGroupVersion.WithResource("configmaps"), "management", recordName(obj.GetLabels()[cleanupLabel]))
		if err != nil {
			t.Error("resource create preceded its recovery record")
			return false, nil, nil
		}
		var rec cleanupRecord
		json.Unmarshal([]byte(saved.(*v1.ConfigMap).Data["record"]), &rec)
		found := false
		for _, res := range rec.Resources {
			if res.Attempt == obj.GetLabels()[attemptLabel] && res.Unknown {
				found = true
			}
		}
		if !found {
			t.Error("resource create preceded its durable attempt")
		}
		return false, nil, nil
	})
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	cm, rec := readCleanupRecord(t, r, s.lifecycle().id)
	if strings.Contains(cm.Data["record"], "must-not-persist") || rec.Closed {
		t.Fatal("record leaked a secret or closed an active task")
	}
	if err := k.Destroy(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CoreV1().ConfigMaps("management").Get(context.Background(), cm.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("completed record retained: %v", err)
	}
}

func TestRecoveryRequiresMatchingTerminationEvidence(t *testing.T) {
	for _, scenario := range []string{"active", "missing", "replacement", "not-ready", "wrong-container", "terminated"} {
		t.Run(scenario, func(t *testing.T) {
			k, r, c := recoveryFixture(t)
			s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
			if err := k.Setup(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			pod, _ := c.CoreV1().Pods("runners").Get(context.Background(), "runner", metav1.GetOptions{})
			switch scenario {
			case "missing":
				c.CoreV1().Pods("runners").Delete(context.Background(), "runner", metav1.DeleteOptions{})
			case "replacement":
				pod.UID = "other-pod"
			case "not-ready":
				pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}
			case "terminated", "wrong-container":
				id := "containerd://old"
				if scenario == "wrong-container" {
					id = "containerd://unrelated"
				}
				pod.Status.ContainerStatuses[0].ContainerID = "containerd://new"
				pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &v1.ContainerStateTerminated{ContainerID: id, FinishedAt: metav1.Now()}
			}
			if scenario != "missing" {
				c.CoreV1().Pods("runners").Update(context.Background(), pod, metav1.UpdateOptions{})
			}
			for i := 0; i < 2; i++ {
				if err := r.reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			_, err := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{})
			if scenario == "terminated" {
				if !apierrors.IsNotFound(err) {
					t.Fatal("dead executor's Pod was not removed")
				}
				if _, err := c.CoreV1().ConfigMaps("management").Get(context.Background(), recordName(s.lifecycle().id), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Fatal("dead executor's completed record was not removed")
				}
			} else {
				if err != nil {
					t.Fatalf("unsafe reclamation without matching evidence: %v", err)
				}
				readCleanupRecord(t, r, s.lifecycle().id)
			}
		})
	}
}

func TestRecoveryUnknownCreateSurvivesRestartAndLateCommit(t *testing.T) {
	k, r, c := recoveryFixture(t)
	s := &Spec{PodSpec: PodSpec{Name: "late", Namespace: "test"}}
	state := s.lifecycle()
	if err := r.initialize(state); err != nil {
		t.Fatal(err)
	}
	state.resources = []cleanupResource{{Kind: "pods", Name: "late", Namespace: "test", Attempt: "attempt", Unknown: true}}
	state.closed = true
	if err := r.sync(state); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, rec := readCleanupRecord(t, r, state.id)
	if !rec.Resources[0].Unknown || rec.Resources[0].Done {
		t.Fatal("NotFound settled an unknown request")
	}
	// This is a new controller with no previous in-memory task state. The
	// resource is committed only AFTER its predecessor observed NotFound.
	restarted := &recovery{client: c, engine: k, config: r.config, owner: r.owner}
	restarted.owner.Session = "new-session"
	_, err := c.CoreV1().Pods("test").Create(context.Background(), &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late", Labels: map[string]string{
		cleanupLabel: state.id, attemptLabel: "attempt", protocolLabel: protocolVersion, poolLabel: "pool",
	}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := restarted.reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "late", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("late Pod was not removed")
	}
	_, rec = readCleanupRecord(t, restarted, state.id)
	if !recordComplete(rec) {
		t.Fatal("late create was not settled durably")
	}
	// A live owner consumes the completed facts, then removes the record.
	if complete, err := k.cleanOnce(state); !complete || err != nil {
		t.Fatalf("owner acknowledgement failed: %v", err)
	}
}

func TestRecoveryPreservesActiveTasksAndSharedObjects(t *testing.T) {
	k, r, c := recoveryFixture(t)
	active := &Spec{PodSpec: PodSpec{Name: "active", Namespace: "test"}}
	ended := &Spec{PodSpec: PodSpec{Name: "ended", Namespace: "test"}}
	for _, s := range []*Spec{active, ended} {
		if err := k.Setup(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	ended.lifecycle().closed = true
	if err := r.sync(ended.lifecycle()); err != nil {
		t.Fatal(err)
	}
	c.CoreV1().Secrets("test").Create(context.Background(), &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "shared"}}, metav1.CreateOptions{})
	c.CoreV1().PersistentVolumeClaims("test").Create(context.Background(), &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "shared"}}, metav1.CreateOptions{})
	for i := 0; i < 3; i++ {
		r.reconcile(context.Background())
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "active", metav1.GetOptions{}); err != nil {
		t.Fatal("active task was removed")
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "ended", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("handed-off task was not removed")
	}
	if _, err := c.CoreV1().Secrets("test").Get(context.Background(), "shared", metav1.GetOptions{}); err != nil {
		t.Fatal("shared Secret was removed")
	}
	if _, err := c.CoreV1().PersistentVolumeClaims("test").Get(context.Background(), "shared", metav1.GetOptions{}); err != nil {
		t.Fatal("shared PVC was removed")
	}
}

func TestRecoveryOtherReplicaCleansOnlyTerminatedOwner(t *testing.T) {
	k, r, c := recoveryFixture(t)
	old := &Spec{PodSpec: PodSpec{Name: "old-task", Namespace: "test"}}
	if err := k.Setup(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	_, err := c.CoreV1().Pods("runners").Create(context.Background(), &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "replica-two"},
		Status:     v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{Name: "runner", ContainerID: "containerd://second", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	k2 := New(c, time.Second, 0).(*Kubernetes)
	config := r.config
	config.PodName, config.PodUID = "replica-two", "uid-replica-two"
	r2, err := newRecovery(context.Background(), k2, config)
	if err != nil {
		t.Fatal(err)
	}
	k2.recovery = r2
	active := &Spec{PodSpec: PodSpec{Name: "active-task", Namespace: "test"}}
	if err := k2.Setup(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	pod, _ := c.CoreV1().Pods("runners").Get(context.Background(), "runner", metav1.GetOptions{})
	pod.Status.ContainerStatuses[0].State = v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ContainerID: "containerd://old", FinishedAt: metav1.Now()}}
	if _, err := c.CoreV1().Pods("runners").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := r2.reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "old-task", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("old task was not removed")
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "active-task", metav1.GetOptions{}); err != nil {
		t.Fatal("other replica's active task was removed")
	}
	readCleanupRecord(t, r2, active.lifecycle().id)
}

func TestRecoveryStopWriteFailureDoesNotBlockPodDeletion(t *testing.T) {
	k, _, c := recoveryFixture(t)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	s.lifecycle().closed = true
	deny := true
	c.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if deny {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "record", errors.New("injected rejection"))
		}
		return false, nil, nil
	})
	if complete, err := k.cleanOnce(s.lifecycle()); complete || err == nil {
		t.Fatal("failed persistence must remain pending")
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("record write failure blocked Pod deletion")
	}
	deny = false
	if complete, err := k.cleanOnce(s.lifecycle()); !complete || err != nil {
		t.Fatalf("cleanup did not settle after record storage recovered: %v", err)
	}
}

func TestRecoveryRejectsChangedAttemptAndUnexpectedDisappearance(t *testing.T) {
	for _, scenario := range []string{"uid", "attempt", "missing"} {
		t.Run(scenario, func(t *testing.T) {
			k, r, c := recoveryFixture(t)
			s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
			if err := k.Setup(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			s.lifecycle().closed = true
			if err := r.sync(s.lifecycle()); err != nil {
				t.Fatal(err)
			}
			pod, _ := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{})
			if scenario == "missing" {
				c.CoreV1().Pods("test").Delete(context.Background(), "task", metav1.DeleteOptions{})
			} else {
				if scenario == "uid" {
					pod.UID = "replacement"
				} else {
					pod.Labels[attemptLabel] = "replacement"
				}
				c.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{})
			}
			for i := 0; i < 2; i++ {
				r.reconcile(context.Background())
			}
			_, record := readCleanupRecord(t, r, s.lifecycle().id)
			if recordComplete(record) {
				t.Fatal("uncertain resource was falsely finalized")
			}
			if scenario != "missing" {
				if _, err := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{}); err != nil {
					t.Fatal("mismatched Pod deleted")
				}
			}
		})
	}
}

func TestRecoveryRecordDeletionUsesUIDAndVersion(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodDelete {
			t.Errorf("unexpected method: %s", req.Method)
		}
		var opts metav1.DeleteOptions
		if err := json.NewDecoder(req.Body).Decode(&opts); err != nil {
			t.Error(err)
		}
		if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "record-uid" || opts.Preconditions.ResourceVersion == nil || *opts.Preconditions.ResourceVersion != "42" {
			t.Error("record deletion was not protected by UID and version")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Code: 409, Reason: metav1.StatusReasonConflict})
	}))
	defer api.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: api.URL})
	if err != nil {
		t.Fatal(err)
	}
	r := &recovery{client: client, config: RecoveryConfig{Namespace: "management"}}
	cm := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "record", UID: "record-uid", ResourceVersion: "42"}}
	if err := r.remove(context.Background(), cm, cleanupRecord{Closed: true}); !apierrors.IsConflict(err) {
		t.Fatalf("conflict hidden: %v", err)
	}
}

func TestRecoveryRejectsTerminationBeforeProcessSession(t *testing.T) {
	_, r, c := recoveryFixture(t)
	pod, _ := c.CoreV1().Pods("runners").Get(context.Background(), "runner", metav1.GetOptions{})
	// Pod status can lag a container restart. A matching old container ID must
	// not be used to delete tasks created by a newer process session.
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://new"
	pod.Status.ContainerStatuses[0].LastTerminationState.Terminated = &v1.ContainerStateTerminated{
		ContainerID: r.owner.ContainerID, FinishedAt: metav1.NewTime(time.Unix(0, r.owner.SessionStarted).Add(-time.Second)),
	}
	c.CoreV1().Pods("runners").Update(context.Background(), pod, metav1.UpdateOptions{})
	if dead, _ := r.executorTerminated(context.Background(), r.owner); dead {
		t.Fatal("stale termination killed a newer process session")
	}
}

func TestRecoveryConcurrentCleanersPreserveCompletion(t *testing.T) {
	k, r, c := recoveryFixture(t)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	s.lifecycle().closed = true
	if err := r.sync(s.lifecycle()); err != nil {
		t.Fatal(err)
	}
	other := &recovery{client: c, engine: k, config: r.config, owner: r.owner}
	for i := 0; i < 3; i++ {
		var workers sync.WaitGroup
		workers.Add(2)
		for _, cleaner := range []*recovery{r, other} {
			go func(r *recovery) { defer workers.Done(); r.reconcile(context.Background()) }(cleaner)
		}
		workers.Wait()
	}
	_, rec := readCleanupRecord(t, r, s.lifecycle().id)
	if !recordComplete(rec) {
		t.Fatal("concurrent cleaners lost completion facts")
	}
	if complete, err := k.cleanOnce(s.lifecycle()); !complete || err != nil {
		t.Fatalf("owner failed to finalize shared progress: %v", err)
	}
}

func TestRecoveryMergeRetainsProgressAndConflictsBlockCreates(t *testing.T) {
	k, r, c := recoveryFixture(t)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := r.initialize(s.lifecycle()); err != nil {
		t.Fatal(err)
	}
	c.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "record", errors.New("injected conflict"))
	})
	if err := k.create(context.Background(), s, "pods", toPod(s)); err == nil {
		t.Fatal("resource create proceeded after intent conflict")
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("Pod was created without durable intent")
	}
	_, old := readCleanupRecord(t, r, s.lifecycle().id)
	old.Resources = []cleanupResource{{Kind: "pods", Namespace: "test", Name: "task", Attempt: "a", UID: types.UID("uid"), Done: true, DeleteRequested: true}}
	old.Closed = true
	stale := old
	stale.Closed = false
	stale.Resources = []cleanupResource{{Kind: "pods", Namespace: "test", Name: "task", Attempt: "a", Unknown: true}}
	merged, err := mergeRecord(old, stale)
	if err != nil || !merged.Closed || !merged.Resources[0].Done || merged.Resources[0].Unknown || merged.Resources[0].UID != "uid" {
		t.Fatalf("stale writer erased progress: %+v %v", merged, err)
	}
}

func TestRecoveryStopsNewActionsAfterLeaseCancellation(t *testing.T) {
	k, r, c := recoveryFixture(t)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	s.lifecycle().closed = true
	if err := r.sync(s.lifecycle()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) { cancel(); return false, nil, nil })
	cm, _ := readCleanupRecord(t, r, s.lifecycle().id)
	if err := r.reconcileRecord(ctx, cm); err != context.Canceled {
		t.Fatalf("expected lost-lease cancellation, got %v", err)
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{}); err != nil {
		t.Fatal("cleanup issued deletion after losing its lease")
	}
}

func TestRecoveryRejectsUnavailableIdentityAndPermissions(t *testing.T) {
	k, r, c := recoveryFixture(t)
	config := r.config
	config.PodUID = "wrong"
	if _, err := newRecovery(context.Background(), k, config); err == nil {
		t.Fatal("incorrect identity accepted")
	}
	c.PrependReactor("create", "selfsubjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authorization.SelfSubjectAccessReview{}, nil
	})
	if _, err := newRecovery(context.Background(), k, r.config); err == nil {
		t.Fatal("missing permissions accepted")
	}
	if err := r.checkTask(context.Background(), &Spec{PodSpec: PodSpec{Namespace: "denied"}}); err == nil {
		t.Fatal("task namespace permissions not checked")
	}
}

func TestRecoveryKnownSecretDoesNotRequireGetPermission(t *testing.T) {
	k, r, c := recoveryFixture(t)
	var secretGets int32
	c.PrependReactor("create", "selfsubjectaccessreviews", func(a ktesting.Action) (bool, runtime.Object, error) {
		review := a.(ktesting.CreateAction).GetObject().(*authorization.SelfSubjectAccessReview)
		attrs := review.Spec.ResourceAttributes
		allowed := true
		if attrs != nil && attrs.Resource == "secrets" && attrs.Verb == "get" {
			atomic.AddInt32(&secretGets, 1)
			allowed = false
		}
		return true, &authorization.SelfSubjectAccessReview{Status: authorization.SubjectAccessReviewStatus{Allowed: allowed}}, nil
	})
	c.PrependReactor("get", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "task", errors.New("get denied"))
	})
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatalf("Setup required Secret GET permission: %v", err)
	}
	if atomic.LoadInt32(&secretGets) != 0 {
		t.Fatalf("permission preflight requested get secrets %d times", secretGets)
	}
	state := s.lifecycle()
	state.mu.Lock()
	state.closed = true
	state.mu.Unlock()
	if err := r.sync(state); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := r.reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Tracker().Get(v1.SchemeGroupVersion.WithResource("secrets"), "test", "task"); !apierrors.IsNotFound(err) {
		t.Fatalf("known Secret was not recovered without GET permission: %v", err)
	}
	_, record := readCleanupRecord(t, r, state.id)
	for _, resource := range record.Resources {
		if resource.Kind == "secrets" && !resource.Done {
			t.Fatal("recovery record did not confirm Secret removal")
		}
	}
}
