package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

type cleanupFake struct{ *fake.Clientset }
type cleanupCore struct{ core.CoreV1Interface }

func (c *cleanupFake) CoreV1() core.CoreV1Interface { return cleanupCore{c.Clientset.CoreV1()} }
func (c cleanupCore) RESTClient() rest.Interface    { return nil }

func cleanupClient() *cleanupFake {
	c := &cleanupFake{fake.NewSimpleClientset()}
	c.PrependReactor("create", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
		obj := a.(ktesting.CreateAction).GetObject().(metav1.Object)
		obj.SetUID(types.UID("uid-" + obj.GetName()))
		return false, nil, nil
	})
	return c
}

func TestDestroyAfterSetupFailure(t *testing.T) {
	for _, test := range []struct {
		name, resource string
	}{
		{"namespace", "namespaces"},
		{"pull-secret", "secrets"},
		{"task-secret", "secrets"},
		{"pod", "pods"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := cleanupClient()
			c.PrependReactor("create", test.resource, func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: test.resource}, "task", errors.New("injected rejection"))
			})
			k := New(c, time.Second, 0)
			s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
			switch test.name {
			case "namespace":
				s.Namespace = "test"
			case "pull-secret":
				s.PullSecret = &Secret{Name: "task-pull", Data: "{}"}
			}
			if err := k.Setup(context.Background(), s); !apierrors.IsForbidden(err) {
				t.Fatalf("expected Setup rejection, got %v", err)
			}
			for i := 0; i < 2; i++ {
				if err := k.Destroy(context.Background(), s); err != nil {
					t.Fatal(err)
				}
			}
			if secrets, _ := c.CoreV1().Secrets("test").List(context.Background(), metav1.ListOptions{}); len(secrets.Items) != 0 {
				t.Fatal("partial Setup left a secret")
			}
		})
	}
}

func TestConcurrentDestroyAcrossSetupStates(t *testing.T) {
	for _, setup := range []string{"not-started", "failed", "succeeded"} {
		t.Run(setup, func(t *testing.T) {
			c := cleanupClient()
			if setup == "failed" {
				c.PrependReactor("create", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "task", errors.New("injected rejection"))
				})
			}
			k := New(c, time.Second, 0)
			s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
			if setup != "not-started" {
				err := k.Setup(context.Background(), s)
				if (setup == "failed" && !apierrors.IsForbidden(err)) || (setup == "succeeded" && err != nil) {
					t.Fatalf("unexpected Setup result: %v", err)
				}
			}
			start, results := make(chan struct{}), make(chan error, 8)
			for i := 0; i < cap(results); i++ {
				go func() {
					<-start
					results <- k.Destroy(context.Background(), s)
				}()
			}
			close(start)
			for i := 0; i < cap(results); i++ {
				select {
				case err := <-results:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent Destroy did not return")
				}
			}
			select {
			case <-s.lifecycle().stop:
			default:
				t.Fatal("Destroy did not close the stop signal")
			}
		})
	}
}

func TestDestroyAfterCreateConnectionLossAndDeleteTimeout(t *testing.T) {
	c := cleanupClient()
	connectionLost := errors.New("http2: client connection lost")
	var creates, deletes int32
	c.PrependReactor("create", "secrets", func(a ktesting.Action) (bool, runtime.Object, error) {
		atomic.AddInt32(&creates, 1)
		obj := a.(ktesting.CreateAction).GetObject()
		obj.(metav1.Object).SetUID("created-before-connection-loss")
		// The API committed the Secret, but its response was lost.
		if err := c.Tracker().Create(a.GetResource(), obj, a.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, nil, connectionLost
	})
	c.PrependReactor("delete", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
		if atomic.AddInt32(&deletes, 1) == 1 {
			return true, nil, errors.New("net/http: TLS handshake timeout")
		}
		return false, nil, nil
	})
	k := New(c, time.Second, 0)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != connectionLost {
		t.Fatalf("Setup lost the original create error: %v", err)
	}
	if err := k.Destroy(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&creates) != 1 || atomic.LoadInt32(&deletes) != 2 {
		t.Fatalf("unexpected request counts: create=%d delete=%d", creates, deletes)
	}
	if _, err := c.CoreV1().Secrets("test").Get(context.Background(), "task", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Secret was not removed after delete recovery: %v", err)
	}
}

func TestDestroyDoesNotDeleteUnownedResources(t *testing.T) {
	c := cleanupClient()
	_, err := c.CoreV1().Secrets("test").Create(context.Background(), &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "task"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	k := New(c, time.Second, 0)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	if err := k.Destroy(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CoreV1().Secrets("test").Get(context.Background(), "task", metav1.GetOptions{}); err != nil {
		t.Fatalf("existing secret was removed: %v", err)
	}
}

func TestDestroyRetriesPodDeleteWithCanceledContext(t *testing.T) {
	c := cleanupClient()
	var attempts int32
	c.PrependReactor("delete", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return true, nil, apierrors.NewServiceUnavailable("temporary delete failure")
		}
		return false, nil, nil
	})
	k := New(c, time.Second, 0)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := k.Destroy(ctx, s); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Fatalf("expected two delete attempts, got %d", attempts)
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Pod still exists: %v", err)
	}
}

func TestCleanupPodPrecedesSecrets(t *testing.T) {
	c := cleanupClient()
	k := New(c, time.Second, 0).(*Kubernetes)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	var deleted []string
	c.PrependReactor("delete", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
		deleted = append(deleted, a.GetResource().Resource)
		if a.GetResource().Resource == "secrets" {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "task", errors.New("injected"))
		}
		return false, nil, nil
	})
	complete, err := k.cleanOnce(s.lifecycle())
	if complete || err == nil {
		t.Fatal("secret rejection must leave cleanup pending")
	}
	if len(deleted) != 2 || deleted[0] != "pods" || deleted[1] != "secrets" {
		t.Fatalf("deletion order: %v", deleted)
	}
	if _, err := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("secret failure prevented Pod removal: %v", err)
	}
}

func TestCleanupRetainsUIDMismatch(t *testing.T) {
	c := cleanupClient()
	k := New(c, time.Second, 0).(*Kubernetes)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.Setup(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	pod, _ := c.CoreV1().Pods("test").Get(context.Background(), "task", metav1.GetOptions{})
	pod.UID = "replacement"
	if _, err := c.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if complete, err := k.cleanOnce(s.lifecycle()); complete || err == nil {
		t.Fatal("UID mismatch must remain pending")
	}
	for _, a := range c.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == "pods" {
			t.Fatal("replacement Pod was deleted")
		}
	}
}

func TestCleanupUnknownCreateFindsLatePod(t *testing.T) {
	c := cleanupClient()
	k := New(c, time.Second, 0).(*Kubernetes)
	s := &Spec{PodSpec: PodSpec{Name: "late", Namespace: "test"}}
	state := s.lifecycle()
	state.resources = []cleanupResource{{Kind: "pods", Namespace: "test", Name: "late", Unknown: true}}
	if complete, err := k.cleanOnce(state); complete || err == nil {
		t.Fatal("NotFound cannot settle an unknown create")
	}
	if state.resources[0].Done {
		t.Fatal("recovery information was lost")
	}
	_, err := c.CoreV1().Pods("test").Create(context.Background(), &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late", Labels: map[string]string{cleanupLabel: state.id}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := k.cleanOnce(state); complete || err != nil {
		t.Fatalf("expected deletion request, got complete=%v error=%v", complete, err)
	}
	if state.resources[0].Unknown || state.resources[0].UID == "" {
		t.Fatal("late create UID was not recorded")
	}
	if complete, err := k.cleanOnce(state); !complete || err != nil {
		t.Fatalf("late Pod not confirmed removed: %v", err)
	}
}

func TestDestroyWithoutSetup(t *testing.T) {
	k := New(cleanupClient(), time.Second, 0)
	s := &Spec{PodSpec: PodSpec{Name: "never-created", Namespace: "test"}}
	if err := k.Destroy(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if err := k.Setup(context.Background(), s); err == nil {
		t.Fatal("destroyed task must not create new resources")
	}
}

func TestResourceRequestsUseSingleCreateAndUIDDelete(t *testing.T) {
	var posts, deletes int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			atomic.AddInt32(&posts, 1)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Code: 503, Reason: metav1.StatusReasonServiceUnavailable})
		case http.MethodGet:
			json.NewEncoder(w).Encode(&v1.Pod{TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"}, ObjectMeta: metav1.ObjectMeta{Name: "task", UID: "original", Labels: map[string]string{cleanupLabel: "owner"}}})
		case http.MethodDelete:
			atomic.AddInt32(&deletes, 1)
			var opts metav1.DeleteOptions
			if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
				t.Error(err)
			}
			if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "original" {
				t.Error("missing UID precondition")
			}
			if opts.GracePeriodSeconds == nil || *opts.GracePeriodSeconds != 30 {
				t.Error("missing normal Pod grace period")
			}
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Success", Code: 200})
		}
	}))
	defer api.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: api.URL})
	if err != nil {
		t.Fatal(err)
	}
	k := New(client, time.Second, 0).(*Kubernetes)
	s := &Spec{PodSpec: PodSpec{Name: "task", Namespace: "test"}}
	if err := k.create(context.Background(), s, "pods", toPod(s)); err == nil {
		t.Fatal("expected Create failure")
	}
	if atomic.LoadInt32(&posts) != 1 {
		t.Fatalf("Create transparently retried %d times", posts)
	}
	if !s.lifecycle().resources[0].Unknown {
		t.Fatal("ambiguous Create response lost its recovery record")
	}
	complete, uid, err := k.cleanResource("owner", cleanupResource{Kind: "pods", Namespace: "test", Name: "task", UID: "original"})
	if err != nil || complete || uid != "original" || atomic.LoadInt32(&deletes) != 1 {
		t.Fatalf("unexpected delete result: %v %v %v", complete, uid, err)
	}
}

func TestLogDelayRespondsToCancellation(t *testing.T) {
	k := New(cleanupClient(), time.Second, time.Hour).(*Kubernetes)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := k.fetchLogs(ctx, &Spec{}, &Step{}, nil); err != context.Canceled {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
