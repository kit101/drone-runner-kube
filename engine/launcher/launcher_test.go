package launcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func TestLaunchAfterCancellationReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := New("task", "test", fake.NewSimpleClientset(), &sync.Mutex{})
	l.Start(ctx)
	cancel()
	select {
	case <-l.stopped:
	case <-time.After(time.Second):
		t.Fatal("launcher did not stop")
	}
	select {
	case err := <-l.Launch(context.Background(), "step", "image", nil):
		if err != context.Canceled {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Launch blocked after shutdown")
	}
	l.Stop()
	l.Stop()
}

func TestQueuedLaunchResolvesOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := New("task", "test", fake.NewSimpleClientset(), &sync.Mutex{})
	l.Start(ctx)
	result := l.Launch(ctx, "step", "image", nil)
	cancel()
	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued caller was abandoned")
	}
}
