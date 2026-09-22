package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type rule struct {
	Name       string `json:"name"`
	Plane      string `json:"plane"`
	Method     string `json:"method"`
	PathPrefix string `json:"pathPrefix"`
	Action     string `json:"action"`
	Status     int    `json:"status,omitempty"`
	Remaining  int    `json:"remaining,omitempty"`
	release    chan struct{}
	released   bool
}

type event struct {
	Time    time.Time `json:"time"`
	Plane   string    `json:"plane"`
	Method  string    `json:"method"`
	Path    string    `json:"path"`
	Outcome string    `json:"outcome"`
	Status  int       `json:"status,omitempty"`
	Rule    string    `json:"rule,omitempty"`
}

type lateCreate struct {
	mu           sync.Mutex
	plane        string
	collection   string
	objectPath   string
	headers      http.Header
	body         []byte
	canceled     bool
	notFoundSeen bool
	submitted    bool
	ready        chan struct{}
}

type proxy struct {
	mu       sync.Mutex
	rules    []*rule
	events   []event
	late     []*lateCreate
	kubeURL  *url.URL
	rpcURL   *url.URL
	kubeHTTP *http.Client
	rpcHTTP  *http.Client
}

func main() {
	p, err := newProxy()
	if err != nil {
		log.Fatal(err)
	}
	go mustServe(&http.Server{Addr: ":9090", Handler: p.controlMux()})
	go mustServe(&http.Server{Addr: ":8080", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.serve("rpc", w, r)
	})})
	tlsServer := &http.Server{Addr: ":8443", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.serve("kube", w, r)
	})}
	log.Fatal(tlsServer.ListenAndServeTLS("/tls/tls.crt", "/tls/tls.key"))
}

func newProxy() (*proxy, error) {
	kubeURL, _ := url.Parse(env("KUBE_UPSTREAM", "https://kubernetes.default.svc:443"))
	rpcURL, _ := url.Parse(env("RPC_UPSTREAM", "http://drone.drone-lab.svc:18080"))
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("cannot load Kubernetes service CA")
	}
	kubeTransport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "kubernetes.default.svc"}}
	rpcTransport := &http.Transport{Proxy: nil}
	return &proxy{
		kubeURL:  kubeURL,
		rpcURL:   rpcURL,
		kubeHTTP: &http.Client{Transport: kubeTransport},
		rpcHTTP:  &http.Client{Transport: rpcTransport},
	}, nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func mustServe(server *http.Server) {
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func (p *proxy) controlMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/rules", p.handleRules)
	mux.HandleFunc("/events", p.handleEvents)
	return mux
}

func (p *proxy) handleRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.mu.Lock()
		defer p.mu.Unlock()
		writeJSON(w, p.rules)
	case http.MethodDelete:
		p.mu.Lock()
		for _, item := range p.rules {
			if item.release != nil && !item.released {
				close(item.release)
				item.released = true
			}
		}
		p.rules = nil
		p.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		var item rule
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&item); err != nil {
			http.Error(w, "invalid rule", http.StatusBadRequest)
			return
		}
		item.Method = strings.ToUpper(item.Method)
		if item.Name == "" || (item.Plane != "kube" && item.Plane != "rpc") || item.Method == "" || item.PathPrefix == "" {
			http.Error(w, "name, plane, method and pathPrefix are required", http.StatusBadRequest)
			return
		}
		if item.Action != "reject" && item.Action != "block" && item.Action != "late_create" {
			http.Error(w, "action must be reject, block or late_create", http.StatusBadRequest)
			return
		}
		if item.Action == "late_create" && item.Method != http.MethodPost {
			http.Error(w, "late_create requires POST", http.StatusBadRequest)
			return
		}
		if item.Status == 0 {
			item.Status = http.StatusServiceUnavailable
		}
		if item.Remaining == 0 {
			item.Remaining = -1
		}
		if item.Action == "block" {
			item.release = make(chan struct{})
		}
		p.mu.Lock()
		p.rules = append(p.rules, &item)
		p.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, item)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (p *proxy) handleEvents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.mu.Lock()
		defer p.mu.Unlock()
		writeJSON(w, p.events)
	case http.MethodDelete:
		p.mu.Lock()
		p.events = nil
		p.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (p *proxy) serve(plane string, w http.ResponseWriter, r *http.Request) {
	if matched := p.matchRule(plane, r.Method, r.URL.Path); matched != nil {
		switch matched.Action {
		case "reject":
			p.addEvent(event{Plane: plane, Method: r.Method, Path: r.URL.Path, Outcome: "rejected", Status: matched.Status, Rule: matched.Name})
			if plane == "kube" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(matched.Status)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"apiVersion": "v1", "kind": "Status", "status": "Failure",
					"message": "fault injected", "reason": statusReason(matched.Status), "code": matched.Status,
				})
			} else {
				http.Error(w, "fault injected", matched.Status)
			}
			return
		case "block":
			p.addEvent(event{Plane: plane, Method: r.Method, Path: r.URL.Path, Outcome: "blocked", Rule: matched.Name})
			select {
			case <-r.Context().Done():
				p.addEvent(event{Plane: plane, Method: r.Method, Path: r.URL.Path, Outcome: "client_canceled", Rule: matched.Name})
				return
			case <-matched.release:
				p.addEvent(event{Plane: plane, Method: r.Method, Path: r.URL.Path, Outcome: "released", Rule: matched.Name})
				p.forward(plane, w, r)
				return
			}
		case "late_create":
			p.handleLateCreate(plane, matched, w, r)
			return
		}
	}
	p.forward(plane, w, r)
}

func (p *proxy) matchRule(plane, method, requestPath string) *rule {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, candidate := range p.rules {
		if candidate.Plane != plane || candidate.Method != method || !strings.HasPrefix(requestPath, candidate.PathPrefix) || candidate.Remaining == 0 {
			continue
		}
		if candidate.Remaining > 0 {
			candidate.Remaining--
		}
		copy := *candidate
		return &copy
	}
	return nil
}

func (p *proxy) handleLateCreate(plane string, matched *rule, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
	if err != nil {
		http.Error(w, "cannot read request", http.StatusBadRequest)
		return
	}
	var metadata struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &metadata); err != nil || metadata.Metadata.Name == "" {
		http.Error(w, "late_create requires metadata.name", http.StatusBadRequest)
		return
	}
	item := &lateCreate{
		plane: plane, collection: r.URL.RequestURI(),
		objectPath: strings.TrimSuffix(r.URL.Path, "/") + "/" + url.PathEscape(metadata.Metadata.Name),
		headers:    r.Header.Clone(), body: body, ready: make(chan struct{}),
	}
	p.mu.Lock()
	p.late = append(p.late, item)
	p.mu.Unlock()
	p.addEvent(event{Plane: plane, Method: r.Method, Path: r.URL.Path, Outcome: "late_create_held", Rule: matched.Name})
	go p.submitLate(item, matched.Name)
	<-r.Context().Done()
	item.mu.Lock()
	item.canceled = true
	ready := item.notFoundSeen && !item.submitted
	if ready {
		item.submitted = true
		close(item.ready)
	}
	item.mu.Unlock()
	p.addEvent(event{Plane: plane, Method: r.Method, Path: r.URL.Path, Outcome: "late_create_client_canceled", Rule: matched.Name})
}

func (p *proxy) submitLate(item *lateCreate, ruleName string) {
	<-item.ready
	target := p.kubeURL.ResolveReference(&url.URL{Path: item.collection})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, target.String(), bytes.NewReader(item.body))
	if err != nil {
		p.addEvent(event{Plane: item.plane, Method: http.MethodPost, Path: item.collection, Outcome: "late_create_build_failed", Rule: ruleName})
		return
	}
	req.Header = item.headers.Clone()
	req.Host = target.Host
	res, err := p.kubeHTTP.Do(req)
	if err != nil {
		p.addEvent(event{Plane: item.plane, Method: http.MethodPost, Path: item.collection, Outcome: "late_create_submit_error", Rule: ruleName})
		return
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	p.addEvent(event{Plane: item.plane, Method: http.MethodPost, Path: item.collection, Outcome: "late_create_submitted", Status: res.StatusCode, Rule: ruleName})
}

func (p *proxy) observeNotFound(plane, requestPath string) {
	p.mu.Lock()
	items := append([]*lateCreate(nil), p.late...)
	p.mu.Unlock()
	for _, item := range items {
		if item.plane != plane || item.objectPath != requestPath {
			continue
		}
		item.mu.Lock()
		item.notFoundSeen = true
		ready := item.canceled && !item.submitted
		if ready {
			item.submitted = true
			close(item.ready)
		}
		item.mu.Unlock()
	}
}

func (p *proxy) forward(plane string, w http.ResponseWriter, incoming *http.Request) {
	upstream, client := p.kubeURL, p.kubeHTTP
	if plane == "rpc" {
		upstream, client = p.rpcURL, p.rpcHTTP
	}
	target := upstream.ResolveReference(&url.URL{Path: incoming.URL.Path, RawQuery: incoming.URL.RawQuery})
	request, err := http.NewRequestWithContext(incoming.Context(), incoming.Method, target.String(), incoming.Body)
	if err != nil {
		http.Error(w, "proxy request failed", http.StatusBadGateway)
		return
	}
	request.Header = incoming.Header.Clone()
	request.Host = target.Host
	response, err := client.Do(request)
	if err != nil {
		p.addEvent(event{Plane: plane, Method: incoming.Method, Path: incoming.URL.Path, Outcome: "upstream_error"})
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	_, copyErr := io.Copy(flushWriter{ResponseWriter: w}, response.Body)
	outcome := "forwarded"
	if copyErr != nil {
		outcome = "stream_canceled"
	}
	p.addEvent(event{Plane: plane, Method: incoming.Method, Path: incoming.URL.Path, Outcome: outcome, Status: response.StatusCode})
	if incoming.Method == http.MethodGet && response.StatusCode == http.StatusNotFound {
		p.observeNotFound(plane, incoming.URL.Path)
		p.addEvent(event{Plane: plane, Method: incoming.Method, Path: incoming.URL.Path, Outcome: "late_create_notfound_observed", Status: response.StatusCode})
	}
}

type flushWriter struct {
	http.ResponseWriter
}

func (w flushWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, err
}

func statusReason(status int) string {
	switch status {
	case http.StatusForbidden:
		return "Forbidden"
	case http.StatusConflict:
		return "Conflict"
	case http.StatusTooManyRequests:
		return "TooManyRequests"
	case http.StatusServiceUnavailable:
		return "ServiceUnavailable"
	default:
		return "InternalError"
	}
}

func (p *proxy) addEvent(item event) {
	item.Time = time.Now().UTC()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.events) >= 5000 {
		copy(p.events, p.events[len(p.events)-4000:])
		p.events = p.events[:4000]
	}
	p.events = append(p.events, item)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("control response encode failed: %v", err)
	}
}

var _ = fmt.Sprintf
var _ = path.Base
