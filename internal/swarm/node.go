package swarm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// DirectProcessor is implemented by the picobot agent loop.
type DirectProcessor interface {
	ProcessDirect(content string, timeout time.Duration) (string, error)
}

// NodeOptions controls node runtime and registration behavior.
type NodeOptions struct {
	NodeID          string
	ListenAddr      string
	PublicURL       string
	OrchestratorURL string
	Specializations []string
	Capacity        int
	TaskTimeout     time.Duration
}

// NodeService executes subtasks locally and exposes HTTP endpoints.
type NodeService struct {
	opts      NodeOptions
	processor DirectProcessor
	client    *http.Client
}

func NewNodeService(opts NodeOptions, processor DirectProcessor) *NodeService {
	if opts.ListenAddr == "" {
		opts.ListenAddr = ":8090"
	}
	if opts.TaskTimeout <= 0 {
		opts.TaskTimeout = 60 * time.Second
	}
	if opts.Capacity <= 0 {
		opts.Capacity = 1
	}
	if strings.TrimSpace(opts.NodeID) == "" {
		h, _ := os.Hostname()
		if h == "" {
			h = fmt.Sprintf("node-%d", time.Now().UTC().UnixNano())
		}
		opts.NodeID = h
	}
	if strings.TrimSpace(opts.PublicURL) == "" {
		opts.PublicURL = derivePublicURL(opts.ListenAddr)
	}
	if processor == nil {
		panic("node processor cannot be nil")
	}
	return &NodeService{
		opts:      opts,
		processor: processor,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (n *NodeService) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", n.handleHealth)
	mux.HandleFunc("/v1/node/tasks", n.handleTask)

	srv := &http.Server{Addr: n.opts.ListenAddr, Handler: mux}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	registered := false
	if strings.TrimSpace(n.opts.OrchestratorURL) != "" {
		regCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			if err := n.registerUntilSuccess(regCtx); err != nil {
				log.Printf("swarm node registration stopped: %v", err)
			}
		}()
		registered = true
	}

	select {
	case <-ctx.Done():
		if registered {
			n.deregister(context.Background())
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (n *NodeService) ProcessFromReader(r io.Reader, w io.Writer) error {
	var req NodeTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	artifact, err := n.processTask(req)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(artifact)
}

func (n *NodeService) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node_id": n.opts.NodeID})
}

func (n *NodeService) handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req NodeTaskRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	artifact, err := n.processTask(req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, artifact)
}

func (n *NodeService) processTask(req NodeTaskRequest) (NodeOutputArtifact, error) {
	if strings.TrimSpace(req.TaskID) == "" || strings.TrimSpace(req.SubtaskID) == "" {
		return NodeOutputArtifact{}, fmt.Errorf("task_id and subtask_id are required")
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return NodeOutputArtifact{}, fmt.Errorf("prompt is required")
	}

	timeout := n.opts.TaskTimeout
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}
	startedAt := time.Now().UTC()
	result, err := n.processor.ProcessDirect(composeNodePrompt(req), timeout)
	completedAt := time.Now().UTC()
	if err != nil {
		return NodeOutputArtifact{}, err
	}

	meta := map[string]string{
		"task_type":          req.Type,
		"duration_ms":        fmt.Sprintf("%d", completedAt.Sub(startedAt).Milliseconds()),
		"dependencies_count": fmt.Sprintf("%d", len(req.Dependencies)),
	}
	for k, v := range req.Metadata {
		meta[k] = v
	}

	return NodeOutputArtifact{
		TaskID:      req.TaskID,
		NodeID:      n.opts.NodeID,
		SubtaskID:   req.SubtaskID,
		Result:      result,
		Confidence:  estimateConfidence(result),
		Metadata:    meta,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}, nil
}

func (n *NodeService) registerUntilSuccess(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := n.register(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (n *NodeService) register(ctx context.Context) error {
	payload := NodeRegistrationRequest{
		NodeID:          n.opts.NodeID,
		Endpoint:        n.opts.PublicURL,
		Specializations: n.opts.Specializations,
		Capacity:        n.opts.Capacity,
		Metadata:        map[string]string{"registered_at": time.Now().UTC().Format(time.RFC3339)},
	}
	body, _ := json.Marshal(payload)
	url := strings.TrimRight(n.opts.OrchestratorURL, "/") + "/v1/nodes/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("register failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func (n *NodeService) deregister(ctx context.Context) {
	if strings.TrimSpace(n.opts.OrchestratorURL) == "" {
		return
	}
	body, _ := json.Marshal(NodeDeregisterRequest{NodeID: n.opts.NodeID})
	url := strings.TrimRight(n.opts.OrchestratorURL, "/") + "/v1/nodes/deregister"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func composeNodePrompt(req NodeTaskRequest) string {
	parts := []string{
		"You are a swarm subtask worker.",
		fmt.Sprintf("Objective:\n%s", req.Objective),
		fmt.Sprintf("Subtask ID: %s", req.SubtaskID),
	}
	if req.Type != "" {
		parts = append(parts, fmt.Sprintf("Subtask Type: %s", req.Type))
	}
	if len(req.Dependencies) > 0 {
		type depSummary struct {
			SubtaskID  string  `json:"subtask_id"`
			NodeID     string  `json:"node_id"`
			Confidence float64 `json:"confidence"`
			Result     string  `json:"result"`
		}
		ids := make([]string, 0, len(req.Dependencies))
		for id := range req.Dependencies {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		summary := make([]depSummary, 0, len(ids))
		for _, id := range ids {
			dep := req.Dependencies[id]
			summary = append(summary, depSummary{
				SubtaskID:  dep.SubtaskID,
				NodeID:     dep.NodeID,
				Confidence: dep.Confidence,
				Result:     truncateText(renderResult(dep.Result), 240),
			})
		}
		b, _ := json.MarshalIndent(summary, "", "  ")
		parts = append(parts, "Dependency Outputs:", string(b))
	}
	parts = append(parts, "Subtask Prompt:", req.Prompt)
	return strings.Join(parts, "\n\n")
}

func estimateConfidence(result string) float64 {
	lower := strings.ToLower(strings.TrimSpace(result))
	if lower == "" {
		return 0.1
	}
	if strings.Contains(lower, "not sure") || strings.Contains(lower, "uncertain") || strings.Contains(lower, "cannot") {
		return 0.45
	}
	return 0.82
}

func derivePublicURL(listenAddr string) string {
	host := ""
	port := "8090"
	if h, p, err := net.SplitHostPort(listenAddr); err == nil {
		host = h
		if p != "" {
			port = p
		}
	} else if strings.HasPrefix(listenAddr, ":") {
		port = strings.TrimPrefix(listenAddr, ":")
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		h, err := os.Hostname()
		if err != nil || h == "" {
			h = "localhost"
		}
		host = h
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

func truncateText(in string, max int) string {
	if len(in) <= max {
		return in
	}
	return in[:max] + "...(truncated)"
}
