package swarm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/local/picobot/internal/providers"
)

type registeredNode struct {
	registration NodeRegistrationRequest
	inFlight     int
	lastSeen     time.Time
}

// Orchestrator decomposes tasks, schedules subtasks to nodes, and assembles outputs.
type Orchestrator struct {
	mu             sync.RWMutex
	nodes          map[string]*registeredNode
	assembler      *Assembler
	taskTimeout    time.Duration
	defaultRetries int
	client         *http.Client
	seq            uint64
}

func NewOrchestrator(taskTimeout time.Duration, defaultRetries int, provider providers.LLMProvider, model string) *Orchestrator {
	if taskTimeout <= 0 {
		taskTimeout = 45 * time.Second
	}
	if defaultRetries <= 0 {
		defaultRetries = 3
	}
	if provider != nil && model == "" {
		model = provider.GetDefaultModel()
	}

	return &Orchestrator{
		nodes:          map[string]*registeredNode{},
		assembler:      NewAssembler(provider, model),
		taskTimeout:    taskTimeout,
		defaultRetries: defaultRetries,
		client: &http.Client{
			Timeout: taskTimeout + 5*time.Second,
		},
	}
}

func (o *Orchestrator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", o.handleHealth)
	mux.HandleFunc("/v1/nodes", o.handleListNodes)
	mux.HandleFunc("/v1/nodes/register", o.handleRegisterNode)
	mux.HandleFunc("/v1/nodes/deregister", o.handleDeregisterNode)
	mux.HandleFunc("/v1/tasks", o.handleSubmitTask)
	return mux
}

func (o *Orchestrator) Run(ctx context.Context, listenAddr string) error {
	srv := &http.Server{Addr: listenAddr, Handler: o.Handler()}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (o *Orchestrator) RegisterNode(req NodeRegistrationRequest) error {
	if strings.TrimSpace(req.NodeID) == "" {
		return fmt.Errorf("node_id is required")
	}
	if strings.TrimSpace(req.Endpoint) == "" {
		return fmt.Errorf("endpoint is required")
	}
	if req.Capacity <= 0 {
		req.Capacity = 1
	}
	now := time.Now().UTC()

	o.mu.Lock()
	defer o.mu.Unlock()
	o.nodes[req.NodeID] = &registeredNode{registration: req, lastSeen: now}
	return nil
}

func (o *Orchestrator) DeregisterNode(nodeID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.nodes, nodeID)
}

func (o *Orchestrator) ListNodes() []NodeSnapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return snapshotsFromMap(o.nodes)
}

func (o *Orchestrator) ExecuteTask(ctx context.Context, req TaskSubmissionRequest) (TaskSubmissionResponse, error) {
	if strings.TrimSpace(req.Objective) == "" {
		return TaskSubmissionResponse{}, fmt.Errorf("objective is required")
	}

	taskID := strings.TrimSpace(req.TaskID)
	if taskID == "" {
		taskID = o.newTaskID()
	}

	graph := req.Graph
	generatedGraph := false
	if graph == nil || len(graph.Subtasks) == 0 {
		auto := buildDefaultGraph(req.Objective)
		graph = &auto
		generatedGraph = true
	}
	if err := validateGraph(*graph); err != nil {
		return TaskSubmissionResponse{}, err
	}

	strategy := req.Strategy
	if strategy == "" {
		strategy = AssemblyDeterministic
	}
	subtaskTimeout := o.taskTimeout
	if req.TimeoutSeconds > 0 {
		subtaskTimeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	pending := map[string]SubtaskSpec{}
	for _, st := range graph.Subtasks {
		pending[st.ID] = st
	}
	completed := map[string]NodeOutputArtifact{}
	trace := make([]SubtaskTrace, 0, len(graph.Subtasks))

	for len(pending) > 0 {
		ready := readySubtasks(pending, completed)
		if len(ready) == 0 {
			return TaskSubmissionResponse{}, fmt.Errorf("task graph stalled; unresolved dependencies or cycle")
		}

		resultsCh := make(chan subtaskResult, len(ready))
		for _, st := range ready {
			deps := dependencySnapshot(completed)
			go func(spec SubtaskSpec, depCopy map[string]NodeOutputArtifact) {
				out, subTrace, err := o.runSubtask(ctx, taskID, req.Objective, spec, depCopy, subtaskTimeout)
				resultsCh <- subtaskResult{output: out, trace: subTrace, err: err}
			}(st, deps)
		}

		for i := 0; i < len(ready); i++ {
			res := <-resultsCh
			trace = append(trace, res.trace)
			if res.err != nil {
				return TaskSubmissionResponse{}, fmt.Errorf("subtask %s failed: %w", res.trace.SubtaskID, res.err)
			}
			completed[res.output.SubtaskID] = res.output
			delete(pending, res.output.SubtaskID)
		}
	}

	orderedOutputs := make([]NodeOutputArtifact, 0, len(graph.Subtasks))
	for _, st := range graph.Subtasks {
		orderedOutputs = append(orderedOutputs, completed[st.ID])
	}
	assembled, details, err := o.assembler.Assemble(strategy, orderedOutputs, graph.Subtasks, req.Objective)
	if err != nil {
		return TaskSubmissionResponse{}, err
	}

	return TaskSubmissionResponse{
		TaskID:          taskID,
		Objective:       req.Objective,
		Strategy:        strategy,
		Result:          assembled,
		Provenance:      orderedOutputs,
		SubtaskTrace:    trace,
		Graph:           *graph,
		GeneratedGraph:  generatedGraph,
		AssemblyDetails: details,
	}, nil
}

type subtaskResult struct {
	output NodeOutputArtifact
	trace  SubtaskTrace
	err    error
}

func (o *Orchestrator) runSubtask(ctx context.Context, taskID, objective string, st SubtaskSpec, deps map[string]NodeOutputArtifact, timeout time.Duration) (NodeOutputArtifact, SubtaskTrace, error) {
	attempts := st.MaxRetries
	if attempts <= 0 {
		attempts = o.defaultRetries
	}

	trace := SubtaskTrace{SubtaskID: st.ID, TaskType: st.Type, Status: "failed", Attempts: make([]AttemptTrace, 0, attempts)}
	if attempts <= 0 {
		return NodeOutputArtifact{}, trace, fmt.Errorf("retry budget must be positive")
	}

	skipped := map[string]bool{}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		node, err := o.pickNode(st, skipped)
		if err != nil {
			lastErr = err
			break
		}

		started := time.Now().UTC()
		attemptTrace := AttemptTrace{Attempt: attempt, NodeID: node.NodeID, StartedAt: started}
		o.incrementInFlight(node.NodeID, 1)
		out, err := o.invokeNode(ctx, node, taskID, objective, st, deps, timeout)
		o.incrementInFlight(node.NodeID, -1)
		attemptTrace.CompletedAt = time.Now().UTC()
		if err != nil {
			attemptTrace.Error = err.Error()
			trace.Attempts = append(trace.Attempts, attemptTrace)
			skipped[node.NodeID] = true
			if len(skipped) >= len(o.ListNodes()) {
				skipped = map[string]bool{}
			}
			lastErr = err
			continue
		}

		trace.Attempts = append(trace.Attempts, attemptTrace)
		trace.AssignedNodeID = node.NodeID
		trace.Status = "completed"
		if out.TaskID == "" {
			out.TaskID = taskID
		}
		if out.NodeID == "" {
			out.NodeID = node.NodeID
		}
		if out.SubtaskID == "" {
			out.SubtaskID = st.ID
		}
		if out.Metadata == nil {
			out.Metadata = map[string]string{}
		}
		if st.Type != "" {
			out.Metadata["task_type"] = st.Type
		}
		if out.Confidence <= 0 {
			out.Confidence = 0.5
		}
		if out.Confidence > 1 {
			out.Confidence = 1
		}
		return out, trace, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no available nodes")
	}
	return NodeOutputArtifact{}, trace, lastErr
}

func (o *Orchestrator) invokeNode(ctx context.Context, node NodeSnapshot, taskID, objective string, st SubtaskSpec, deps map[string]NodeOutputArtifact, timeout time.Duration) (NodeOutputArtifact, error) {
	depPayload := map[string]NodeOutputArtifact{}
	for _, dep := range st.DependsOn {
		if out, ok := deps[dep]; ok {
			depPayload[dep] = out
		}
	}

	payload := NodeTaskRequest{
		TaskID:         taskID,
		SubtaskID:      st.ID,
		Objective:      objective,
		Prompt:         st.Prompt,
		Type:           st.Type,
		Dependencies:   depPayload,
		TimeoutSeconds: int(timeout.Seconds()),
		Metadata:       st.Metadata,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return NodeOutputArtifact{}, err
	}

	endpoint := strings.TrimRight(node.Endpoint, "/") + "/v1/node/tasks"
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return NodeOutputArtifact{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(httpReq)
	if err != nil {
		return NodeOutputArtifact{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return NodeOutputArtifact{}, fmt.Errorf("node %s returned %d: %s", node.NodeID, resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var out NodeOutputArtifact
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return NodeOutputArtifact{}, err
	}
	if out.Result == nil {
		return NodeOutputArtifact{}, fmt.Errorf("node %s returned empty result", node.NodeID)
	}
	return out, nil
}

func (o *Orchestrator) pickNode(st SubtaskSpec, skipped map[string]bool) (NodeSnapshot, error) {
	nodes := o.ListNodes()
	if len(nodes) == 0 {
		return NodeSnapshot{}, fmt.Errorf("no registered nodes")
	}

	type candidate struct {
		node  NodeSnapshot
		score int
	}
	cands := make([]candidate, 0, len(nodes))
	for _, node := range nodes {
		score := 0
		if node.Capacity <= 0 {
			node.Capacity = 1
		}
		if node.InFlight >= node.Capacity {
			score -= 1000
		}
		score += (node.Capacity - node.InFlight) * 10
		if specializationMatch(st.Type, node.Specializations) {
			score += 100
		}
		if st.Type == "" && specializationMatch(st.Prompt, node.Specializations) {
			score += 40
		}
		cands = append(cands, candidate{node: node, score: score})
	}

	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].score != cands[j].score {
			return cands[i].score > cands[j].score
		}
		return cands[i].node.NodeID < cands[j].node.NodeID
	})

	for _, cand := range cands {
		if !skipped[cand.node.NodeID] {
			return cand.node, nil
		}
	}
	return cands[0].node, nil
}

func (o *Orchestrator) incrementInFlight(nodeID string, delta int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if node, ok := o.nodes[nodeID]; ok {
		node.inFlight += delta
		if node.inFlight < 0 {
			node.inFlight = 0
		}
		node.lastSeen = time.Now().UTC()
	}
}

func (o *Orchestrator) newTaskID() string {
	n := atomic.AddUint64(&o.seq, 1)
	return fmt.Sprintf("task-%d-%d", time.Now().UTC().UnixNano(), n)
}

func (o *Orchestrator) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "nodes": len(o.ListNodes())})
}

func (o *Orchestrator) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req NodeRegistrationRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := o.RegisterNode(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (o *Orchestrator) handleDeregisterNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req NodeDeregisterRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.NodeID) == "" {
		writeError(w, http.StatusBadRequest, "node_id is required")
		return
	}
	o.DeregisterNode(req.NodeID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deregistered"})
}

func (o *Orchestrator) handleListNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": o.ListNodes()})
}

func (o *Orchestrator) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req TaskSubmissionRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := o.ExecuteTask(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func snapshotsFromMap(nodes map[string]*registeredNode) []NodeSnapshot {
	out := make([]NodeSnapshot, 0, len(nodes))
	for id, node := range nodes {
		out = append(out, NodeSnapshot{
			NodeID:          id,
			Endpoint:        node.registration.Endpoint,
			Specializations: append([]string(nil), node.registration.Specializations...),
			Capacity:        node.registration.Capacity,
			InFlight:        node.inFlight,
			LastSeen:        node.lastSeen,
			Metadata:        node.registration.Metadata,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func buildDefaultGraph(objective string) TaskGraph {
	return TaskGraph{Subtasks: []SubtaskSpec{
		{
			ID:     "extract-requirements",
			Type:   "analysis",
			Prompt: fmt.Sprintf("Extract the key requirements, assumptions, and constraints for this objective:\n%s", objective),
		},
		{
			ID:        "propose-solution",
			Type:      "generation",
			DependsOn: []string{"extract-requirements"},
			Prompt:    fmt.Sprintf("Propose a concrete implementation plan for this objective:\n%s", objective),
		},
		{
			ID:        "critique-solution",
			Type:      "validation",
			DependsOn: []string{"propose-solution"},
			Prompt:    "Identify risks, missing edge cases, and failure modes in the proposed solution.",
		},
		{
			ID:        "finalize-response",
			Type:      "synthesis",
			DependsOn: []string{"extract-requirements", "propose-solution", "critique-solution"},
			Prompt:    "Produce the final consolidated answer using the upstream dependency outputs.",
		},
	}}
}

func validateGraph(g TaskGraph) error {
	if len(g.Subtasks) == 0 {
		return fmt.Errorf("graph must include at least one subtask")
	}
	byID := map[string]SubtaskSpec{}
	for _, st := range g.Subtasks {
		if strings.TrimSpace(st.ID) == "" {
			return fmt.Errorf("subtask id is required")
		}
		if _, exists := byID[st.ID]; exists {
			return fmt.Errorf("duplicate subtask id: %s", st.ID)
		}
		if strings.TrimSpace(st.Prompt) == "" {
			return fmt.Errorf("subtask %s must include prompt", st.ID)
		}
		byID[st.ID] = st
	}

	for _, st := range g.Subtasks {
		for _, dep := range st.DependsOn {
			if dep == st.ID {
				return fmt.Errorf("subtask %s cannot depend on itself", st.ID)
			}
			if _, ok := byID[dep]; !ok {
				return fmt.Errorf("subtask %s depends on unknown id %s", st.ID, dep)
			}
		}
	}

	state := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		if state[id] == 1 {
			return fmt.Errorf("cycle detected involving %s", id)
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		for _, dep := range byID[id].DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for _, st := range g.Subtasks {
		if err := visit(st.ID); err != nil {
			return err
		}
	}
	return nil
}

func readySubtasks(pending map[string]SubtaskSpec, completed map[string]NodeOutputArtifact) []SubtaskSpec {
	out := make([]SubtaskSpec, 0, len(pending))
	for _, st := range pending {
		ready := true
		for _, dep := range st.DependsOn {
			if _, ok := completed[dep]; !ok {
				ready = false
				break
			}
		}
		if ready {
			out = append(out, st)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func dependencySnapshot(in map[string]NodeOutputArtifact) map[string]NodeOutputArtifact {
	out := make(map[string]NodeOutputArtifact, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func specializationMatch(taskTypeOrPrompt string, specializations []string) bool {
	needle := strings.ToLower(strings.TrimSpace(taskTypeOrPrompt))
	if needle == "" {
		return false
	}
	for _, spec := range specializations {
		s := strings.ToLower(strings.TrimSpace(spec))
		if s == "" {
			continue
		}
		if s == needle || strings.Contains(needle, s) || strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

func decodeJSON(r io.Reader, target any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
