package swarm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExecuteTaskDistributesAcrossSpecializedNodes(t *testing.T) {
	nodeA := newFakeNodeServer(t, "node-a", false)
	defer nodeA.Close()
	nodeB := newFakeNodeServer(t, "node-b", false)
	defer nodeB.Close()
	nodeC := newFakeNodeServer(t, "node-c", false)
	defer nodeC.Close()

	o := NewOrchestrator(2*time.Second, 2, nil, "")
	mustNoErr(t, o.RegisterNode(NodeRegistrationRequest{NodeID: "node-a", Endpoint: nodeA.URL, Specializations: []string{"analysis"}, Capacity: 1}))
	mustNoErr(t, o.RegisterNode(NodeRegistrationRequest{NodeID: "node-b", Endpoint: nodeB.URL, Specializations: []string{"generation"}, Capacity: 1}))
	mustNoErr(t, o.RegisterNode(NodeRegistrationRequest{NodeID: "node-c", Endpoint: nodeC.URL, Specializations: []string{"validation"}, Capacity: 1}))

	resp, err := o.ExecuteTask(t.Context(), TaskSubmissionRequest{
		Objective: "Create a short architecture recommendation",
		Graph: &TaskGraph{Subtasks: []SubtaskSpec{
			{ID: "s1", Type: "analysis", Prompt: "Analyze"},
			{ID: "s2", Type: "generation", Prompt: "Generate", DependsOn: []string{"s1"}},
			{ID: "s3", Type: "validation", Prompt: "Validate", DependsOn: []string{"s2"}},
		}},
		Strategy: AssemblyDeterministic,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if len(resp.Provenance) != 3 {
		t.Fatalf("expected 3 provenance entries, got %d", len(resp.Provenance))
	}
	seen := map[string]bool{}
	for _, out := range resp.Provenance {
		seen[out.NodeID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected results from 3 distinct nodes, got %d (%v)", len(seen), seen)
	}
}

func TestExecuteTaskReassignsOnNodeFailure(t *testing.T) {
	bad := newFakeNodeServer(t, "node-a", true)
	defer bad.Close()
	good := newFakeNodeServer(t, "node-b", false)
	defer good.Close()

	o := NewOrchestrator(2*time.Second, 2, nil, "")
	mustNoErr(t, o.RegisterNode(NodeRegistrationRequest{NodeID: "node-a", Endpoint: bad.URL, Specializations: []string{"analysis"}, Capacity: 1}))
	mustNoErr(t, o.RegisterNode(NodeRegistrationRequest{NodeID: "node-b", Endpoint: good.URL, Specializations: []string{"analysis"}, Capacity: 1}))

	resp, err := o.ExecuteTask(t.Context(), TaskSubmissionRequest{
		Objective: "Retry behavior",
		Graph: &TaskGraph{Subtasks: []SubtaskSpec{
			{ID: "only", Type: "analysis", Prompt: "Analyze", MaxRetries: 2},
		}},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := resp.Provenance[0].NodeID; got != "node-b" {
		t.Fatalf("expected task to finish on node-b, got %s", got)
	}
	if len(resp.SubtaskTrace) != 1 || len(resp.SubtaskTrace[0].Attempts) < 2 {
		t.Fatalf("expected at least 2 attempts, got %#v", resp.SubtaskTrace)
	}
	if resp.SubtaskTrace[0].Attempts[0].Error == "" {
		t.Fatalf("expected first attempt to fail")
	}
}

func newFakeNodeServer(t *testing.T, nodeID string, fail bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/node/tasks" {
			http.NotFound(w, r)
			return
		}
		if fail {
			http.Error(w, "intentional failure", http.StatusInternalServerError)
			return
		}
		var req NodeTaskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(NodeOutputArtifact{
			TaskID:     req.TaskID,
			NodeID:     nodeID,
			SubtaskID:  req.SubtaskID,
			Result:     fmt.Sprintf("%s:%s", nodeID, req.SubtaskID),
			Confidence: 0.9,
			Metadata:   map[string]string{"task_type": req.Type},
		})
	}))
}

func mustNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
