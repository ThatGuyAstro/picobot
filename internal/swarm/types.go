package swarm

import "time"

// AssemblyStrategy controls how subtask outputs are merged.
type AssemblyStrategy string

const (
	AssemblyDeterministic AssemblyStrategy = "deterministic"
	AssemblyMajorityVote  AssemblyStrategy = "majority_vote"
	AssemblyWeightedAvg   AssemblyStrategy = "weighted_average"
	AssemblyLLMSynthesis  AssemblyStrategy = "llm_synthesis"
)

// TaskSubmissionRequest is the high-order task contract accepted by the orchestrator.
type TaskSubmissionRequest struct {
	TaskID         string            `json:"task_id,omitempty"`
	Objective      string            `json:"objective"`
	Graph          *TaskGraph        `json:"graph,omitempty"`
	Strategy       AssemblyStrategy  `json:"strategy,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// TaskGraph is a DAG of subtasks.
type TaskGraph struct {
	Subtasks []SubtaskSpec `json:"subtasks"`
}

// SubtaskSpec defines one node-executable unit in the task graph.
type SubtaskSpec struct {
	ID         string            `json:"id"`
	Prompt     string            `json:"prompt"`
	Type       string            `json:"type,omitempty"`
	DependsOn  []string          `json:"depends_on,omitempty"`
	Weight     float64           `json:"weight,omitempty"`
	MaxRetries int               `json:"max_retries,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// NodeTaskRequest is sent from orchestrator to node.
type NodeTaskRequest struct {
	TaskID         string                        `json:"task_id"`
	SubtaskID      string                        `json:"subtask_id"`
	Objective      string                        `json:"objective"`
	Prompt         string                        `json:"prompt"`
	Type           string                        `json:"type,omitempty"`
	Dependencies   map[string]NodeOutputArtifact `json:"dependencies,omitempty"`
	TimeoutSeconds int                           `json:"timeout_seconds,omitempty"`
	Metadata       map[string]string             `json:"metadata,omitempty"`
}

// NodeOutputArtifact is the standardized subtask output schema.
type NodeOutputArtifact struct {
	TaskID      string            `json:"task_id"`
	NodeID      string            `json:"node_id"`
	SubtaskID   string            `json:"subtask_id"`
	Result      any               `json:"result"`
	Confidence  float64           `json:"confidence"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	StartedAt   time.Time         `json:"started_at,omitempty"`
	CompletedAt time.Time         `json:"completed_at,omitempty"`
}

// NodeRegistrationRequest is sent by nodes on startup.
type NodeRegistrationRequest struct {
	NodeID          string            `json:"node_id"`
	Endpoint        string            `json:"endpoint"`
	Specializations []string          `json:"specializations,omitempty"`
	Capacity        int               `json:"capacity,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// NodeDeregisterRequest is sent by nodes on shutdown.
type NodeDeregisterRequest struct {
	NodeID string `json:"node_id"`
}

// NodeSnapshot is exposed by the orchestrator for diagnostics.
type NodeSnapshot struct {
	NodeID          string            `json:"node_id"`
	Endpoint        string            `json:"endpoint"`
	Specializations []string          `json:"specializations,omitempty"`
	Capacity        int               `json:"capacity"`
	InFlight        int               `json:"in_flight"`
	LastSeen        time.Time         `json:"last_seen"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// TaskSubmissionResponse is the assembled high-order response with provenance.
type TaskSubmissionResponse struct {
	TaskID          string               `json:"task_id"`
	Objective       string               `json:"objective"`
	Strategy        AssemblyStrategy     `json:"strategy"`
	Result          any                  `json:"result"`
	Provenance      []NodeOutputArtifact `json:"provenance"`
	SubtaskTrace    []SubtaskTrace       `json:"subtask_trace"`
	Graph           TaskGraph            `json:"graph"`
	GeneratedGraph  bool                 `json:"generated_graph"`
	AssemblyDetails map[string]any       `json:"assembly_details,omitempty"`
}

// SubtaskTrace captures scheduling and retry history.
type SubtaskTrace struct {
	SubtaskID      string         `json:"subtask_id"`
	TaskType       string         `json:"task_type,omitempty"`
	AssignedNodeID string         `json:"assigned_node_id,omitempty"`
	Status         string         `json:"status"`
	Attempts       []AttemptTrace `json:"attempts"`
}

// AttemptTrace records one node attempt.
type AttemptTrace struct {
	Attempt     int       `json:"attempt"`
	NodeID      string    `json:"node_id"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	Error       string    `json:"error,omitempty"`
}
