package swarm

import "testing"

func TestMajorityVote(t *testing.T) {
	a := NewAssembler(nil, "")
	result, details, err := a.Assemble(AssemblyMajorityVote, []NodeOutputArtifact{
		{SubtaskID: "a", NodeID: "n1", Result: "cat", Confidence: 0.8},
		{SubtaskID: "b", NodeID: "n2", Result: "dog", Confidence: 0.8},
		{SubtaskID: "c", NodeID: "n3", Result: "cat", Confidence: 0.8},
	}, nil, "")
	if err != nil {
		t.Fatalf("assemble failed: %v", err)
	}
	winner := result.(map[string]any)["winner"].(string)
	if winner != "cat" {
		t.Fatalf("expected cat, got %s", winner)
	}
	if details["winner_votes"].(int) != 2 {
		t.Fatalf("expected 2 votes, got %v", details["winner_votes"])
	}
}

func TestWeightedAverage(t *testing.T) {
	a := NewAssembler(nil, "")
	result, _, err := a.Assemble(AssemblyWeightedAvg, []NodeOutputArtifact{
		{SubtaskID: "a", NodeID: "n1", Result: 10.0, Confidence: 1.0},
		{SubtaskID: "b", NodeID: "n2", Result: 20.0, Confidence: 2.0},
	}, nil, "")
	if err != nil {
		t.Fatalf("assemble failed: %v", err)
	}
	value := result.(map[string]any)["value"].(float64)
	if value != 16.666667 {
		t.Fatalf("unexpected weighted average: %v", value)
	}
}
