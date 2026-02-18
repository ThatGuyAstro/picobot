package swarm

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/local/picobot/internal/providers"
)

// Assembler merges node-level artifacts into a final task result.
type Assembler struct {
	provider providers.LLMProvider
	model    string
}

func NewAssembler(provider providers.LLMProvider, model string) *Assembler {
	return &Assembler{provider: provider, model: model}
}

func (a *Assembler) Assemble(strategy AssemblyStrategy, outputs []NodeOutputArtifact, ordered []SubtaskSpec, objective string) (any, map[string]any, error) {
	if len(outputs) == 0 {
		return nil, nil, fmt.Errorf("cannot assemble empty output set")
	}

	switch strategy {
	case AssemblyMajorityVote:
		result, details := majorityVote(outputs)
		return result, details, nil
	case AssemblyWeightedAvg:
		result, details, err := weightedAverage(outputs)
		return result, details, err
	case AssemblyLLMSynthesis:
		result, details := a.llmSynthesis(outputs, objective)
		return result, details, nil
	case "", AssemblyDeterministic:
		fallthrough
	default:
		result, details := deterministicMerge(outputs, ordered)
		return result, details, nil
	}
}

func majorityVote(outputs []NodeOutputArtifact) (map[string]any, map[string]any) {
	counts := map[string]int{}
	winning := ""
	winnerCount := 0
	for _, out := range outputs {
		key := renderResult(out.Result)
		counts[key]++
		if counts[key] > winnerCount || (counts[key] == winnerCount && (winning == "" || key < winning)) {
			winnerCount = counts[key]
			winning = key
		}
	}

	return map[string]any{"winner": winning}, map[string]any{
		"method":       string(AssemblyMajorityVote),
		"winner":       winning,
		"winner_votes": winnerCount,
		"vote_counts":  counts,
	}
}

func weightedAverage(outputs []NodeOutputArtifact) (map[string]any, map[string]any, error) {
	var weightedSum float64
	var weightSum float64
	inputs := make([]map[string]any, 0, len(outputs))

	for _, out := range outputs {
		value, ok := parseNumeric(out.Result)
		if !ok {
			return nil, nil, fmt.Errorf("weighted_average requires numeric results (subtask=%s)", out.SubtaskID)
		}
		weight := out.Confidence
		if weight <= 0 {
			weight = 1
		}
		weightedSum += value * weight
		weightSum += weight
		inputs = append(inputs, map[string]any{
			"subtask_id": out.SubtaskID,
			"node_id":    out.NodeID,
			"value":      value,
			"weight":     weight,
		})
	}
	if weightSum == 0 {
		return nil, nil, fmt.Errorf("weighted_average produced zero total weight")
	}

	avg := weightedSum / weightSum
	avg = math.Round(avg*1e6) / 1e6
	return map[string]any{"value": avg}, map[string]any{
		"method":          string(AssemblyWeightedAvg),
		"weighted_sum":    weightedSum,
		"total_weight":    weightSum,
		"resolved_inputs": inputs,
	}, nil
}

func (a *Assembler) llmSynthesis(outputs []NodeOutputArtifact, objective string) (any, map[string]any) {
	fallback, fallbackDetails := deterministicMerge(outputs, nil)
	if a.provider == nil {
		fallbackDetails["method"] = string(AssemblyLLMSynthesis)
		fallbackDetails["fallback_reason"] = "no provider configured"
		return fallback, fallbackDetails
	}

	payload, _ := json.MarshalIndent(outputs, "", "  ")
	prompt := fmt.Sprintf("Synthesize a coherent final result for this objective: %s\n\nSubtask outputs (JSON):\n%s\n\nReturn concise final result only.", objective, string(payload))

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	resp, err := a.provider.Chat(ctx, []providers.Message{{Role: "user", Content: prompt}}, nil, a.model)
	if err != nil || strings.TrimSpace(resp.Content) == "" {
		fallbackDetails["method"] = string(AssemblyLLMSynthesis)
		if err != nil {
			fallbackDetails["fallback_reason"] = err.Error()
		} else {
			fallbackDetails["fallback_reason"] = "empty synthesis response"
		}
		return fallback, fallbackDetails
	}

	return map[string]any{"summary": strings.TrimSpace(resp.Content)}, map[string]any{
		"method": string(AssemblyLLMSynthesis),
		"model":  a.model,
	}
}

func deterministicMerge(outputs []NodeOutputArtifact, ordered []SubtaskSpec) (map[string]any, map[string]any) {
	index := map[string]int{}
	for i, st := range ordered {
		index[st.ID] = i
	}
	copies := make([]NodeOutputArtifact, len(outputs))
	copy(copies, outputs)
	sort.SliceStable(copies, func(i, j int) bool {
		ii, iok := index[copies[i].SubtaskID]
		jj, jok := index[copies[j].SubtaskID]
		if iok && jok && ii != jj {
			return ii < jj
		}
		if copies[i].SubtaskID != copies[j].SubtaskID {
			return copies[i].SubtaskID < copies[j].SubtaskID
		}
		return copies[i].NodeID < copies[j].NodeID
	})

	merged := make([]map[string]any, 0, len(copies))
	for _, out := range copies {
		merged = append(merged, map[string]any{
			"subtask_id": out.SubtaskID,
			"node_id":    out.NodeID,
			"result":     out.Result,
			"confidence": out.Confidence,
			"metadata":   out.Metadata,
		})
	}

	return map[string]any{"items": merged}, map[string]any{
		"method": string(AssemblyDeterministic),
		"count":  len(merged),
	}
}

func parseNumeric(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func renderResult(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
