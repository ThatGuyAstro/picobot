package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/local/picobot/internal/agent"
	"github.com/local/picobot/internal/chat"
	"github.com/local/picobot/internal/config"
	"github.com/local/picobot/internal/providers"
	"github.com/local/picobot/internal/swarm"
)

func addSwarmCommand(rootCmd *cobra.Command) {
	swarmCmd := &cobra.Command{
		Use:   "swarm",
		Short: "Run distributed swarm components (orchestrator + nodes)",
	}

	orchestratorCmd := buildOrchestratorCommand()
	nodeCmd := buildNodeCommand()
	submitCmd := buildSubmitCommand()

	swarmCmd.AddCommand(orchestratorCmd, nodeCmd, submitCmd)
	rootCmd.AddCommand(swarmCmd)
}

func buildOrchestratorCommand() *cobra.Command {
	var listenAddr string
	var taskTimeout time.Duration
	var retries int
	var modelFlag string

	cmd := &cobra.Command{
		Use:   "orchestrator",
		Short: "Run the swarm orchestrator service",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, _ := config.LoadConfig()
			provider := providers.NewProviderFromConfig(cfg)
			model := modelFlag
			if model == "" && cfg.Agents.Defaults.Model != "" {
				model = cfg.Agents.Defaults.Model
			}
			if model == "" {
				model = provider.GetDefaultModel()
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			orch := swarm.NewOrchestrator(taskTimeout, retries, provider, model)
			fmt.Fprintf(cmd.OutOrStdout(), "swarm orchestrator listening on %s\n", listenAddr)
			if err := orch.Run(ctx, listenAddr); err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "orchestrator error:", err)
			}
		},
	}

	cmd.Flags().StringVar(&listenAddr, "listen", ":8080", "HTTP listen address")
	cmd.Flags().DurationVar(&taskTimeout, "task-timeout", 45*time.Second, "Per-subtask timeout")
	cmd.Flags().IntVar(&retries, "retries", 3, "Max dispatch attempts per subtask")
	cmd.Flags().StringVarP(&modelFlag, "model", "M", "", "Model for LLM-based synthesis")
	return cmd
}

func buildNodeCommand() *cobra.Command {
	var listenAddr string
	var orchestratorURL string
	var publicURL string
	var nodeID string
	var workspace string
	var specializations string
	var capacity int
	var timeout time.Duration
	var modelFlag string
	var stdinMode bool

	cmd := &cobra.Command{
		Use:   "node",
		Short: "Run a swarm compute node (HTTP server or stdin mode)",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, _ := config.LoadConfig()
			provider := providers.NewProviderFromConfig(cfg)
			model := modelFlag
			if model == "" && cfg.Agents.Defaults.Model != "" {
				model = cfg.Agents.Defaults.Model
			}
			if model == "" {
				model = provider.GetDefaultModel()
			}

			nodeID = resolveNodeID(nodeID)
			if workspace == "" {
				workspace = filepath.Join(defaultPicobotHome(), "swarm", nodeID, "workspace")
			}
			workspace = expandHome(workspace)
			if err := config.InitializeWorkspace(workspace); err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "failed to initialize workspace:", err)
				return
			}

			maxIter := cfg.Agents.Defaults.MaxToolIterations
			if maxIter <= 0 {
				maxIter = 100
			}
			hub := chat.NewHub(50)
			ag := agent.NewAgentLoop(hub, provider, model, maxIter, workspace, nil)

			node := swarm.NewNodeService(swarm.NodeOptions{
				NodeID:          nodeID,
				ListenAddr:      listenAddr,
				PublicURL:       publicURL,
				OrchestratorURL: orchestratorURL,
				Specializations: splitCSV(specializations),
				Capacity:        capacity,
				TaskTimeout:     timeout,
			}, ag)

			if stdinMode {
				if err := node.ProcessFromReader(os.Stdin, cmd.OutOrStdout()); err != nil {
					fmt.Fprintln(cmd.ErrOrStderr(), "stdin task failed:", err)
					os.Exit(1)
				}
				return
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()
			fmt.Fprintf(cmd.OutOrStdout(), "swarm node %s listening on %s\n", nodeID, listenAddr)
			if err := node.Start(ctx); err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "node error:", err)
			}
		},
	}

	cmd.Flags().StringVar(&listenAddr, "listen", ":8090", "HTTP listen address")
	cmd.Flags().StringVar(&orchestratorURL, "orchestrator-url", "", "Orchestrator base URL for register/deregister")
	cmd.Flags().StringVar(&publicURL, "public-url", "", "Public node URL advertised to orchestrator")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "Node identity (defaults to hostname)")
	cmd.Flags().StringVar(&workspace, "workspace", "", "Node workspace directory (defaults to ~/.picobot/swarm/<node>/workspace)")
	cmd.Flags().StringVar(&specializations, "specializations", "analysis,generation,validation,synthesis", "Comma-separated specialization tags")
	cmd.Flags().IntVar(&capacity, "capacity", 1, "Max concurrent subtask slots")
	cmd.Flags().DurationVar(&timeout, "task-timeout", 60*time.Second, "Subtask execution timeout")
	cmd.Flags().StringVarP(&modelFlag, "model", "M", "", "Model to use")
	cmd.Flags().BoolVar(&stdinMode, "stdin", false, "Process one NodeTaskRequest JSON from stdin and exit")

	return cmd
}

func buildSubmitCommand() *cobra.Command {
	var orchestratorURL string
	var objective string
	var requestFile string
	var taskID string
	var strategy string
	var timeoutSeconds int

	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a high-order task to the orchestrator",
		Run: func(cmd *cobra.Command, args []string) {
			req := swarm.TaskSubmissionRequest{}
			if requestFile != "" {
				data, err := os.ReadFile(requestFile)
				if err != nil {
					fmt.Fprintln(cmd.ErrOrStderr(), "read request file failed:", err)
					return
				}
				if err := json.Unmarshal(data, &req); err != nil {
					fmt.Fprintln(cmd.ErrOrStderr(), "invalid request json:", err)
					return
				}
			}
			if objective != "" {
				req.Objective = objective
			}
			if taskID != "" {
				req.TaskID = taskID
			}
			if strategy != "" {
				req.Strategy = swarm.AssemblyStrategy(strategy)
			}
			if timeoutSeconds > 0 {
				req.TimeoutSeconds = timeoutSeconds
			}
			if strings.TrimSpace(req.Objective) == "" {
				fmt.Fprintln(cmd.ErrOrStderr(), "objective is required (via --objective or request file)")
				return
			}

			body, _ := json.Marshal(req)
			url := strings.TrimRight(orchestratorURL, "/") + "/v1/tasks"
			httpReq, err := http.NewRequestWithContext(cmd.Context(), http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "create request failed:", err)
				return
			}
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(httpReq)
			if err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "submit failed:", err)
				return
			}
			defer resp.Body.Close()

			payload, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				fmt.Fprintf(cmd.ErrOrStderr(), "orchestrator returned %d: %s\n", resp.StatusCode, strings.TrimSpace(string(payload)))
				return
			}

			var pretty bytes.Buffer
			if err := json.Indent(&pretty, payload, "", "  "); err == nil {
				fmt.Fprintln(cmd.OutOrStdout(), pretty.String())
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), string(payload))
		},
	}

	cmd.Flags().StringVar(&orchestratorURL, "orchestrator-url", "http://localhost:8080", "Orchestrator base URL")
	cmd.Flags().StringVar(&objective, "objective", "", "High-order task objective")
	cmd.Flags().StringVar(&requestFile, "file", "", "Path to a full TaskSubmissionRequest JSON file")
	cmd.Flags().StringVar(&taskID, "task-id", "", "Optional task ID")
	cmd.Flags().StringVar(&strategy, "strategy", "", "Assembly strategy: deterministic|majority_vote|weighted_average|llm_synthesis")
	cmd.Flags().IntVar(&timeoutSeconds, "timeout-seconds", 0, "Per-subtask timeout seconds")
	return cmd
}

func defaultPicobotHome() string {
	if home := os.Getenv("PICOBOT_HOME"); strings.TrimSpace(home) != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".picobot"
	}
	return filepath.Join(home, ".picobot")
}

func resolveNodeID(explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return explicit
	}
	h, err := os.Hostname()
	if err == nil && strings.TrimSpace(h) != "" {
		return h
	}
	return fmt.Sprintf("node-%d", time.Now().UTC().UnixNano())
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		if home != "" {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}
