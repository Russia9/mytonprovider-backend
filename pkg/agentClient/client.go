package agentclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	agentregistry "mytonprovider-backend/pkg/agentRegistry"
	"mytonprovider-backend/pkg/constants"
	"mytonprovider-backend/pkg/models/db"
)

var ErrNoAgents = errors.New("no agents registered")
var ErrAllAgentsFailed = errors.New("all agents failed")

// --- Wire types ---

type RegisterRequest struct {
	URL      string `json:"url"`
	ADNLPort string `json:"adnl_port"`
}

type RegisterResponse struct {
	ID string `json:"id"`
}

type PingProvidersRequest struct {
	Providers []PingProvider `json:"providers"`
}

type PingProvider struct {
	Pubkey string `json:"pubkey"`
}

type PingProvidersResponse struct {
	Results []PingResult `json:"results"`
}

type PingResult struct {
	Pubkey       string `json:"pubkey"`
	IsOnline     bool   `json:"is_online"`
	RatePerMBDay int64  `json:"rate_per_mb_day"`
	MinBounty    int64  `json:"min_bounty"`
	MinSpan      uint32 `json:"min_span"`
	MaxSpan      uint32 `json:"max_span"`
}

type CheckProofsRequest struct {
	Providers []ProofProvider `json:"providers"`
}

type ProofProvider struct {
	Pubkey          string          `json:"pubkey"`
	ProviderAddress string          `json:"provider_address"`
	Contracts       []ProofContract `json:"contracts"`
}

type ProofContract struct {
	Address string `json:"address"`
	BagID   string `json:"bag_id"`
}

type CheckProofsResponse struct {
	IPs          []db.ProviderIP          `json:"ips"`
	ProofResults []db.ContractProofsCheck `json:"proof_results"`
}

// ReasonCode alias so callers don't need to import constants directly.
type ReasonCode = constants.ReasonCode

// --- Client ---

type Client struct {
	httpClient    *http.Client
	internalToken string
	logger        *slog.Logger
}

func New(internalToken string, logger *slog.Logger) *Client {
	return &Client{
		httpClient:    &http.Client{},
		internalToken: internalToken,
		logger:        logger,
	}
}

// DistributePing splits pubkeys evenly across agents, dispatches in parallel, then retries
// any failed chunks on the agents that succeeded (pass 2). Returns ErrNoAgents if the
// registry is empty, ErrAllAgentsFailed if every agent failed in pass 1.
// If pass 2 retries also fail the coordinator logs a warning and accepts partial results —
// missed providers are picked up on the next 1-minute cycle.
func (c *Client) DistributePing(
	ctx context.Context,
	agents []*agentregistry.Agent,
	pubkeys []string,
) (statuses []db.ProviderStatusUpdate, updates []db.ProviderUpdate, err error) {
	if len(agents) == 0 {
		return nil, nil, ErrNoAgents
	}

	chunks := splitStrings(pubkeys, len(agents))

	type pass1Result struct {
		resp *PingProvidersResponse
		err  error
	}
	results := make([]pass1Result, len(chunks))

	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, ch []string, agent *agentregistry.Agent) {
			defer wg.Done()
			tCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			r, rErr := c.callPingProviders(tCtx, agent, ch)
			results[idx] = pass1Result{resp: r, err: rErr}
		}(i, chunk, agents[i])
	}
	wg.Wait()

	var successAgents []*agentregistry.Agent
	var failedChunks [][]string
	for i, r := range results {
		if r.err != nil {
			failedChunks = append(failedChunks, chunks[i])
		} else {
			successAgents = append(successAgents, agents[i])
			statuses, updates = appendPingResults(statuses, updates, r.resp.Results)
		}
	}

	if len(successAgents) == 0 {
		return nil, nil, ErrAllAgentsFailed
	}

	// Pass 2: retry failed chunks on agents that succeeded.
	for i, chunk := range failedChunks {
		agent := successAgents[i%len(successAgents)]
		tCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		r, rErr := c.callPingProviders(tCtx, agent, chunk)
		cancel()
		if rErr != nil {
			c.logger.Warn("ping retry failed, affected providers skip this status cycle",
				slog.Int("providers_count", len(chunk)),
				slog.String("agent", agent.URL),
				slog.String("error", rErr.Error()),
			)
			continue
		}
		statuses, updates = appendPingResults(statuses, updates, r.Results)
	}

	return statuses, updates, nil
}

// DistributeProofs splits provider groups evenly across agents, dispatches in parallel, then
// retries failed chunks on successful agents (pass 2). Returns ErrNoAgents / ErrAllAgentsFailed
// for hard failures. If pass 2 retries fail, partial results are accepted (caller logs WARN).
func (c *Client) DistributeProofs(
	ctx context.Context,
	agents []*agentregistry.Agent,
	providers []ProofProvider,
) (ips []db.ProviderIP, proofResults []db.ContractProofsCheck, err error) {
	if len(agents) == 0 {
		return nil, nil, ErrNoAgents
	}

	chunks := splitProviders(providers, len(agents))

	type pass1Result struct {
		resp *CheckProofsResponse
		err  error
	}
	results := make([]pass1Result, len(chunks))

	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, ch []ProofProvider, agent *agentregistry.Agent) {
			defer wg.Done()
			tCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			defer cancel()
			r, rErr := c.callCheckProofs(tCtx, agent, ch)
			results[idx] = pass1Result{resp: r, err: rErr}
		}(i, chunk, agents[i])
	}
	wg.Wait()

	var successAgents []*agentregistry.Agent
	var failedChunks [][]ProofProvider
	for i, r := range results {
		if r.err != nil {
			failedChunks = append(failedChunks, chunks[i])
		} else {
			successAgents = append(successAgents, agents[i])
			ips = append(ips, r.resp.IPs...)
			proofResults = append(proofResults, r.resp.ProofResults...)
		}
	}

	if len(successAgents) == 0 {
		return nil, nil, ErrAllAgentsFailed
	}

	// Pass 2: retry failed chunks on agents that succeeded.
	for i, chunk := range failedChunks {
		agent := successAgents[i%len(successAgents)]
		tCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		r, rErr := c.callCheckProofs(tCtx, agent, chunk)
		cancel()
		if rErr != nil {
			c.logger.Warn("proof check retry failed, affected contracts will not be updated this cycle (old reason codes persist up to 24h)",
				slog.Int("providers_count", len(chunk)),
				slog.String("agent", agent.URL),
				slog.String("error", rErr.Error()),
			)
			continue
		}
		ips = append(ips, r.IPs...)
		proofResults = append(proofResults, r.ProofResults...)
	}

	return ips, proofResults, nil
}

// --- internal helpers ---

func (c *Client) callPingProviders(ctx context.Context, agent *agentregistry.Agent, pubkeys []string) (*PingProvidersResponse, error) {
	providers := make([]PingProvider, len(pubkeys))
	for i, pk := range pubkeys {
		providers[i] = PingProvider{Pubkey: pk}
	}
	var resp PingProvidersResponse
	if err := c.post(ctx, agent.URL+"/internal/v1/workers/ping-providers", PingProvidersRequest{Providers: providers}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) callCheckProofs(ctx context.Context, agent *agentregistry.Agent, providers []ProofProvider) (*CheckProofsResponse, error) {
	var resp CheckProofsResponse
	if err := c.post(ctx, agent.URL+"/internal/v1/workers/check-proofs", CheckProofsRequest{Providers: providers}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) post(ctx context.Context, url string, body, out interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", c.internalToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func appendPingResults(statuses []db.ProviderStatusUpdate, updates []db.ProviderUpdate, results []PingResult) ([]db.ProviderStatusUpdate, []db.ProviderUpdate) {
	for _, r := range results {
		statuses = append(statuses, db.ProviderStatusUpdate{
			Pubkey:   r.Pubkey,
			IsOnline: r.IsOnline,
		})
		if r.IsOnline {
			updates = append(updates, db.ProviderUpdate{
				Pubkey:       r.Pubkey,
				RatePerMBDay: r.RatePerMBDay,
				MinBounty:    r.MinBounty,
				MinSpan:      r.MinSpan,
				MaxSpan:      r.MaxSpan,
			})
		}
	}
	return statuses, updates
}

func splitStrings(s []string, n int) [][]string {
	if n <= 0 || len(s) == 0 {
		return [][]string{s}
	}
	n = min(n, len(s))
	chunks := make([][]string, n)
	for i, v := range s {
		chunks[i%n] = append(chunks[i%n], v)
	}
	return chunks
}

func splitProviders(s []ProofProvider, n int) [][]ProofProvider {
	if n <= 0 || len(s) == 0 {
		return [][]ProofProvider{s}
	}
	n = min(n, len(s))
	chunks := make([][]ProofProvider, n)
	for i, v := range s {
		chunks[i%n] = append(chunks[i%n], v)
	}
	return chunks
}
