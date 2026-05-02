package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"

	agentclient "mytonprovider-backend/pkg/agentClient"
	agentserver "mytonprovider-backend/pkg/agentServer"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg := loadConfig()

	logLevel := slog.LevelInfo
	if level, ok := logLevels[cfg.LogLevel]; ok {
		logLevel = level
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	// Initialise ADNL/DHT clients.
	dhtClient, providerClient, err := newProviderClient(context.Background(), cfg)
	if err != nil {
		logger.Error("failed to create provider client", slog.String("error", err.Error()))
		return err
	}

	// Build agent server.
	workers := agentserver.NewWorkers(providerClient, dhtClient, cfg.Key, logger)
	h := agentserver.New(workers, cfg.InternalToken, logger)

	app := fiber.New()
	app.Get("/health", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })
	h.RegisterRoutes(app)

	// Register with coordinator and start heartbeat.
	agentURL := fmt.Sprintf("http://0.0.0.0:%s", cfg.Port)
	agentID, err := registerWithCoordinator(cfg, agentURL)
	if err != nil {
		logger.Error("failed to register with coordinator", slog.String("error", err.Error()))
		return err
	}
	logger.Info("registered with coordinator", "id", agentID, "coordinator", cfg.CoordinatorURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go heartbeatLoop(ctx, cfg, agentID, logger)

	go func() {
		if err := app.Listen(":" + cfg.Port); err != nil {
			logger.Error("agent server error", slog.String("error", err.Error()))
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	cancel()
	return app.ShutdownWithTimeout(5 * time.Second)
}

func newProviderClient(ctx context.Context, cfg *Config) (*dht.Client, *transport.Client, error) {
	lsCfg, err := liteclient.GetConfigFromUrl(ctx, cfg.TONConfigURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get liteclient config: %w", err)
	}

	_, dhtAdnlKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate DHT ADNL key: %w", err)
	}

	dl, err := adnl.DefaultListener("0.0.0.0:" + cfg.ADNLPort)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create default listener: %w", err)
	}

	netMgr := adnl.NewMultiNetReader(dl)

	dhtGate := adnl.NewGatewayWithNetManager(dhtAdnlKey, netMgr)
	if err = dhtGate.StartClient(); err != nil {
		return nil, nil, fmt.Errorf("failed to start DHT gateway: %w", err)
	}

	dc, err := dht.NewClientFromConfig(dhtGate, lsCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create DHT client: %w", err)
	}

	gateProvider := adnl.NewGatewayWithNetManager(cfg.Key, netMgr)
	if err = gateProvider.StartClient(); err != nil {
		return nil, nil, fmt.Errorf("failed to start ADNL gateway for provider: %w", err)
	}

	tc := transport.NewClient(gateProvider, dc)
	return dc, tc, nil
}

func registerWithCoordinator(cfg *Config, agentURL string) (string, error) {
	body, err := json.Marshal(agentclient.RegisterRequest{
		URL:      agentURL,
		ADNLPort: cfg.ADNLPort,
	})
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, cfg.CoordinatorURL+"/internal/v1/agents", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", cfg.InternalToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("coordinator returned status %d", resp.StatusCode)
	}

	var result agentclient.RegisterResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	return result.ID, nil
}

func heartbeatLoop(ctx context.Context, cfg *Config, agentID string, logger *slog.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	url := fmt.Sprintf("%s/internal/v1/agents/%s/heartbeat", cfg.CoordinatorURL, agentID)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
			if err != nil {
				continue
			}
			req.Header.Set("X-Internal-Token", cfg.InternalToken)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				logger.Warn("heartbeat failed", "error", err.Error())
				continue
			}
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				logger.Warn("agent evicted from coordinator, re-registering")
				newID, rErr := registerWithCoordinator(cfg, fmt.Sprintf("http://0.0.0.0:%s", cfg.Port))
				if rErr != nil {
					logger.Error("re-registration failed", "error", rErr.Error())
					continue
				}
				agentID = newID
				url = fmt.Sprintf("%s/internal/v1/agents/%s/heartbeat", cfg.CoordinatorURL, agentID)
				logger.Info("re-registered with coordinator", "id", agentID)
			}
		}
	}
}
