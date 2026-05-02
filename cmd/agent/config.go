package main

import (
	"crypto/ed25519"
	"log"
	"log/slog"

	"github.com/caarlos0/env/v11"
)

var logLevels = map[uint8]slog.Level{
	0: slog.LevelDebug,
	1: slog.LevelInfo,
	2: slog.LevelWarn,
	3: slog.LevelError,
}

type Config struct {
	CoordinatorURL string             `env:"COORDINATOR_URL" required:"true"`
	InternalToken  string             `env:"INTERNAL_TOKEN" envDefault:""`
	Host           string             `env:"AGENT_HOST" envDefault:"0.0.0.0"`
	Port           string             `env:"AGENT_PORT" envDefault:"9091"`
	ADNLPort       string             `env:"AGENT_ADNL_PORT" envDefault:"16168"`
	Key            ed25519.PrivateKey `env:"SYSTEM_KEY" required:"false"`
	TONConfigURL   string             `env:"TON_CONFIG_URL" required:"true" envDefault:"https://ton-blockchain.github.io/global.config.json"`
	LogLevel       uint8              `env:"SYSTEM_LOG_LEVEL" envDefault:"1"`
}

func loadConfig() *Config {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		log.Fatalf("Failed to parse agent config: %v", err)
	}

	if cfg.Key == nil {
		_, priv, _ := ed25519.GenerateKey(nil)
		cfg.Key = priv
	}

	return cfg
}
