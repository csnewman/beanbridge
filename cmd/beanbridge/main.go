package main

import (
	"log/slog"
	"os"

	"github.com/csnewman/beanbridge/internal/bridge"
	"gopkg.in/yaml.v3"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	var cfg *bridge.Config

	rawCfg, err := os.ReadFile("example.yaml")
	if err != nil {
		panic(err)
	}

	if err := yaml.Unmarshal(rawCfg, &cfg); err != nil {
		panic(err)
	}

	s, err := bridge.NewServer(logger, cfg)
	if err != nil {
		panic(err)
	}

	if err := s.Serve(); err != nil {
		panic(err)
	}
}
