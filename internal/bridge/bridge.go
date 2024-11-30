package bridge

import (
	"errors"
	"fmt"
	"github.com/csnewman/beanbridge/internal/backend"
	"github.com/csnewman/beanbridge/internal/backend/memory"
	"github.com/csnewman/beanbridge/internal/backend/nullsink"
	"github.com/csnewman/beanbridge/internal/beanstalk"
	"log/slog"
)

var ErrUnknownBackend = errors.New("unknown backend type")

type Config struct {
	Address string `json:"address"`
	Backend string `json:"backend"`
}

type Server struct {
	logger  *slog.Logger
	bs      *beanstalk.Server
	backend backend.Backend
}

func NewServer(logger *slog.Logger, cfg *Config) (*Server, error) {
	s := &Server{
		logger: logger,
	}

	bs, err := beanstalk.NewServer(logger, cfg.Address, s.handleConnection)
	if err != nil {
		return nil, fmt.Errorf("failed to create beanstalk server: %w", err)
	}

	s.bs = bs

	switch cfg.Backend {
	case "nullsink":
		s.backend = nullsink.NewBackend(logger)
	case "memory":
		s.backend = memory.NewBackend(logger)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownBackend, cfg.Backend)
	}

	return s, nil
}

const defaultTube = "default"

func (s *Server) handleConnection(conn *beanstalk.Conn) beanstalk.Handler {
	return &Conn{
		logger:   s.logger.With("remote", conn.Addr()),
		server:   s,
		conn:     conn,
		mainTube: s.backend.ResolveTube(defaultTube),
		watching: []backend.Tube{
			s.backend.ResolveTube(defaultTube),
		},
	}
}

func (s *Server) Serve() error {
	return s.bs.Serve()
}
