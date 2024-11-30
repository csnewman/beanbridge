package beanstalk

import (
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
)

type Server struct {
	logger       *slog.Logger
	listener     net.Listener
	factory      Factory
	shuttingDown atomic.Bool
}

func NewServer(logger *slog.Logger, address string, factory Factory) (*Server, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	return &Server{
		logger:   logger,
		listener: l,
		factory:  factory,
	}, nil
}

func (s *Server) Serve() error {
	s.logger.Info("Listening for beanstalk connections", "addr", s.listener.Addr())

	for {
		rwc, err := s.listener.Accept()
		if err != nil {
			if s.shuttingDown.Load() {
				return nil
			}

			return fmt.Errorf("failed to accept connection: %w", err)
		}

		c := newConn(s.logger, rwc, s.factory)

		go func() {
			if err := c.serve(); err != nil {
				s.logger.Warn("Error while serving connection", "err", err)
			}
		}()
	}
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Close() {
	s.shuttingDown.Store(true)

	_ = s.listener.Close()
}
