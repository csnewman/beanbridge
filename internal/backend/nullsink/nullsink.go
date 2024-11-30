package nullsink

import (
	"log/slog"
	"sync/atomic"

	"github.com/csnewman/beanbridge/internal/backend"
	"github.com/csnewman/beanbridge/internal/beanstalk"
)

type Backend struct {
	logger *slog.Logger
	lastID atomic.Uint64
}

func NewBackend(logger *slog.Logger) backend.Backend {
	return &Backend{
		logger: logger,
	}
}

type Tube struct {
	Backend *Backend
	name    string
}

func (b *Backend) ResolveTube(name string) backend.Tube {
	return &Tube{
		Backend: b,
		name:    name,
	}
}

func (t *Tube) Name() string {
	return t.name
}

func (t *Tube) Release() {
}

func (b *Backend) Put(tube backend.Tube, _ uint64, _ uint64, _ uint64, body []byte) (uint64, bool, error) {
	id := b.lastID.Add(1)

	b.logger.Debug("Dropping message", "tube", tube.Name(), "body", string(body))

	return id, false, nil
}

func (b *Backend) Reserve(_ []backend.Tube, _ int64) (uint64, []byte, error) {
	return 0, nil, beanstalk.ErrReserveTimeout
}
