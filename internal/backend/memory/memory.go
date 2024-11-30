package memory

import (
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/csnewman/beanbridge/internal/backend"
	"github.com/csnewman/beanbridge/internal/beanstalk"
)

type Backend struct {
	logger *slog.Logger
	mu     sync.Mutex
	tubes  map[string]*Tube
	jobs   map[uint64]*Job
	lastID uint64
}

func NewBackend(logger *slog.Logger) backend.Backend {
	b := &Backend{
		logger: logger,
		tubes:  make(map[string]*Tube),
		jobs:   make(map[uint64]*Job),
	}

	go b.background()

	return b
}

func (b *Backend) ResolveTube(name string) backend.Tube {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.tubes[name]
	if !ok {
		t = &Tube{
			backend: b,
			name:    name,
		}

		b.tubes[name] = t
	}

	return t
}

func (b *Backend) background() {
	for {
		if b.process() {
			continue
		}

		time.Sleep(time.Millisecond * 500)
	}
}

func (b *Backend) process() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	for _, tube := range b.tubes {

		did := 0

		for len(tube.delayed) > 0 {
			dl := len(tube.delayed)

			j := tube.delayed[dl-1]

			if j.ReleaseTime.After(now) {
				break
			}

			tube.delayed[dl-1] = nil
			tube.delayed = tube.delayed[:dl-1]

			tube.ready = append(tube.ready, j)

			did++
		}

		for len(tube.reserved) > 0 {
			rl := len(tube.reserved)

			j := tube.reserved[rl-1]

			if j.ReleaseTime.After(now) {
				break
			}

			tube.reserved[rl-1] = nil
			tube.reserved = tube.reserved[:rl-1]

			tube.ready = append(tube.ready, j)

			did++
		}

		if did > 0 {
			tube.sortReady()
		}
	}

	return false
}

type Tube struct {
	backend  *Backend
	name     string
	ready    []*Job
	delayed  []*Job
	reserved []*Job
}

func (t *Tube) Name() string {
	return t.name
}

func (t *Tube) Release() {
}

func (t *Tube) String() string {
	return t.name
}

func (t *Tube) sortDelayed() {
	slices.SortFunc(t.delayed, func(a, b *Job) int {
		if a.ReleaseTime.Before(b.ReleaseTime) {
			return 1
		} else if b.ReleaseTime.Before(a.ReleaseTime) {
			return -1
		}

		return 0
	})
}

func (t *Tube) sortReserved() {
	slices.SortFunc(t.reserved, func(a, b *Job) int {
		if a.ReleaseTime.Before(b.ReleaseTime) {
			return 1
		} else if b.ReleaseTime.Before(a.ReleaseTime) {
			return -1
		}

		return 0
	})
}

func (t *Tube) sortReady() {
	// Largest priority latest in queue
	slices.SortFunc(t.ready, func(a, b *Job) int {
		if a.Priority < b.Priority {
			return 1
		} else if a.Priority > b.Priority {
			return -1
		}

		return 0
	})
}

type Job struct {
	ID          uint64
	Tube        *Tube
	Priority    uint64
	ReleaseTime time.Time
	TTR         uint64
	Data        []byte
}

func (b *Backend) Put(tube backend.Tube, pri uint64, delay uint64, ttr uint64, data []byte) (uint64, bool, error) {
	b.logger.Debug(
		"Put request",
		"tube", tube,
		"pri", pri,
		"delay", delay,
		"ttr", ttr,
		"bytes", len(data),
	)

	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastID++
	id := b.lastID

	t, ok := tube.(*Tube)
	if !ok {
		panic("invalid tube")
	}

	j := &Job{
		ID:       id,
		Tube:     t,
		Data:     data,
		Priority: pri,
		TTR:      ttr,
	}

	b.jobs[id] = j

	if delay > 0 {
		j.ReleaseTime = time.Now().Add(time.Second * time.Duration(delay))

		t.delayed = append(t.delayed, j)
		t.sortDelayed()
	} else {
		t.ready = append(t.ready, j)
		t.sortReady()
	}

	return id, false, nil
}

func (b *Backend) Reserve(tubes []backend.Tube, timeout int64) (uint64, []byte, error) {
	b.logger.Debug(
		"Reserve request",
		"tubes", tubes,
		"timeout", timeout,
	)

	j := b.tryReserve(tubes, timeout)

	if j == nil {
		return 0, nil, beanstalk.ErrReserveTimeout
	}

	return j.ID, j.Data, nil
}

func (b *Backend) tryReserve(tubes []backend.Tube, timeout int64) *Job {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, tube := range tubes {
		t, ok := tube.(*Tube)
		if !ok {
			panic("invalid tube")
		}

		rl := len(t.ready)

		if rl == 0 {
			continue
		}

		j := t.ready[rl-1]
		t.ready[rl-1] = nil
		t.ready = t.ready[:rl-1]

		j.ReleaseTime = time.Now().Add(time.Second * time.Duration(j.TTR))
		t.reserved = append(t.reserved, j)
		t.sortReserved()

		return j
	}

	return nil
}
