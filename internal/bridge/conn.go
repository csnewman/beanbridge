package bridge

import (
	"github.com/csnewman/beanbridge/internal/backend"
	"github.com/csnewman/beanbridge/internal/beanstalk"
	"log/slog"
)

type Conn struct {
	logger *slog.Logger
	server *Server
	conn   *beanstalk.Conn

	mainTube backend.Tube
	watching []backend.Tube
}

func (c *Conn) Use(tube string) (string, error) {
	if c.mainTube.Name() == tube {
		return tube, nil
	}

	c.mainTube.Release()

	c.mainTube = c.server.backend.ResolveTube(tube)

	return tube, nil
}

func (c *Conn) Put(pri uint64, delay uint64, ttr uint64, data []byte) (uint64, bool, error) {
	return c.server.backend.Put(c.mainTube, pri, delay, ttr, data)
}

func (c *Conn) Watch(tube string) (int, error) {
	for _, t := range c.watching {
		if t.Name() == tube {
			return len(c.watching), nil
		}
	}

	t := c.server.backend.ResolveTube(tube)

	c.watching = append(c.watching, t)

	return len(c.watching), nil
}

func (c *Conn) Ignore(tube string) (int, error) {
	var newWatching []backend.Tube

	for _, t := range c.watching {
		if t.Name() == tube {
			t.Release()

			continue
		}

		newWatching = append(newWatching, t)
	}

	return len(c.watching), nil
}

func (c *Conn) Reserve(timeout int64) (uint64, []byte, error) {
	return c.server.backend.Reserve(c.watching, timeout)
}

func (c *Conn) ReserveByID(id uint64) (uint64, []byte, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Delete(id uint64) error {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Release(id uint64, pri uint64, delay uint64) error {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Bury(id uint64, pri uint64) error {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Touch(id uint64) error {
	//TODO implement me
	panic("implement me")
}
