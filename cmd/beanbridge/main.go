package main

import (
	"github.com/csnewman/beanbridge/internal/beanstalk"
	"log/slog"
	"os"
)

type Connection struct {
}

func (c *Connection) Use(tube string) (string, error) {
	return tube, nil
}

func (c *Connection) Reserve(timeout int64) (uint64, []byte, error) {
	return 123, []byte("hello"), nil
}

func (c *Connection) ReserveByID(id uint64) (uint64, []byte, error) {
	return 0, nil, beanstalk.ErrNotFound
}

func (c *Connection) Put(pri uint64, delay uint64, ttr uint64, data []byte) (uint64, bool, error) {
	return 456, false, nil
}

func (c *Connection) Delete(id uint64) error {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Release(id uint64, pri uint64, delay uint64) error {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Bury(id uint64, pri uint64) error {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Touch(id uint64) error {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Watch(tube string) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Ignore(tube string) (int, error) {
	//TODO implement me
	panic("implement me")
}

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	server, err := beanstalk.NewServer(logger, ":11300", func(conn *beanstalk.Conn) beanstalk.Handler {

		return &Connection{}

	})
	if err != nil {
		panic(err)
	}

	if err := server.Serve(); err != nil {
		panic(err)
	}

}
