package beanstalk

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
)

var (
	ErrBadFormat      = errors.New("bad format")
	ErrReserveTimeout = errors.New("reserve timeout")
	ErrDeadlineSoon   = errors.New("deadline soon")
	ErrNotFound       = errors.New("not found")
	ErrNotIgnored     = errors.New("not ignored")
)

type Factory func(conn *Conn) Handler

type Conn struct {
	logger  *slog.Logger
	rwc     net.Conn
	reader  *bufio.Reader
	factory Factory
	handler Handler
}

type Handler interface {
	Put(pri uint64, delay uint64, ttr uint64, data []byte) (uint64, bool, error)

	Use(tube string) (string, error)

	Reserve(timeout int64) (uint64, []byte, error)

	ReserveByID(id uint64) (uint64, []byte, error)

	Delete(id uint64) error

	Release(id uint64, pri uint64, delay uint64) error

	Bury(id uint64, pri uint64) error

	Touch(id uint64) error

	Watch(tube string) (int, error)

	Ignore(tube string) (int, error)
}

func newConn(logger *slog.Logger, rwc net.Conn, factory Factory) *Conn {
	return &Conn{
		logger:  logger,
		rwc:     rwc,
		reader:  bufio.NewReader(rwc),
		factory: factory,
	}
}

func (c *Conn) serve() error {
	defer c.rwc.Close()

	c.logger = c.logger.With("remote", c.rwc.RemoteAddr())

	c.logger.Info("Accepted new beanstalk connection")

	c.handler = c.factory(c)

	for {
		line, err := readFullLine(c.reader)
		if err != nil {
			return fmt.Errorf("read line failed: %w", err)
		}

		c.logger.Debug("Read line", "line", line)

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		if err := c.process(fields); err != nil {
			return err
		}
	}
}

func (c *Conn) process(fields []string) error {
	cmd := strings.ToLower(fields[0])

	switch cmd {
	case cmdQuit:
		panic("todo")

	case cmdPut:
		if len(fields) != 5 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		pri, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		delay, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		ttr, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		bytes, err := strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		data, err := readBlob(c.reader, int(bytes))
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		c.logger.Debug("Put request", "pri", pri, "delay", delay, "ttr", ttr, "bytes", len(data))

		id, buried, err := c.handler.Put(pri, delay, ttr, data)
		if err != nil {
			return fmt.Errorf("failed to put: %w", err)
		}

		if buried {
			return writeLine(c.rwc, resBuriedID, id)
		}

		return writeLine(c.rwc, resInserted, id)

	case cmdUse:
		if len(fields) != 2 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		tube, err := c.handler.Use(fields[1])
		if err != nil {
			return fmt.Errorf("failed to use: %w", err)
		}

		return writeLine(c.rwc, resUsing, tube)

	case cmdReserve, cmdReserveWithTimeout:
		var timeout int64

		switch cmd {
		case cmdReserve:
			if len(fields) != 1 {
				return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
			}

			timeout = -1
		case cmdReserveWithTimeout:
			if len(fields) != 2 {
				return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
			}

			parsed, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrBadFormat, err)
			}

			timeout = parsed
		default:
			panic("unexpected")
		}

		id, data, err := c.handler.Reserve(timeout)
		if errors.Is(err, ErrReserveTimeout) && timeout >= 0 {
			return writeLine(c.rwc, resTimedOut)
		} else if errors.Is(err, ErrNotFound) {
			return writeLine(c.rwc, resNotFound)
		} else if errors.Is(err, ErrDeadlineSoon) {
			return writeLine(c.rwc, resDeadlineSoon)
		} else if err != nil {
			return fmt.Errorf("failed to reserve: %w", err)
		}

		return writeLine(c.rwc, resReserved, id, len(data), data)

	case cmdReserveJob:
		if len(fields) != 2 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		id, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		id, data, err := c.handler.ReserveByID(id)
		if errors.Is(err, ErrNotFound) {
			return writeLine(c.rwc, resNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to reserve by id: %w", err)
		}

		return writeLine(c.rwc, resReserved, id, len(data), data)

	case cmdDelete:
		if len(fields) != 2 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		id, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		err = c.handler.Delete(id)
		if errors.Is(err, ErrNotFound) {
			return writeLine(c.rwc, resNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to bury: %w", err)
		}

		return writeLine(c.rwc, resDeleted)

	case cmdRelease:
		if len(fields) != 4 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		id, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		pri, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		delay, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		err = c.handler.Release(id, pri, delay)
		if errors.Is(err, ErrNotFound) {
			return writeLine(c.rwc, resNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to bury: %w", err)
		}

		return writeLine(c.rwc, resReleased)

	case cmdBury:
		if len(fields) != 3 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		id, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		pri, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		err = c.handler.Bury(id, pri)
		if errors.Is(err, ErrNotFound) {
			return writeLine(c.rwc, resNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to bury: %w", err)
		}

		return writeLine(c.rwc, resBuried)

	case cmdTouch:
		if len(fields) != 2 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		id, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadFormat, err)
		}

		err = c.handler.Touch(id)
		if errors.Is(err, ErrNotFound) {
			return writeLine(c.rwc, resNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to touch: %w", err)
		}

		return writeLine(c.rwc, resTouched)

	case cmdWatch:
		if len(fields) != 2 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		count, err := c.handler.Watch(fields[1])
		if err != nil {
			return fmt.Errorf("failed to watch: %w", err)
		}

		return writeLine(c.rwc, resWatching, count)

	case cmdIgnore:
		if len(fields) != 2 {
			return fmt.Errorf("%w: unexpected field count", ErrBadFormat)
		}

		count, err := c.handler.Ignore(fields[1])
		if errors.Is(err, ErrNotIgnored) {
			return writeLine(c.rwc, resNotIgnored)
		} else if err != nil {
			return fmt.Errorf("failed to watch: %w", err)
		}

		return writeLine(c.rwc, resWatching, count)

	default:
		return writeLine(c.rwc, resUnknownCommand)
	}
}
