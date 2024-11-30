package beanstalk_test

import (
	"errors"
	"testing"
	"time"

	bc "github.com/beanstalkd/go-beanstalk"
	"github.com/csnewman/beanbridge/internal/beanstalk"
	"github.com/csnewman/beanbridge/internal/mocks"
	"github.com/csnewman/beanbridge/internal/testutils"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	t.Parallel()

	handler := mocks.NewMockBeanstalkHandler(t)

	testutils.Server(t, func(conn *beanstalk.Conn) beanstalk.Handler {
		return handler
	}, func(t *testing.T, s *beanstalk.Server) {
		c, err := bc.Dial(s.Addr().Network(), s.Addr().String())
		require.NoError(t, err, "Client should connect")

		handler.EXPECT().
			Put(uint64(1), uint64(0), uint64(120), []byte("hello")).
			Return(123, false, nil)

		id, err := c.Put([]byte("hello"), 1, 0, 120*time.Second)
		require.NoError(t, err, "Put should not error")
		require.Equal(t, uint64(123), id, "Put should return id 123")

		handler.EXPECT().
			Put(uint64(101), uint64(102), uint64(103), []byte("world")).
			Return(234, true, nil)

		id, err = c.Put([]byte("world"), 101, 102*time.Second, 103*time.Second)
		require.ErrorContains(t, err, "BURIED 234", "Put should return buried")

		handler.EXPECT().
			Put(uint64(101), uint64(102), uint64(103), []byte("error")).
			Return(0, false, errors.New("example"))

		id, err = c.Put([]byte("error"), 101, 102*time.Second, 103*time.Second)
		require.ErrorContains(t, err, "internal error", "Put should return error")
	})
}

func TestUse(t *testing.T) {
	t.Parallel()

	handler := mocks.NewMockBeanstalkHandler(t)

	testutils.Server(t, func(conn *beanstalk.Conn) beanstalk.Handler {
		return handler
	}, func(t *testing.T, s *beanstalk.Server) {
		c, err := bc.Dial(s.Addr().Network(), s.Addr().String())
		require.NoError(t, err, "Client should connect")

		handler.EXPECT().
			Put(uint64(1), uint64(0), uint64(120), []byte("hello")).
			Return(123, false, nil)

		handler.EXPECT().
			Use("tube1").
			Return("tube1", nil)

		t1 := bc.NewTube(c, "tube1")

		id, err := t1.Put([]byte("hello"), 1, 0, 120*time.Second)
		require.NoError(t, err, "Put should not error")
		require.Equal(t, uint64(123), id, "Put should return id 123")

		t2 := bc.NewTube(c, "tube2")

		handler.EXPECT().
			Use("tube2").
			Return("", errors.New("example"))

		id, err = t2.Put([]byte("hello"), 1, 0, 120*time.Second)
		require.ErrorContains(t, err, "internal error", "Put should return error")
	})
}
