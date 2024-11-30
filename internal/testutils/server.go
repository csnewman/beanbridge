package testutils

import (
	"context"
	"github.com/csnewman/beanbridge/internal/beanstalk"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"testing"
)

func Server(t *testing.T, factory beanstalk.Factory, f func(t *testing.T, s *beanstalk.Server)) {
	s, err := beanstalk.NewServer(slogt.New(t), ":", factory)
	require.NoError(t, err, "Server should not error")
	require.NotNil(t, s, "Server should not be nil")

	g, ctx := errgroup.WithContext(context.Background())
	_ = ctx

	g.Go(func() error {
		return s.Serve()
	})

	g.Go(func() error {
		defer s.Close()

		f(t, s)

		return nil
	})

	require.NoError(t, g.Wait())
}
