package postgres

import (
	"context"
	"log/slog"
	"time"
)

// WithoutFlusher keeps New from starting a flusher. Staged appends then become
// readable only through a flusher another process runs, or one started here
// with RunFlusher.
func WithoutFlusher() Option {
	return func(s *Store) { s.flusher = false }
}

// WithFlushOptions configures the flusher New starts.
func WithFlushOptions(opts FlushOptions) Option {
	return func(s *Store) { s.flushOpts = opts }
}

// WithLogger sets where the flusher New starts reports errors. Defaults to
// slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(s *Store) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// keepFlushing runs RunFlusher until ctx is cancelled, starting it again after
// an error (a lost connection, a database that is not migrated yet) with a
// backoff, so the store's flusher outlives a database restart.
func (s *Store) keepFlushing(ctx context.Context) {
	defer close(s.stopped)
	const first, most = time.Second, 30 * time.Second
	wait := first
	for {
		started := time.Now()
		err := s.RunFlusher(ctx, s.flushOpts)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > most {
			wait = first
		}
		s.logger.Error("dcb flusher stopped, restarting", "error", err, "in", wait)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(wait*2, most)
	}
}

// Close stops the flusher New started; when it was the one leading, it first
// moves what is still staged. It is a no-op for a store without one, and safe
// to call more than once.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		if s.stop != nil {
			s.stop()
			<-s.stopped
		}
	})
	return nil
}
