package postgres

import "context"

// FlushForTest lets the external tests drain staging themselves, on a store
// built WithoutFlusher.
func (s *Store) FlushForTest(ctx context.Context, batchSize int) (int64, error) {
	return s.flush(ctx, batchSize)
}
