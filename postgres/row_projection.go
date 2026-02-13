package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"eventstore"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Compile-time check that RowProjection implements CheckpointProjector.
var _ eventstore.CheckpointProjector = (*RowProjection[any])(nil)

// RowHandler defines how events map to individual rows in a [RowProjection].
// Each event produces a key identifying which row to update and a
// transformation of that row's current value.
type RowHandler[V any] interface {
	// Key extracts the row key from an event. Different event types may store
	// the key in different places (tags, data payload, etc.).
	Key(eventstore.SequencedEvent) (string, error)

	// Apply transforms the current row value. current is nil if the row does
	// not exist yet. Return nil to delete the row.
	Apply(eventstore.SequencedEvent, *V) (*V, error)
}

// RowProjection is a postgres-backed [eventstore.CheckpointProjector] that
// stores one row per entity. It does not keep the full projection in memory —
// each [Apply] loads the affected row, calls the handler, and writes it back.
type RowProjection[V any] struct {
	db       *gorm.DB
	name     string
	query    eventstore.QueryItem
	position uint64
	handler  RowHandler[V]
}

// NewRowProjection creates a row-based projection. If a checkpoint exists in
// the database for the given name, the position is restored from it.
func NewRowProjection[V any](db *gorm.DB, name string, query eventstore.QueryItem, handler RowHandler[V]) (*RowProjection[V], error) {
	p := &RowProjection[V]{
		db:      db,
		name:    name,
		query:   query,
		handler: handler,
	}
	err := p.loadCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("could not load checkpoint for %q: %w", name, err)
	}
	return p, nil
}

// EventQuery returns the projection's query.
func (p *RowProjection[V]) EventQuery() eventstore.QueryItem {
	return p.query
}

// After returns the sequence number of the last applied event.
func (p *RowProjection[V]) After() int64 {
	return int64(p.position)
}

// Apply extracts the row key, loads the current value, calls the handler, and
// persists the result — all in a single transaction.
func (p *RowProjection[V]) Apply(ctx context.Context, e eventstore.SequencedEvent) error {
	key, err := p.handler.Key(e)
	if err != nil {
		return fmt.Errorf("could not extract key from %s event: %w", e.Type, err)
	}

	err = p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Load current row value.
		current, err := p.loadRow(tx, key)
		if err != nil {
			return fmt.Errorf("could not load row %q: %w", key, err)
		}

		// Apply handler.
		newValue, err := p.handler.Apply(e, current)
		if err != nil {
			return fmt.Errorf("could not apply %s event to row %q: %w", e.Type, key, err)
		}

		// Upsert or delete row.
		if newValue == nil {
			err = tx.Delete(&projectionRow{}, "projection_name = ? AND key = ?", p.name, key).Error
			if err != nil {
				return fmt.Errorf("could not delete row %q: %w", key, err)
			}
		} else {
			valueBytes, err := json.Marshal(newValue)
			if err != nil {
				return fmt.Errorf("could not marshal row %q: %w", key, err)
			}
			err = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "projection_name"}, {Name: "key"}},
				DoUpdates: clause.AssignmentColumns([]string{"value"}),
			}).Create(&projectionRow{
				ProjectionName: p.name,
				Key:            key,
				Value:          valueBytes,
			}).Error
			if err != nil {
				return fmt.Errorf("could not upsert row %q: %w", key, err)
			}
		}

		// Update checkpoint.
		p.position = e.SequenceNumber
		err = tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "projection_name"}},
			DoUpdates: clause.AssignmentColumns([]string{"position"}),
		}).Create(&projectionCheckpoint{
			ProjectionName: p.name,
			Position:       int64(p.position),
		}).Error
		if err != nil {
			return fmt.Errorf("could not upsert checkpoint: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not apply event to projection %q: %w", p.name, err)
	}
	return nil
}

// Get retrieves a single row by key. Returns nil if the row does not exist.
func (p *RowProjection[V]) Get(ctx context.Context, key string) (*V, error) {
	var row projectionRow
	err := p.db.WithContext(ctx).Where("projection_name = ? AND key = ?", p.name, key).First(&row).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("could not get row %q: %w", key, err)
	}
	var v V
	err = json.Unmarshal(row.Value, &v)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal row %q: %w", key, err)
	}
	return &v, nil
}

// Reset deletes all rows and the checkpoint for this projection and resets
// the in-memory position.
func (p *RowProjection[V]) Reset(ctx context.Context) error {
	err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Delete(&projectionRow{}, "projection_name = ?", p.name).Error
		if err != nil {
			return fmt.Errorf("could not delete rows: %w", err)
		}
		err = tx.Delete(&projectionCheckpoint{}, "projection_name = ?", p.name).Error
		if err != nil {
			return fmt.Errorf("could not delete checkpoint: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not reset projection %q: %w", p.name, err)
	}
	p.position = 0
	return nil
}

func (p *RowProjection[V]) loadCheckpoint() error {
	var cp projectionCheckpoint
	err := p.db.Where("projection_name = ?", p.name).First(&cp).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		return fmt.Errorf("could not load checkpoint: %w", err)
	}
	p.position = uint64(cp.Position)
	return nil
}

func (p *RowProjection[V]) loadRow(tx *gorm.DB, key string) (*V, error) {
	var row projectionRow
	err := tx.Where("projection_name = ? AND key = ?", p.name, key).First(&row).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("could not query row: %w", err)
	}
	var v V
	err = json.Unmarshal(row.Value, &v)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal row value: %w", err)
	}
	return &v, nil
}
