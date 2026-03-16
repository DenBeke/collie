package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"eventstore"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Compile-time check that Projection implements CheckpointProjector.
var _ eventstore.CheckpointProjector = (*Projection[any])(nil)

// Projection is a postgres-backed [eventstore.CheckpointProjector] that keeps
// the full projection state in memory and persists it to the database on every
// applied event. On construction it loads existing state from the database,
// allowing it to survive restarts.
type Projection[S any] struct {
	eventstore.Projection[S]
	db       *gorm.DB
	name     string
	position uint64
}

// NewProjection creates a postgres-backed projection. If a snapshot exists in
// the database for the given name, the state and position are restored from it.
// Otherwise the projection starts fresh with the initial state from p.
func NewProjection[S any](db *gorm.DB, name string, p eventstore.Projection[S]) (*Projection[S], error) {
	proj := &Projection[S]{
		Projection: p,
		db:         db,
		name:       name,
	}
	err := proj.load()
	if err != nil {
		return nil, fmt.Errorf("could not load projection %q: %w", name, err)
	}
	return proj, nil
}

// After returns the sequence number of the last applied event.
func (p *Projection[S]) After() int64 {
	return int64(p.position)
}

// Apply applies a single event to the in-memory state and persists the
// snapshot and checkpoint to the database in a single transaction.
func (p *Projection[S]) Apply(ctx context.Context, e eventstore.SequencedEvent) error {
	err := p.Projection.Apply(ctx, e)
	if err != nil {
		return fmt.Errorf("could not apply %s event: %w", e.Type, err)
	}
	p.position = e.SequenceNumber
	return p.save(ctx)
}

// Reset deletes the snapshot and checkpoint from the database and resets the
// in-memory state to its zero value.
func (p *Projection[S]) Reset(ctx context.Context) error {
	err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Delete(&projectionSnapshot{}, "projection_name = ?", p.name).Error
		if err != nil {
			return fmt.Errorf("could not delete snapshot: %w", err)
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
	var zero S
	p.State = zero
	p.position = 0
	return nil
}

func (p *Projection[S]) load() error {
	var checkpoint projectionCheckpoint
	err := p.db.Where("projection_name = ?", p.name).First(&checkpoint).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil // fresh projection
		}
		return fmt.Errorf("could not load checkpoint: %w", err)
	}
	p.position = uint64(checkpoint.Position)

	var snapshot projectionSnapshot
	err = p.db.Where("projection_name = ?", p.name).First(&snapshot).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		return fmt.Errorf("could not load snapshot: %w", err)
	}
	err = json.Unmarshal(snapshot.State, &p.State)
	if err != nil {
		return fmt.Errorf("could not unmarshal snapshot state: %w", err)
	}
	return nil
}

func (p *Projection[S]) save(ctx context.Context) error {
	stateBytes, err := json.Marshal(p.State)
	if err != nil {
		return fmt.Errorf("could not marshal projection state: %w", err)
	}
	err = p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "projection_name"}},
			DoUpdates: clause.AssignmentColumns([]string{"position"}),
		}).Create(&projectionCheckpoint{
			ProjectionName: p.name,
			Position:       int64(p.position),
		}).Error
		if err != nil {
			return fmt.Errorf("could not upsert checkpoint: %w", err)
		}
		err = tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "projection_name"}},
			DoUpdates: clause.AssignmentColumns([]string{"state"}),
		}).Create(&projectionSnapshot{
			ProjectionName: p.name,
			State:          stateBytes,
		}).Error
		if err != nil {
			return fmt.Errorf("could not upsert snapshot: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not save projection %q: %w", p.name, err)
	}
	return nil
}
