package postgres

import (
	"context"
	"fmt"

	"eventstore/snapshot"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Compile-time check that SnapshotStore implements snapshot.Store.
var _ snapshot.Store = (*SnapshotStore)(nil)

// SnapshotStore is a postgres-backed [snapshot.Store]. It persists
// projection snapshots and checkpoints to the projection_snapshots and
// projection_checkpoints tables (created by [MigrateProjections]), so an
// [snapshot.Projection] survives restarts and resumes from its last
// checkpoint.
type SnapshotStore struct {
	db *gorm.DB
}

// NewSnapshotStore creates a postgres-backed snapshot store. Call
// [MigrateProjections] on the same database before use.
func NewSnapshotStore(db *gorm.DB) *SnapshotStore {
	return &SnapshotStore{db: db}
}

// Load returns the persisted state and position for name. found is false when
// no checkpoint exists yet.
func (store *SnapshotStore) Load(ctx context.Context, name string) (state []byte, position uint64, found bool, err error) {
	var checkpoint projectionCheckpoint
	err = store.db.WithContext(ctx).Where("projection_name = ?", name).First(&checkpoint).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, 0, false, nil // fresh projection
		}
		return nil, 0, false, fmt.Errorf("could not load checkpoint: %w", err)
	}
	position = uint64(checkpoint.Position)

	var snapshot projectionSnapshot
	err = store.db.WithContext(ctx).Where("projection_name = ?", name).First(&snapshot).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// Known position but no state snapshot yet.
			return nil, position, true, nil
		}
		return nil, 0, false, fmt.Errorf("could not load snapshot: %w", err)
	}
	return snapshot.State, position, true, nil
}

// Save upserts the checkpoint and snapshot for name in a single transaction.
func (store *SnapshotStore) Save(ctx context.Context, name string, state []byte, position uint64) error {
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "projection_name"}},
			DoUpdates: clause.AssignmentColumns([]string{"position"}),
		}).Create(&projectionCheckpoint{
			ProjectionName: name,
			Position:       int64(position),
		}).Error; err != nil {
			return fmt.Errorf("could not upsert checkpoint: %w", err)
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "projection_name"}},
			DoUpdates: clause.AssignmentColumns([]string{"state"}),
		}).Create(&projectionSnapshot{
			ProjectionName: name,
			State:          state,
		}).Error; err != nil {
			return fmt.Errorf("could not upsert snapshot: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not save projection %q: %w", name, err)
	}
	return nil
}

// Delete removes the snapshot and checkpoint for name.
func (store *SnapshotStore) Delete(ctx context.Context, name string) error {
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&projectionSnapshot{}, "projection_name = ?", name).Error; err != nil {
			return fmt.Errorf("could not delete snapshot: %w", err)
		}
		if err := tx.Delete(&projectionCheckpoint{}, "projection_name = ?", name).Error; err != nil {
			return fmt.Errorf("could not delete checkpoint: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not delete projection %q: %w", name, err)
	}
	return nil
}
