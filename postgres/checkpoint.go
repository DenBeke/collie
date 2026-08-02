package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/DenBeke/collie/reactor"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// reactorCheckpoint is how far one reactor got, keyed by registered name — its
// own table, not a row among the projections. See [reactor.CheckpointStore].
type reactorCheckpoint struct {
	Name     string `gorm:"primaryKey"`
	Position int64  `gorm:"not null"`
}

// Compile-time check that CheckpointStore implements reactor.CheckpointStore.
var _ reactor.CheckpointStore = (*CheckpointStore)(nil)

// CheckpointStore is a postgres-backed [reactor.CheckpointStore], keeping
// positions in the reactor_checkpoints table [CheckpointStore.Migrate] creates.
type CheckpointStore struct {
	db *gorm.DB
}

// NewCheckpointStore creates a postgres-backed reactor checkpoint store. Call
// [CheckpointStore.Migrate] on the same database before use.
func NewCheckpointStore(db *gorm.DB) *CheckpointStore {
	return &CheckpointStore{db: db}
}

// Migrate creates the reactor_checkpoints table, separately from Store.Migrate
// and [MigrateProjections]: reactors may run against a different database.
func (store *CheckpointStore) Migrate() error {
	return store.db.AutoMigrate(&reactorCheckpoint{})
}

// Load returns the position stored for name; found is false when there is none.
func (store *CheckpointStore) Load(ctx context.Context, name string) (position uint64, found bool, err error) {
	var checkpoint reactorCheckpoint
	err = store.db.WithContext(ctx).Where("name = ?", name).First(&checkpoint).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, false, nil // a reactor that has never run
		}
		return 0, false, fmt.Errorf("could not load the checkpoint for reactor %q: %w", name, err)
	}
	return uint64(checkpoint.Position), true, nil
}

// Save upserts the position for name.
func (store *CheckpointStore) Save(ctx context.Context, name string, position uint64) error {
	err := store.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"position"}),
	}).Create(&reactorCheckpoint{
		Name:     name,
		Position: int64(position),
	}).Error
	if err != nil {
		return fmt.Errorf("could not save the checkpoint for reactor %q: %w", name, err)
	}
	return nil
}
