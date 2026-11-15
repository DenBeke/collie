package postgres

import (
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// projectionCheckpoint tracks the stream position for any projection type.
type projectionCheckpoint struct {
	ProjectionName string `gorm:"primaryKey"`
	Position       int64  `gorm:"not null;default:0"`
}

// projectionSnapshot stores the serialized state for snapshot-based projections.
type projectionSnapshot struct {
	ProjectionName string `gorm:"primaryKey"`
	State          []byte `gorm:"type:jsonb;not null"`
}

// projectionEntry stores one value of a keyed projection, under its key.
type projectionEntry struct {
	ProjectionName string `gorm:"primaryKey"`
	Key            string `gorm:"primaryKey"`
	Value          []byte `gorm:"type:jsonb;not null"`
	Tags           []byte `gorm:"type:jsonb"`
}

// renameProjectionRows is a no-op once the table has its new name, so
// MigrateProjections stays idempotent.
const renameProjectionRows = `ALTER TABLE IF EXISTS projection_rows RENAME TO projection_entries`

// MigrateProjections creates the projection tables and foreign key constraints.
// Call this separately from Store.Migrate — projections may use a different
// database or be used without the postgres event store.
func MigrateProjections(db *gorm.DB) error {
	// The keyed projections' table was called projection_rows. It is renamed
	// before AutoMigrate, which would otherwise create an empty table next to it
	// and leave every keyed projection without its rows.
	if err := db.Exec(renameProjectionRows).Error; err != nil {
		return fmt.Errorf("could not rename projection_rows to projection_entries: %w", err)
	}
	if err := db.AutoMigrate(
		&projectionCheckpoint{},
		&projectionSnapshot{},
		&projectionEntry{},
	); err != nil {
		return err
	}

	fks := []string{
		`ALTER TABLE projection_snapshots ADD CONSTRAINT fk_snapshots_checkpoint
		 FOREIGN KEY (projection_name) REFERENCES projection_checkpoints(projection_name)
		 ON DELETE CASCADE`,
		// The constraint of a renamed table keeps its name; renamed first, so
		// the statement after it does not add a second one.
		`ALTER TABLE projection_entries RENAME CONSTRAINT fk_rows_checkpoint TO fk_entries_checkpoint`,
		`ALTER TABLE projection_entries ADD CONSTRAINT fk_entries_checkpoint
		 FOREIGN KEY (projection_name) REFERENCES projection_checkpoints(projection_name)
		 ON DELETE CASCADE`,
	}
	// Ignore "already exists" errors for idempotency — on a silent session,
	// because the caller's logger would otherwise report two errors on every
	// startup after the first, each of which means the schema is exactly right.
	quiet := db.Session(&gorm.Session{Logger: logger.Discard})
	for _, sql := range fks {
		quiet.Exec(sql)
	}
	return nil
}
