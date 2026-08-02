package postgres

import (
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

// projectionRow stores a single row for row-based projections.
type projectionRow struct {
	ProjectionName string `gorm:"primaryKey"`
	Key            string `gorm:"primaryKey"`
	Value          []byte `gorm:"type:jsonb;not null"`
	Tags           []byte `gorm:"type:jsonb"`
}

// MigrateProjections creates the projection tables and foreign key constraints.
// Call this separately from Store.Migrate — projections may use a different
// database or be used without the postgres event store.
func MigrateProjections(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&projectionCheckpoint{},
		&projectionSnapshot{},
		&projectionRow{},
	); err != nil {
		return err
	}

	fks := []string{
		`ALTER TABLE projection_snapshots ADD CONSTRAINT fk_snapshots_checkpoint
		 FOREIGN KEY (projection_name) REFERENCES projection_checkpoints(projection_name)
		 ON DELETE CASCADE`,
		`ALTER TABLE projection_rows ADD CONSTRAINT fk_rows_checkpoint
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
