package postgres

// projectionCheckpoint tracks the stream position for any projection type.
type projectionCheckpoint struct {
	ProjectionName string `gorm:"primaryKey"`
	Position       int64  `gorm:"not null;default:0"`
}

// projectionSnapshot stores the serialized state for snapshot-based projections.
type projectionSnapshot struct {
	ProjectionName string               `gorm:"primaryKey"`
	State          []byte               `gorm:"type:jsonb;not null"`
	Checkpoint     projectionCheckpoint `gorm:"foreignKey:ProjectionName;constraint:OnDelete:CASCADE"`
}

// projectionRow stores a single row for row-based projections.
type projectionRow struct {
	ProjectionName string               `gorm:"primaryKey"`
	Key            string               `gorm:"primaryKey"`
	Value          []byte               `gorm:"type:jsonb;not null"`
	Checkpoint     projectionCheckpoint `gorm:"foreignKey:ProjectionName;constraint:OnDelete:CASCADE"`
}

// MigrateProjections creates the projection tables and foreign key constraints.
// Call this separately from Store.Migrate — projections may use a different
// database or be used without the postgres event store.
func MigrateProjections(db interface{ AutoMigrate(...any) error }) error {
	return db.AutoMigrate(
		&projectionCheckpoint{},
		&projectionSnapshot{},
		&projectionRow{},
	)
}
