package postgres

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"

	"eventstore/encryption"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// encryptionKey stores a per-subject AES-256 key. This struct intentionally
// omits gorm.Model to avoid soft-delete — Forget must hard-delete keys
// so that encrypted event data becomes permanently unrecoverable.
type encryptionKey struct {
	TagID string `gorm:"column:tag_id;primaryKey"`
	Tags  []byte `gorm:"type:jsonb;not null"`
	Key   []byte `gorm:"type:bytea;not null"`
}

// Compile-time check that PostgresKeyStore implements encryption.KeyStore.
var _ encryption.KeyStore = (*PostgresKeyStore)(nil)

// PostgresKeyStore is a PostgreSQL-backed KeyStore that persists encryption
// keys in an encryption_keys table.
type PostgresKeyStore struct {
	db *gorm.DB
}

// NewPostgresKeyStore creates a key store backed by the given database connection.
func NewPostgresKeyStore(db *gorm.DB) *PostgresKeyStore {
	return &PostgresKeyStore{db: db}
}

// MigrateKeyStore creates the encryption_keys table.
func MigrateKeyStore(db *gorm.DB) error {
	return db.AutoMigrate(&encryptionKey{})
}

// GetOrCreate returns the encryption key for the exact tag set,
// creating a new random 32-byte key if none exists.
func (s *PostgresKeyStore) GetOrCreate(ctx context.Context, tags map[string]string) ([]byte, error) {
	id := encryption.TagID(tags)

	var existing encryptionKey
	err := s.db.WithContext(ctx).Where("tag_id = ?", id).First(&existing).Error
	if err == nil {
		return existing.Key, nil
	}
	if err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("could not query encryption key: %w", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("could not generate encryption key: %w", err)
	}

	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("could not marshal tags: %w", err)
	}

	record := encryptionKey{TagID: id, Tags: tagsJSON, Key: key}

	// ON CONFLICT DO NOTHING handles concurrent inserts for the same tag_id.
	result := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tag_id"}},
		DoNothing: true,
	}).Create(&record)
	if result.Error != nil {
		return nil, fmt.Errorf("could not insert encryption key: %w", result.Error)
	}

	// If another goroutine won the race, read their key.
	if result.RowsAffected == 0 {
		err := s.db.WithContext(ctx).Where("tag_id = ?", id).First(&existing).Error
		if err != nil {
			return nil, fmt.Errorf("could not read encryption key after conflict: %w", err)
		}
		return existing.Key, nil
	}

	return key, nil
}

// Get returns the encryption key for the exact tag set.
// Returns (nil, false, nil) if the key does not exist (e.g. it was forgotten).
func (s *PostgresKeyStore) Get(ctx context.Context, tags map[string]string) ([]byte, bool, error) {
	id := encryption.TagID(tags)

	var existing encryptionKey
	err := s.db.WithContext(ctx).Where("tag_id = ?", id).First(&existing).Error
	if err == gorm.ErrRecordNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("could not query encryption key: %w", err)
	}
	return existing.Key, true, nil
}

// Forget deletes all keys whose tag sets are a superset of the provided tags.
// Uses PostgreSQL's JSONB containment operator (@>) for superset matching.
func (s *PostgresKeyStore) Forget(ctx context.Context, tags map[string]string) error {
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return fmt.Errorf("could not marshal tags for forget: %w", err)
	}
	if err := s.db.WithContext(ctx).Where("tags @> ?::jsonb", string(tagsJSON)).Delete(&encryptionKey{}).Error; err != nil {
		return fmt.Errorf("could not forget encryption keys: %w", err)
	}
	return nil
}
