package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// appendMode is how Append writes: through staging (the default) or straight
// into events under the global lock. Exactly one of them may write to a
// database, because each relies on being the only writer to events.
type appendMode string

const (
	modeStaging appendMode = "staging"
	modeGlobal  appendMode = "global-lock"
)

// ErrAppendModeMismatch is returned when the database records a different
// append mode than the store is configured for: by Migrate, by an append that
// reached a function the database's mode disables, and by RunFlusher on a
// store in global-lock mode.
var ErrAppendModeMismatch = errors.New("the database is in another append mode")

// modeRefused is the SQLSTATE the disabled functions raise.
const modeRefused = "DCB01"

// migrateLock serializes Migrate and SwitchAppendMode across processes, so two
// processes starting at once cannot both find no mode and record different
// ones. ASCII "DCB_mig".
const migrateLock int64 = 0x4443425F6D6967

// WithGlobalLock makes Append write straight into events under one global
// advisory lock instead of through staging: every append serializes, events
// are readable as soon as Append returns, conditions need no tags, and there
// is no flusher. The database must be in the same mode (see Migrate).
func WithGlobalLock() Option {
	return func(s *Store) { s.mode = modeGlobal }
}

// recordedMode returns the mode the database records, or "" for a database no
// mode-aware Migrate has run on yet.
func recordedMode(tx *gorm.DB) (appendMode, error) {
	var exists bool
	if err := tx.Raw(`SELECT to_regprocedure('dcb_append_mode()') IS NOT NULL`).Scan(&exists).Error; err != nil {
		return "", fmt.Errorf("could not look up the append mode: %w", err)
	}
	if !exists {
		return "", nil
	}
	var mode appendMode
	if err := tx.Raw(`SELECT dcb_append_mode()`).Scan(&mode).Error; err != nil {
		return "", fmt.Errorf("could not read the append mode: %w", err)
	}
	return mode, nil
}

// installMode records mode in the database and installs its functions: the
// ones of the other mode are replaced by bodies that refuse, so a process
// still running in that mode gets an error instead of becoming a second writer
// to events. Going to the global lock needs an empty staging table, or what is
// staged there would never become readable.
func installMode(tx *gorm.DB, mode appendMode) error {
	if err := tx.Exec(`SELECT pg_advisory_xact_lock(?)`, migrateLock).Error; err != nil {
		return fmt.Errorf("could not lock the append mode: %w", err)
	}
	if mode == modeGlobal {
		var staged bool
		if err := tx.Raw(`SELECT EXISTS (SELECT 1 FROM staging)`).Scan(&staged).Error; err != nil {
			return fmt.Errorf("could not check the staging table: %w", err)
		}
		if staged {
			return fmt.Errorf("%w: the staging table still holds appends; let a flusher move them before switching to the global lock", ErrAppendModeMismatch)
		}
	}

	functions := []struct{ name, ddl string }{
		{"dcb_append", fmt.Sprintf(dcbAppendFnTemplate, appendLock)},
		{"dcb_append_staging", fmt.Sprintf(dcbAppendStagingFnTemplate, appendLock)},
		{"dcb_flush", dcbFlushFn},
	}
	if mode == modeStaging {
		functions[0].ddl = refusing("dcb_append(p_events jsonb, p_query jsonb, p_after bigint)", "boolean", "appends through staging, not under the global lock")
	} else {
		functions[1].ddl = refusing("dcb_append_staging(p_events jsonb, p_query jsonb, p_after bigint, p_keys bigint[])", "boolean", "appends under the global lock, not through staging")
		functions[2].ddl = refusing("dcb_flush(p_batch int)", "bigint", "appends under the global lock and has no flusher")
	}
	for _, f := range functions {
		if err := tx.Exec(f.ddl).Error; err != nil {
			return fmt.Errorf("could not create the %s function: %w", f.name, err)
		}
	}
	modeFn := fmt.Sprintf(`CREATE OR REPLACE FUNCTION dcb_append_mode() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT '%s'::text $$`, mode)
	if err := tx.Exec(modeFn).Error; err != nil {
		return fmt.Errorf("could not record the append mode: %w", err)
	}
	return nil
}

// refusing is the DDL of a function the database's mode disables.
func refusing(signature, returns, why string) string {
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s RETURNS %s LANGUAGE plpgsql AS $func$
BEGIN
    RAISE EXCEPTION 'dcb: this database %s' USING ERRCODE = '%s';
END;
$func$;`, signature, returns, why, modeRefused)
}

// modeError turns the error of a disabled function into ErrAppendModeMismatch.
func modeError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == modeRefused {
		return fmt.Errorf("%w: %s", ErrAppendModeMismatch, pgErr.Message)
	}
	return err
}

// SwitchAppendMode records this store's append mode in the database, replacing
// the one there, which Migrate never does. Switch before starting processes in
// the new mode: processes still running in the old one get
// ErrAppendModeMismatch on every append. Switching to the global lock needs an
// empty staging table, so stop the staging processes and let a flusher drain
// it first.
func (s *Store) SwitchAppendMode(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return installMode(tx, s.mode)
	})
}
