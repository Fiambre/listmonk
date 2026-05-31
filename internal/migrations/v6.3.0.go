package migrations

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/stuffbin"
)

func V6_3_0(db *sqlx.DB, fs stuffbin.FileSystem, ko *koanf.Koanf, lo *log.Logger) error {
	// Add pause_reason column to campaigns. Idempotent via IF NOT EXISTS.
	if _, err := db.Exec(`
		ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS pause_reason TEXT DEFAULT NULL;
	`); err != nil {
		return err
	}

	// Add 5 new per-SMTP fields to the smtp settings JSON array.
	// Migrates existing global message_rate and sliding_window values to each SMTP entry.
	// Idempotent: only updates rows where at least one entry is missing a new field.
	if _, err := db.Exec(`
		UPDATE settings SET value = s.updated
		FROM (
			SELECT JSONB_AGG(
				v
				|| CASE WHEN NOT (v ? 'max_rate') THEN
				       JSONB_BUILD_OBJECT('max_rate',
				           COALESCE((SELECT value::int FROM settings WHERE key='app.message_rate'), 0))
				   ELSE '{}'::JSONB END
				|| CASE WHEN NOT (v ? 'sliding_window') THEN
				       JSONB_BUILD_OBJECT('sliding_window',
				           COALESCE((SELECT value::bool FROM settings WHERE key='app.message_sliding_window'), false))
				   ELSE '{}'::JSONB END
				|| CASE WHEN NOT (v ? 'sliding_window_rate') THEN
				       JSONB_BUILD_OBJECT('sliding_window_rate',
				           COALESCE((SELECT value::int FROM settings WHERE key='app.message_sliding_window_rate'), 0))
				   ELSE '{}'::JSONB END
				|| CASE WHEN NOT (v ? 'sliding_window_duration') THEN
				       JSONB_BUILD_OBJECT('sliding_window_duration',
				           COALESCE((SELECT value#>>'{}' FROM settings WHERE key='app.message_sliding_window_duration'), ''))
				   ELSE '{}'::JSONB END
				|| CASE WHEN NOT (v ? 'daily_send_quota') THEN
				       JSONB_BUILD_OBJECT('daily_send_quota', 0)
				   ELSE '{}'::JSONB END
			) AS updated FROM settings, JSONB_ARRAY_ELEMENTS(value) v WHERE key = 'smtp'
		) s WHERE key = 'smtp'
		AND EXISTS (
			SELECT 1 FROM JSONB_ARRAY_ELEMENTS(value) v
			WHERE NOT (v ? 'max_rate') OR NOT (v ? 'daily_send_quota')
		);
	`); err != nil {
		return err
	}

	return nil
}
