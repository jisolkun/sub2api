package repository

import (
	"context"
	"fmt"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// IncrementOpenAIOverloadCount serializes against other increments and manual
// resets on the account row. The flag and counter survive process restarts.
func (r *accountRepository) IncrementOpenAIOverloadCount(ctx context.Context, id int64, threshold int) error {
	if threshold < 0 || threshold > 1000 {
		return fmt.Errorf("invalid overload threshold")
	}
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	txCtx := dbent.NewTxContext(ctx, tx)
	result, err := tx.Client().ExecContext(txCtx, `
UPDATE accounts SET extra = COALESCE(extra, '{}'::jsonb) || jsonb_build_object(
  'openai_overload_count', LEAST(1000, CASE
    WHEN jsonb_typeof(extra->'openai_overload_count') = 'number'
      THEN GREATEST(0, LEAST(1000, (extra->>'openai_overload_count')::numeric))::int
    ELSE 0 END + 1),
  'openai_force_non_stream', NOT COALESCE(extra->'openai_non_stream_unsupported' = 'true'::jsonb, false) AND (COALESCE(extra->'openai_force_non_stream' = 'true'::jsonb, false) OR
    (CASE WHEN jsonb_typeof(extra->'openai_overload_count') = 'number'
      THEN GREATEST(0, LEAST(1000, (extra->>'openai_overload_count')::numeric))::int
    ELSE 0 END + 1 >= $2))
), updated_at = NOW()
WHERE id = $1 AND platform = 'openai' AND deleted_at IS NULL`, id, threshold)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return service.ErrAccountNotFound
	}
	if err := enqueueSchedulerOutbox(txCtx, tx.Client(), service.SchedulerOutboxEventAccountChanged, &id, nil, nil); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.syncSchedulerAccountSnapshot(ctx, id)
	return nil
}
