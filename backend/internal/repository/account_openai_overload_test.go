package repository

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Opt-in real PostgreSQL check. It creates an isolated schema rather than
// migrating or modifying any existing application tables.
func TestOpenAIOverloadCounterPostgres(t *testing.T) {
	dsn := os.Getenv("SUB2API_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SUB2API_TEST_POSTGRES_DSN to an isolated test PostgreSQL")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	schema := fmt.Sprintf("overload_test_%d", time.Now().UnixNano())
	_, err = db.Exec("CREATE SCHEMA " + schema)
	require.NoError(t, err)
	defer db.Exec("DROP SCHEMA " + schema + " CASCADE")
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	isolated, err := sql.Open("postgres", parsed.String())
	require.NoError(t, err)
	defer isolated.Close()
	_, err = isolated.Exec(`CREATE TABLE accounts(id bigint PRIMARY KEY,platform text,extra jsonb,updated_at timestamptz,deleted_at timestamptz);
CREATE TABLE scheduler_outbox(event_type text,account_id bigint,group_id bigint,payload jsonb,dedup_key text);
CREATE UNIQUE INDEX outbox_dedup ON scheduler_outbox(dedup_key) WHERE dedup_key IS NOT NULL;
INSERT INTO accounts(id,platform,extra) VALUES(1,'openai','{"unrelated":"preserved"}'),(2,'openai','{}');`)
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, isolated)))
	defer client.Close()
	repo := newAccountRepositoryWithSQL(client, isolated, nil)
	ctx := context.Background()
	read := func(id int64) (int, bool) {
		var count int
		var active bool
		err := isolated.QueryRow(`SELECT COALESCE((extra->>'openai_overload_count')::int,0),COALESCE((extra->>'openai_force_non_stream')::bool,false) FROM accounts WHERE id=$1`, id).Scan(&count, &active)
		require.NoError(t, err)
		return count, active
	}
	require.NoError(t, repo.IncrementOpenAIOverloadCount(ctx, 1, 3))
	count, active := read(1)
	require.Equal(t, 1, count)
	require.False(t, active)
	require.NoError(t, repo.IncrementOpenAIOverloadCount(ctx, 1, 3))
	count, active = read(1)
	require.Equal(t, 2, count)
	require.False(t, active)
	require.NoError(t, repo.IncrementOpenAIOverloadCount(ctx, 1, 3))
	count, active = read(1)
	require.Equal(t, 3, count)
	require.True(t, active)
	count, active = read(2)
	require.Zero(t, count)
	require.False(t, active)
	require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{service.OpenAIForceNonStreamExtraKey: false, service.OpenAIOverloadCountExtraKey: 0}))
	count, active = read(1)
	require.Zero(t, count)
	require.False(t, active)
	var wg sync.WaitGroup
	errors := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errors <- repo.IncrementOpenAIOverloadCount(ctx, 1, 3) }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	count, active = read(1)
	require.Equal(t, 20, count)
	require.True(t, active)
	var unrelated string
	require.NoError(t, isolated.QueryRow(`SELECT extra->>'unrelated' FROM accounts WHERE id=1`).Scan(&unrelated))
	require.Equal(t, "preserved", unrelated)
	require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{service.OpenAIForceNonStreamExtraKey: false, service.OpenAINonStreamUnsupportedExtraKey: true}))
	require.NoError(t, repo.IncrementOpenAIOverloadCount(ctx, 1, 1))
	_, active = read(1)
	require.False(t, active)
}
