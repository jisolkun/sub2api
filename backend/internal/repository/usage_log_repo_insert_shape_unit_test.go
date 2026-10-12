//go:build unit

package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

var (
	usageLogStaticInsertShapeRe = regexp.MustCompile(`(?s)INSERT INTO usage_logs \((.*?)\) VALUES \((.*?)\)`)
	usageLogPlaceholderRe       = regexp.MustCompile(`\$(\d+)`)
)

// newSQLCapturingMock 返回把实际下发 SQL 记录到 captured 的 sqlmock；语句一律视为匹配，
// 参数仍由 WithArgs 校验。
func newSQLCapturingMock(t *testing.T, captured *[]string) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	matcher := sqlmock.QueryMatcherFunc(func(_, actualSQL string) error {
		*captured = append(*captured, actualSQL)
		return nil
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

// requireStaticInsertMatchesArgTypes 断言手写的 INSERT：列清单长度与 VALUES 占位符数量
// 都等于 usageLogInsertArgTypes，且占位符恰为 $1..$N 各出现一次。
func requireStaticInsertMatchesArgTypes(t *testing.T, query string) {
	t.Helper()
	m := usageLogStaticInsertShapeRe.FindStringSubmatch(query)
	require.Len(t, m, 3, "unrecognised INSERT shape:\n%s", query)

	want := len(usageLogInsertArgTypes)
	columns := 0
	for _, col := range strings.Split(m[1], ",") {
		if strings.TrimSpace(col) != "" {
			columns++
		}
	}
	require.Equal(t, want, columns, "INSERT column list must match usageLogInsertArgTypes")

	seen := make(map[int]struct{}, want)
	for _, ph := range usageLogPlaceholderRe.FindAllStringSubmatch(m[2], -1) {
		n, err := strconv.Atoi(ph[1])
		require.NoError(t, err)
		_, dup := seen[n]
		require.False(t, dup, "duplicate placeholder $%d", n)
		seen[n] = struct{}{}
	}
	require.Len(t, seen, want, "VALUES placeholder count must match usageLogInsertArgTypes")
	for i := 1; i <= want; i++ {
		_, ok := seen[i]
		require.True(t, ok, "missing placeholder $%d", i)
	}
}

// TestUsageLogStaticInsertShape_PlaceholdersMatchArgTypes 覆盖两条不经占位符生成器、
// 直接手写 $1..$N 的 INSERT 路径，防止加列后漏补占位符只在集成测试才暴露。
func TestUsageLogStaticInsertShape_PlaceholdersMatchArgTypes(t *testing.T) {
	upstreamRequestID := "20260902080329-oneapi"
	log := &service.UsageLog{
		UserID:            1,
		APIKeyID:          2,
		AccountID:         3,
		RequestID:         "client:insert-shape",
		UpstreamRequestID: &upstreamRequestID,
		Model:             "claude-3",
		InputTokens:       10,
		CreatedAt:         time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
	}
	prepared := prepareUsageLogInsert(log)
	args := anySliceToDriverValues(prepared.args)

	t.Run("createSingle", func(t *testing.T) {
		var captured []string
		db, mock := newSQLCapturingMock(t, &captured)
		repo := &usageLogRepository{sql: db}

		mock.ExpectQuery("INSERT INTO usage_logs").
			WithArgs(args...).
			WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(int64(1), log.CreatedAt))

		inserted, err := repo.Create(context.Background(), log)
		require.NoError(t, err)
		require.True(t, inserted)
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, captured, 1)
		requireStaticInsertMatchesArgTypes(t, captured[0])
	})

	t.Run("execUsageLogInsertNoResult", func(t *testing.T) {
		var captured []string
		db, mock := newSQLCapturingMock(t, &captured)

		mock.ExpectExec("INSERT INTO usage_logs").
			WithArgs(args...).
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, execUsageLogInsertNoResult(context.Background(), db, prepared))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, captured, 1)
		requireStaticInsertMatchesArgTypes(t, captured[0])
	})
}

// TestPrepareUsageLogInsert_UpstreamRequestIDArgWiring 把 upstream_request_id 钉在
// session_id 之前，与参数类型表保持同位；缺失时落 NULL 而不是空串。
func TestPrepareUsageLogInsert_UpstreamRequestIDArgWiring(t *testing.T) {
	upstreamRequestID := "req_upstream_123"
	prepared := prepareUsageLogInsert(&service.UsageLog{
		UserID:            1,
		APIKeyID:          2,
		RequestID:         "client:wiring",
		Model:             "gpt-5",
		UpstreamRequestID: &upstreamRequestID,
		CreatedAt:         time.Now().UTC(),
	})
	require.Len(t, prepared.args, len(usageLogInsertArgTypes))

	// 末尾顺序：upstream_request_id, session_id, native_compaction_v2,
	// forced_non_stream, created_at。
	idx := len(prepared.args) - 5
	arg, ok := prepared.args[idx].(sql.NullString)
	require.True(t, ok, "upstream_request_id arg should be sql.NullString, got %T", prepared.args[idx])
	require.True(t, arg.Valid)
	require.Equal(t, upstreamRequestID, arg.String)
	require.Equal(t, "text", usageLogInsertArgTypes[idx])

	absent := prepareUsageLogInsert(&service.UsageLog{UserID: 1, APIKeyID: 2, RequestID: "client:absent", Model: "gpt-5", CreatedAt: time.Now().UTC()})
	nullArg, ok := absent.args[idx].(sql.NullString)
	require.True(t, ok)
	require.False(t, nullArg.Valid, "absent upstream request id must be NULL")

	require.Contains(t, usageLogSelectColumns, "upstream_request_id")
}

// usageLogScanFixtureValue 为 usageLogSelectColumns 的每个列给出与 scanUsageLog
// 目的地类型相容的驱动值；可空列统一给 nil。
func usageLogScanFixtureValue(column string) driver.Value {
	switch column {
	case "id", "user_id", "api_key_id", "account_id", "group_id", "subscription_id", "channel_id",
		"input_tokens", "output_tokens", "cache_creation_tokens", "cache_read_tokens",
		"cache_creation_5m_tokens", "cache_creation_1h_tokens", "image_output_tokens",
		"image_input_tokens", "image_count", "video_count", "billing_type", "request_type",
		"duration_ms", "first_token_ms", "video_duration_seconds":
		return int64(1)
	case "image_output_cost", "image_input_cost", "input_cost", "output_cost",
		"cache_creation_cost", "cache_read_cost", "total_cost", "actual_cost", "rate_multiplier":
		return float64(1)
	case "stream", "forced_non_stream", "openai_ws_mode", "native_compaction_v2",
		"cache_ttl_overridden", "long_context_billing_applied":
		return true
	case "model":
		return "gpt-6-luna"
	case "created_at":
		return time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	default:
		return nil
	}
}

// TestUsageLogSelectColumns_ScanAlignment 钉住 usageLogSelectColumns 与 scanUsageLog
// 目的地清单的对齐关系。回归背景：forced_non_stream 曾被插进 SELECT 但 scan 顺序
// 没同步，整张使用记录列表在 openai_ws_mode 上报 bool→int64 扫描错误。
func TestUsageLogSelectColumns_ScanAlignment(t *testing.T) {
	columns := strings.Split(usageLogSelectColumns, ", ")
	// SELECT 比 INSERT 多一列首列 id（库端生成），其余 63 列与参数类型表一一对应。
	require.Len(t, columns, len(usageLogInsertArgTypes)+1,
		"SELECT column count must be insert arg-types plus the id column")

	idx := func(name string) int {
		pos := slices.Index(columns, name)
		require.GreaterOrEqual(t, pos, 0, "column %q missing from usageLogSelectColumns", name)
		return pos
	}
	// scanUsageLog 的读取顺序：stream, forced_non_stream, openai_ws_mode, ...
	require.Less(t, idx("stream"), idx("forced_non_stream"),
		"forced_non_stream must sit right after stream, matching scanUsageLog")
	require.Less(t, idx("forced_non_stream"), idx("openai_ws_mode"),
		"openai_ws_mode must come after forced_non_stream, matching scanUsageLog")
	require.Equal(t, len(columns)-2, idx("native_compaction_v2"))
	require.Equal(t, len(columns)-1, idx("created_at"))

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	values := make([]driver.Value, 0, len(columns))
	for _, column := range columns {
		values = append(values, usageLogScanFixtureValue(column))
	}
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows(columns).AddRow(values...),
	)

	rows, err := db.Query("SELECT "+usageLogSelectColumns+" FROM usage_logs WHERE id = $1", 1)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	log, err := scanUsageLog(rows)
	require.NoError(t, err, "scanUsageLog destinations must match usageLogSelectColumns positionally")
	require.NotNil(t, log)
	require.Equal(t, "gpt-6-luna", log.Model)
	require.True(t, log.ForcedNonStream)
	require.NoError(t, rows.Err())
}
