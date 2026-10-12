package admin

import (
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAIOverloadSettingsPartialUpdateAndValidation(t *testing.T) {
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{service.SettingKeyOpenAIOverloadNonStreamEnabled: "true", service.SettingKeyOpenAIOverloadNonStreamThreshold: "3"})
	rec := doUpdateSettings(t, h, map[string]any{"openai_overload_non_stream_threshold": 5}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "true", repo.values[service.SettingKeyOpenAIOverloadNonStreamEnabled])
	require.Equal(t, "5", repo.values[service.SettingKeyOpenAIOverloadNonStreamThreshold])
	for _, value := range []any{0, -1, 1001, 1.5} {
		rec = doUpdateSettings(t, h, map[string]any{"openai_overload_non_stream_threshold": value}, nil)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Equal(t, "5", repo.values[service.SettingKeyOpenAIOverloadNonStreamThreshold])
	}
	rec = doUpdateSettings(t, h, map[string]any{"openai_overload_non_stream_enabled": false}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "false", repo.values[service.SettingKeyOpenAIOverloadNonStreamEnabled])
	require.Equal(t, "5", repo.values[service.SettingKeyOpenAIOverloadNonStreamThreshold])
}
