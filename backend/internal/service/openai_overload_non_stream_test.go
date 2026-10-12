//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const overloadTestMessage = "Our servers are currently overloaded. Please try again later."
const nonStreamTestResponse = `{"id":"resp_test","object":"response","created_at":123,"status":"completed","model":"test-model","output":[{"type":"message","id":"msg_test","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello","annotations":[]}]},{"type":"function_call","id":"fc_test","call_id":"call_test","status":"completed","name":"weather","arguments":"{\"city\":\"Paris\"}"}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}`

type overloadSettingsTestRepo struct {
	SettingRepository
	values map[string]string
}

type overloadMissingThresholdRepo struct{ overloadSettingsTestRepo }

func (r *overloadMissingThresholdRepo) GetValue(_ context.Context, key string) (string, error) {
	if key == SettingKeyOpenAIOverloadNonStreamThreshold {
		return "", ErrSettingNotFound
	}
	return r.values[key], nil
}

func (r *overloadSettingsTestRepo) GetValue(_ context.Context, key string) (string, error) {
	return r.values[key], nil
}
func (r *overloadSettingsTestRepo) GetAll(context.Context) (map[string]string, error) {
	return r.values, nil
}

func (r *overloadSettingsTestRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	result := map[string]string{}
	for _, key := range keys {
		result[key] = r.values[key]
	}
	return result, nil
}

type overloadCounterTestRepo struct {
	AccountRepository
	count     int
	threshold int
	updates   map[string]any
}

func (r *overloadCounterTestRepo) UpdateExtra(_ context.Context, _ int64, updates map[string]any) error {
	r.updates = updates
	return nil
}

func (r *overloadCounterTestRepo) IncrementOpenAIOverloadCount(_ context.Context, _ int64, threshold int) error {
	r.count++
	r.threshold = threshold
	return nil
}

func overloadTestSettings(enabled bool, threshold string) *SettingService {
	value := "false"
	if enabled {
		value = "true"
	}
	return NewSettingService(&overloadSettingsTestRepo{values: map[string]string{
		SettingKeyOpenAIOverloadNonStreamEnabled: value, SettingKeyOpenAIOverloadNonStreamThreshold: threshold,
	}}, &config.Config{})
}

func TestOpenAIOverloadSettingsDefaultsAndCacheInvalidation(t *testing.T) {
	settings := overloadTestSettings(false, "")
	got := settings.GetOpenAIOverloadNonStreamSettings(context.Background())
	require.False(t, got.Enabled)
	require.Equal(t, 1, got.Threshold)
	repo := settings.settingRepo.(*overloadSettingsTestRepo)
	repo.values[SettingKeyOpenAIOverloadNonStreamEnabled] = "true"
	repo.values[SettingKeyOpenAIOverloadNonStreamThreshold] = "3"
	settings.invalidateOpenAIOverloadNonStreamSettings()
	got = settings.GetOpenAIOverloadNonStreamSettings(context.Background())
	require.True(t, got.Enabled)
	require.Equal(t, 3, got.Threshold)
}

func TestOpenAIOverloadSettingsMissingThresholdUsesOne(t *testing.T) {
	s := NewSettingService(&overloadMissingThresholdRepo{overloadSettingsTestRepo{values: map[string]string{SettingKeyOpenAIOverloadNonStreamEnabled: "true"}}}, &config.Config{})
	got := s.GetOpenAIOverloadNonStreamSettings(context.Background())
	require.True(t, got.Enabled)
	require.Equal(t, 1, got.Threshold)
}

func TestOpenAIOverloadZeroThresholdForcesAllAccounts(t *testing.T) {
	for _, tc := range []struct {
		enabled      bool
		threshold    string
		extra        map[string]any
		expectForced bool
	}{
		{enabled: true, threshold: "0", expectForced: true},
		{enabled: true, threshold: "0", extra: map[string]any{OpenAIOverloadCountExtraKey: 7}, expectForced: true},
		{enabled: true, threshold: "0", extra: map[string]any{OpenAINonStreamUnsupportedExtraKey: true}, expectForced: true},
		{enabled: false, threshold: "0", expectForced: false},
		{enabled: true, threshold: "1", extra: map[string]any{OpenAIOverloadCountExtraKey: 7}, expectForced: false},
	} {
		s := &OpenAIGatewayService{settingService: overloadTestSettings(tc.enabled, tc.threshold)}
		a := &Account{ID: 1, Platform: PlatformOpenAI, Extra: tc.extra}
		require.Equal(t, tc.expectForced, s.forceOpenAINonStream(context.Background(), a), "enabled=%v threshold=%s extra=%v", tc.enabled, tc.threshold, tc.extra)
	}
}

func TestOpenAIOverloadObserverCountsErrorPairOnceAcrossReadBoundaries(t *testing.T) {
	stream := "data: {\"type\":\"error\",\"error\":{\"message\":\"" + overloadTestMessage + "\"}}\n\n" +
		"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"" + overloadTestMessage + "\"}}}\n\n"
	count := 0
	reader := &openAIOverloadObservingBody{ReadCloser: io.NopCloser(strings.NewReader(stream)), onOverload: func() { count++ }}
	var output strings.Builder
	p := make([]byte, 3)
	for {
		n, err := reader.Read(p)
		output.Write(p[:n])
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, stream, output.String())
	require.Equal(t, 1, count)
}

func TestOpenAIOverloadObserverIgnoresSuccessEchoAndOtherErrors(t *testing.T) {
	for _, body := range []string{
		`{"output":[{"text":"` + overloadTestMessage + `"}]}`,
		`data: {"type":"response.output_text.delta","delta":"` + overloadTestMessage + `"}` + "\n\n",
		`{"error":{"message":"Rate limit exceeded"}}`,
		`data: {"error":{"code":"server_is_overloaded","message":"overloaded"}}` + "\n\n",
	} {
		reader := &openAIOverloadObservingBody{ReadCloser: io.NopCloser(strings.NewReader(body)), onOverload: func() { t.Fatal("unexpected trigger") }}
		out, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, body, string(out))
	}
}

func TestOpenAIOverloadObserverRecognizesJSONAndDisabledSettings(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		repo := &overloadCounterTestRepo{}
		s := &OpenAIGatewayService{accountRepo: repo, settingService: overloadTestSettings(enabled, "3")}
		a := &Account{ID: 1, Platform: PlatformOpenAI}
		reader := &openAIOverloadObservingBody{ReadCloser: io.NopCloser(strings.NewReader(`{"error":{"message":"` + overloadTestMessage + `"}}`)), onOverload: func() { s.recordOpenAIOverload(context.Background(), a) }}
		_, err := io.ReadAll(reader)
		require.NoError(t, err)
		if enabled {
			require.Equal(t, 1, repo.count)
			require.Equal(t, 3, repo.threshold)
		} else {
			require.Zero(t, repo.count)
		}
	}
}

func TestOpenAIOverloadForcedRequestPreservesOriginalAndHonorsToggle(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s := &OpenAIGatewayService{settingService: overloadTestSettings(enabled, "1")}
		a := &Account{Platform: PlatformOpenAI, Extra: map[string]any{OpenAIForceNonStreamExtraKey: true}}
		original := `{"model":"test-model","stream":true,"input":"hi"}`
		req, err := http.NewRequest(http.MethodPost, "https://example.com/v1/responses", strings.NewReader(original))
		require.NoError(t, err)
		req.Header.Set("Accept", "text/event-stream")
		prepared, bridge, err := s.prepareOpenAIForcedNonStreamRequest(req, a)
		require.NoError(t, err)
		body, err := io.ReadAll(prepared.Body)
		require.NoError(t, err)
		require.Equal(t, enabled, bridge)
		require.Equal(t, !enabled, gjson.GetBytes(body, "stream").Bool())
		require.Equal(t, "text/event-stream", req.Header.Get("Accept"))
		if enabled {
			require.Equal(t, "application/json", prepared.Header.Get("Accept"))
			require.Equal(t, int64(len(body)), prepared.ContentLength)
		}
	}
}

func TestOpenAINonStreamSSEContainsTextToolsAndUsage(t *testing.T) {
	stream, err := buildOpenAINonStreamResponsesSSE([]byte(nonStreamTestResponse))
	require.NoError(t, err)
	sequence := int64(0)
	for _, line := range strings.Split(string(stream), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := []byte(strings.TrimPrefix(line, "data: "))
		require.Equal(t, sequence, gjson.GetBytes(payload, "sequence_number").Int())
		sequence++
	}
	require.Contains(t, string(stream), `"delta":"Hello"`)
	require.Contains(t, string(stream), "event: response.function_call_arguments.delta")
	require.Contains(t, string(stream), "event: response.completed")
	final, ok := extractCodexFinalResponse(string(stream))
	require.True(t, ok)
	require.JSONEq(t, nonStreamTestResponse, string(final))
}

func TestOpenAINonStreamSSERejectsIncompleteResponseAndPreservesFailure(t *testing.T) {
	for _, body := range []string{`not json`, `{"status":"in_progress","output":[]}`, `{"status":"completed","output":[]}`} {
		_, err := buildOpenAINonStreamResponsesSSE([]byte(body))
		require.Error(t, err)
	}
	stream, err := buildOpenAINonStreamResponsesSSE([]byte(`{"id":"resp_failure","status":"failed","error":{"message":"` + overloadTestMessage + `"}}`))
	require.NoError(t, err)
	require.Contains(t, string(stream), "event: response.failed")
	require.NotContains(t, string(stream), "event: response.completed")
}

func TestOpenAIOverloadDoUpstreamReturnsSSEForForcedAccount(t *testing.T) {
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(nonStreamTestResponse))}}
	s := &OpenAIGatewayService{httpUpstream: upstream, cfg: &config.Config{}, settingService: overloadTestSettings(true, "1")}
	a := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{OpenAIForceNonStreamExtraKey: true}}
	req, err := http.NewRequest(http.MethodPost, "https://example.com/v1/responses", strings.NewReader(`{"stream":true,"input":"hi"}`))
	require.NoError(t, err)
	resp, err := s.doOpenAIUpstream(req, "", a)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.False(t, gjson.GetBytes(upstream.lastBody, "stream").Bool())
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	stream, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(stream), "event: response.output_text.delta")
	final, ok := extractCodexFinalResponse(string(stream))
	require.True(t, ok)
	require.JSONEq(t, nonStreamTestResponse, string(final))
}

func TestOpenAIOverloadUnsupportedUpstreamFallsBackOnceAndMarksAccount(t *testing.T) {
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{StatusCode: 400, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"detail":"Stream must be set to true."}`))},
		{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":" + nonStreamTestResponse + "}\n\n"))},
	}}
	repo := &overloadCounterTestRepo{}
	s := &OpenAIGatewayService{httpUpstream: upstream, accountRepo: repo, cfg: &config.Config{}, settingService: overloadTestSettings(true, "1")}
	a := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{OpenAIForceNonStreamExtraKey: true}}
	req, err := http.NewRequest(http.MethodPost, "https://example.com/v1/responses", strings.NewReader(`{"stream":true,"input":"hi"}`))
	require.NoError(t, err)
	resp, err := s.doOpenAIUpstream(req, "", a)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Len(t, upstream.bodies, 2)
	require.False(t, gjson.GetBytes(upstream.bodies[0], "stream").Bool())
	require.True(t, gjson.GetBytes(upstream.bodies[1], "stream").Bool())
	require.Equal(t, false, repo.updates[OpenAIForceNonStreamExtraKey])
	require.Equal(t, true, repo.updates[OpenAINonStreamUnsupportedExtraKey])
	stream, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(stream), "response.completed")
	a.Extra = repo.updates
	require.False(t, s.forceOpenAINonStream(context.Background(), a))
}

func TestOpenAIOverloadOtherBadRequestDoesNotRetry(t *testing.T) {
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"message":"Invalid input"}}`))}}
	repo := &overloadCounterTestRepo{}
	s := &OpenAIGatewayService{httpUpstream: upstream, accountRepo: repo, cfg: &config.Config{}, settingService: overloadTestSettings(true, "1")}
	a := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{OpenAIForceNonStreamExtraKey: true}}
	req, err := http.NewRequest(http.MethodPost, "https://example.com/v1/responses", strings.NewReader(`{"stream":true}`))
	require.NoError(t, err)
	resp, err := s.doOpenAIUpstream(req, "", a)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Len(t, upstream.bodies, 1)
	require.Empty(t, repo.updates)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.JSONEq(t, `{"error":{"message":"Invalid input"}}`, string(body))
}

func TestOpenAINonStreamSSEPreservesIncompleteTerminal(t *testing.T) {
	stream, err := buildOpenAINonStreamResponsesSSE([]byte(`{"status":"incomplete","id":"resp_1","output":[],"incomplete_details":{"reason":"max_output_tokens"}}`))
	require.NoError(t, err)
	require.Contains(t, string(stream), "event: response.incomplete")
	require.NotContains(t, string(stream), "event: response.completed")
}

func TestOpenAIOverloadForwardKeepsClientStreamingInBothPaths(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "standard", true: "passthrough"}[passthrough], func(t *testing.T) {
			setup := newAstraOAuthSetup(t, passthrough)
			setup.svc.settingService = overloadTestSettings(true, "1")
			setup.svc.toolCorrector = NewCodexToolCorrector()
			setup.account.Extra[OpenAIForceNonStreamExtraKey] = true
			setup.upstream.resp = &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(nonStreamTestResponse))}
			result, err := setup.svc.Forward(context.Background(), setup.c, setup.account, astraRequestBody("gpt-5.4", true, "", ""))
			require.NoError(t, err)
			require.False(t, gjson.GetBytes(setup.upstream.lastBody, "stream").Bool())
			require.True(t, result.Stream)
			require.Equal(t, 2, result.Usage.InputTokens)
			require.Equal(t, 3, result.Usage.OutputTokens)
			require.Contains(t, setup.rec.Header().Get("Content-Type"), "text/event-stream")
			require.Contains(t, setup.rec.Body.String(), "response.output_text.delta")
			require.Contains(t, setup.rec.Body.String(), "Hello")
			require.Contains(t, setup.rec.Body.String(), "response.completed")
		})
	}
}

func TestOpenAIOverloadChatCompatibilityKeepsStreamingTextAndTools(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(nonStreamTestResponse))}}
	s := &OpenAIGatewayService{httpUpstream: upstream, cfg: &config.Config{}, settingService: overloadTestSettings(true, "1"), toolCorrector: NewCodexToolCorrector()}
	a := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"access_token": "test-token"}, Extra: map[string]any{OpenAIForceNonStreamExtraKey: true}}
	result, err := s.ForwardAsChatCompletions(context.Background(), c, a, []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hi"}],"stream":true}`), "", "")
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(upstream.lastBody, "stream").Bool())
	require.True(t, result.Stream)
	require.Contains(t, rec.Body.String(), "Hello")
	require.Contains(t, rec.Body.String(), "weather")
	require.Contains(t, rec.Body.String(), "[DONE]")
}

func TestOpenAIOverloadChatCompatibilityKeepsSynchronousClientJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(nonStreamTestResponse))}}
	s := &OpenAIGatewayService{httpUpstream: upstream, cfg: &config.Config{}, settingService: overloadTestSettings(true, "1"), toolCorrector: NewCodexToolCorrector()}
	a := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"access_token": "test-token"}, Extra: map[string]any{OpenAIForceNonStreamExtraKey: true}}
	result, err := s.ForwardAsChatCompletions(context.Background(), c, a, []byte(`{"model":"gpt-6-luna","messages":[{"role":"user","content":"hi"}],"stream":false}`), "", "")
	require.NoError(t, err)
	require.False(t, result.Stream)
	require.False(t, gjson.GetBytes(upstream.lastBody, "stream").Bool())
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.Equal(t, "Hello", gjson.Get(rec.Body.String(), "choices.0.message.content").String())
	require.NotContains(t, rec.Body.String(), "data:")
}

func TestOpenAINonStreamSSEPreservesRefusal(t *testing.T) {
	stream, err := buildOpenAINonStreamResponsesSSE([]byte(`{"id":"resp_refusal","status":"completed","output":[{"id":"msg_refusal","type":"message","role":"assistant","content":[{"type":"refusal","refusal":"Cannot help"}]}]}`))
	require.NoError(t, err)
	require.Contains(t, string(stream), "event: response.refusal.delta")
	require.Contains(t, string(stream), `"delta":"Cannot help"`)
}
