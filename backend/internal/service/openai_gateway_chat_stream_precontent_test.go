//go:build unit

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// preContentUpstreamSSE 复用 cyber 测试的构造方式：response.created 之后接给定事件，
// 每个事件独立成帧（data 行后跟空行）。
func preContentUpstreamSSE(events ...string) string {
	lines := []string{
		`data: {"type":"response.created","response":{"id":"resp_pre","model":"gpt-6-luna","status":"in_progress","output":[]}}`,
		"",
	}
	for _, event := range events {
		lines = append(lines, event, "")
	}
	return strings.Join(lines, "\n")
}

func preContentUpstreamRecorder(body string) *httpUpstreamRecorder {
	return &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}}
}

// 复现线上问题：上游在 response.created 之后立刻返回 529 过载 error 事件。
// role chunk 属于 bookkeeping，必须被预内容缓冲扣留，保证 clientOutputStarted
// 仍为 false，让过载错误进入 failover（同账号有界重试 / 切号）而不是转发给客户端。
func TestForwardAsChatCompletions_StreamOverloadBeforeContentFailsOver(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-6-luna","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	overloadErrEvent := `data: {"type":"error","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later.","param":null,"type":"service_unavailable_error"},"sequence_number":2}`
	svc := &OpenAIGatewayService{httpUpstream: preContentUpstreamRecorder(preContentUpstreamSSE(overloadErrEvent))}

	result, err := svc.ForwardAsChatCompletions(context.Background(), c, compatCyberOAuthAccount(), body, "", "gpt-6-luna")
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr), "overload before content must trigger failover, got %v", err)
	require.Nil(t, result)
	require.Empty(t, rec.Body.String(), "no bytes may reach the client before failover")
}

// 正常流：role chunk 在首个内容 chunk 之前按序补发，流结尾仍有 [DONE]。
func TestForwardAsChatCompletions_StreamReleasesHeldRoleChunkBeforeContent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-6-luna","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	sse := preContentUpstreamSSE(
		`data: {"type":"response.output_text.delta","delta":"Hello"}`,
		`data: {"type":"response.completed","response":{"id":"resp_pre","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`,
	)
	svc := &OpenAIGatewayService{httpUpstream: preContentUpstreamRecorder(sse)}

	result, err := svc.ForwardAsChatCompletions(context.Background(), c, compatCyberOAuthAccount(), body, "", "gpt-6-luna")
	require.NoError(t, err)
	require.NotNil(t, result)
	out := rec.Body.String()
	require.Contains(t, out, `"role":"assistant"`, "role chunk must still be delivered")
	require.Contains(t, out, "Hello")
	require.Contains(t, out, "data: [DONE]")
	require.Less(t,
		strings.Index(out, `"role":"assistant"`),
		strings.Index(out, "Hello"),
		"role chunk must precede the first content chunk",
	)
}

// 预内容缓冲下 keepalive 不得写出任何字节（否则 handler 侧 failover 窗口被关闭）。
func TestChatStreamKeepaliveSkipsWhilePreContentHold(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-6-luna","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	overloadErrEvent := `data: {"type":"error","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later.","param":null,"type":"service_unavailable_error"},"sequence_number":2}`
	svc := &OpenAIGatewayService{httpUpstream: preContentUpstreamRecorder(preContentUpstreamSSE(overloadErrEvent))}
	svc.cfg = nil

	result, err := svc.ForwardAsChatCompletions(context.Background(), c, compatCyberOAuthAccount(), body, "", "gpt-6-luna")
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr))
	require.Nil(t, result)
	require.Empty(t, rec.Body.String())
}

// forced_non_stream 标记只应出现在客户端确实要求流式的请求上：
// 客户端本就非流式的请求被统一改写不算「流式(非流)」。
func TestOpenAIOverloadForcedNonStreamMarksOnlyWhenClientWantsSSE(t *testing.T) {
	s := &OpenAIGatewayService{settingService: overloadTestSettings(true, "0")}
	a := &Account{ID: 1, Platform: PlatformOpenAI}
	for _, wantsSSE := range []bool{true, false} {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		stream := "false"
		if wantsSSE {
			stream = "true"
		}
		original := `{"model":"m","stream":` + stream + `,"input":"hi"}`
		req, err := http.NewRequest(http.MethodPost, "https://example.com/v1/responses", strings.NewReader(original))
		require.NoError(t, err)
		req = req.WithContext(WithOpenAIGinContext(t.Context(), c))

		prepared, _, err := s.prepareOpenAIForcedNonStreamRequest(req, a)
		require.NoError(t, err)
		out, err := io.ReadAll(prepared.Body)
		require.NoError(t, err)
		require.False(t, gjson.GetBytes(out, "stream").Bool(), "upstream body must be rewritten to stream=false")
		require.Equal(t, wantsSSE, GetOpenAIForcedNonStream(c),
			"forced_non_stream tag must follow the client's stream request")
	}
}
