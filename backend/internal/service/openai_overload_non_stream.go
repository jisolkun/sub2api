package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	SettingKeyOpenAIOverloadNonStreamEnabled   = "openai_overload_non_stream_enabled"
	SettingKeyOpenAIOverloadNonStreamThreshold = "openai_overload_non_stream_threshold"
	OpenAIForceNonStreamExtraKey               = "openai_force_non_stream"
	OpenAIOverloadCountExtraKey                = "openai_overload_count"
	OpenAINonStreamUnsupportedExtraKey         = "openai_non_stream_unsupported"
	openAIOverloadMessage                      = "our servers are currently overloaded. please try again later"
)

type openAIOverloadNonStreamSettings struct {
	Enabled   bool
	Threshold int
	ExpiresAt time.Time
}

func parseOpenAIOverloadNonStreamSettings(values map[string]string) openAIOverloadNonStreamSettings {
	n, err := strconv.Atoi(values[SettingKeyOpenAIOverloadNonStreamThreshold])
	if err != nil || n < 0 || n > 1000 {
		n = 1
	}
	return openAIOverloadNonStreamSettings{Enabled: values[SettingKeyOpenAIOverloadNonStreamEnabled] == "true", Threshold: n}
}

func (s *SettingService) GetOpenAIOverloadNonStreamSettings(ctx context.Context) openAIOverloadNonStreamSettings {
	if s == nil || s.settingRepo == nil {
		return openAIOverloadNonStreamSettings{Threshold: 1}
	}
	if cached, ok := s.openAIOverloadNonStreamCache.Load().(*openAIOverloadNonStreamSettings); ok && time.Now().Before(cached.ExpiresAt) {
		return *cached
	}
	// Serialize refreshes and settings writes so a stale read cannot overwrite a save.
	s.openAIOverloadNonStreamMu.Lock()
	defer s.openAIOverloadNonStreamMu.Unlock()
	if cached, ok := s.openAIOverloadNonStreamCache.Load().(*openAIOverloadNonStreamSettings); ok && time.Now().Before(cached.ExpiresAt) {
		return *cached
	}
	dbCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	enabled, err := s.settingRepo.GetValue(dbCtx, SettingKeyOpenAIOverloadNonStreamEnabled)
	threshold, thresholdErr := s.settingRepo.GetValue(dbCtx, SettingKeyOpenAIOverloadNonStreamThreshold)
	values := map[string]string{SettingKeyOpenAIOverloadNonStreamEnabled: enabled, SettingKeyOpenAIOverloadNonStreamThreshold: threshold}
	result := parseOpenAIOverloadNonStreamSettings(values)
	if (err != nil && !errors.Is(err, ErrSettingNotFound)) || (thresholdErr != nil && !errors.Is(thresholdErr, ErrSettingNotFound)) {
		result.Enabled = false
	}
	result.ExpiresAt = time.Now().Add(5 * time.Second)
	s.openAIOverloadNonStreamCache.Store(&result)
	return result
}

func (s *SettingService) invalidateOpenAIOverloadNonStreamSettings() {
	s.openAIOverloadNonStreamMu.Lock()
	defer s.openAIOverloadNonStreamMu.Unlock()
	s.openAIOverloadNonStreamCache.Store(&openAIOverloadNonStreamSettings{})
}

func (s *OpenAIGatewayService) forceOpenAINonStream(ctx context.Context, account *Account) bool {
	if s == nil || account == nil || account.Platform != PlatformOpenAI {
		return false
	}
	settings := s.settingService.GetOpenAIOverloadNonStreamSettings(ctx)
	if !settings.Enabled {
		return false
	}
	// A threshold of 0 forces every account onto the non-streaming upstream
	// without waiting for overload errors; accounts whose upstream explicitly
	// rejects non-streaming keep their regular streaming path.
	if settings.Threshold == 0 {
		unsupported, _ := account.Extra[OpenAINonStreamUnsupportedExtraKey].(bool)
		return !unsupported
	}
	active, _ := account.Extra[OpenAIForceNonStreamExtraKey].(bool)
	unsupported, _ := account.Extra[OpenAINonStreamUnsupportedExtraKey].(bool)
	return active && !unsupported
}

// Counting is a single database update, shared across concurrent requests and instances.
type openAIOverloadCounterRepository interface {
	IncrementOpenAIOverloadCount(context.Context, int64, int) error
}

func (s *OpenAIGatewayService) recordOpenAIOverload(ctx context.Context, account *Account) {
	if s == nil || account == nil || account.Platform != PlatformOpenAI {
		return
	}
	if unsupported, _ := account.Extra[OpenAINonStreamUnsupportedExtraKey].(bool); unsupported {
		return
	}
	settings := s.settingService.GetOpenAIOverloadNonStreamSettings(ctx)
	if !settings.Enabled {
		return
	}
	repo, ok := s.accountRepo.(openAIOverloadCounterRepository)
	if !ok {
		return
	}
	stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := repo.IncrementOpenAIOverloadCount(stateCtx, account.ID, settings.Threshold); err != nil {
		slog.Warn("failed to record OpenAI overload", "account_id", account.ID, "error", err)
	}
}

func isOpenAIOverloadErrorPayload(payload []byte) bool {
	for _, path := range []string{"error.message", "response.error.message"} {
		if strings.Contains(strings.ToLower(gjson.GetBytes(payload, path).String()), openAIOverloadMessage) {
			return true
		}
	}
	return false
}

// Inspect error envelopes only. A successful output quoting the message must not trigger.
// Each upstream attempt counts once, even when error and response.failed both arrive.
type openAIOverloadObservingBody struct {
	io.ReadCloser
	onOverload   func()
	pending      []byte
	discardLine  bool
	jsonBody     []byte
	jsonTooLarge bool
	once         sync.Once
}

func (b *openAIOverloadObservingBody) inspect(payload []byte) {
	if isOpenAIOverloadErrorPayload(payload) {
		b.once.Do(b.onOverload)
	}
}

func (b *openAIOverloadObservingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if !b.jsonTooLarge {
		if len(b.jsonBody)+n <= 64*1024 {
			b.jsonBody = append(b.jsonBody, p[:n]...)
		} else {
			b.jsonTooLarge = true
			b.jsonBody = nil
		}
	}
	for _, ch := range p[:n] {
		if ch == '\n' {
			if !b.discardLine {
				line := bytes.TrimSpace(b.pending)
				if bytes.HasPrefix(line, []byte("data:")) {
					b.inspect(bytes.TrimSpace(line[5:]))
				}
			}
			b.pending = b.pending[:0]
			b.discardLine = false
		} else if !b.discardLine {
			if len(b.pending) < 64*1024 {
				b.pending = append(b.pending, ch)
			} else {
				b.pending = b.pending[:0]
				b.discardLine = true
			}
		}
	}
	if err != nil {
		b.inspect(b.jsonBody)
		line := bytes.TrimSpace(b.pending)
		if !b.discardLine && bytes.HasPrefix(line, []byte("data:")) {
			b.inspect(bytes.TrimSpace(line[5:]))
		}
	}
	return n, err
}

func (s *OpenAIGatewayService) prepareOpenAIForcedNonStreamRequest(request *http.Request, account *Account) (*http.Request, bool, error) {
	if !s.forceOpenAINonStream(request.Context(), account) || !strings.HasSuffix(strings.TrimRight(request.URL.Path, "/"), "/responses") || request.GetBody == nil {
		return request, false, nil
	}
	reader, err := request.GetBody()
	if err != nil {
		return nil, false, err
	}
	body, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return nil, false, err
	}
	wantsSSE := gjson.GetBytes(body, "stream").Bool()
	body, err = sjson.SetBytes(body, "stream", false)
	if err != nil {
		return nil, false, err
	}
	clone := request.Clone(request.Context())
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	clone.ContentLength = int64(len(body))
	clone.Header.Del("Content-Length")
	clone.Header.Set("Accept", "application/json")
	return clone, wantsSSE, nil
}

func openAINonStreamExplicitlyUnsupported(body []byte) bool {
	for _, path := range []string{"detail", "error.message"} {
		message := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, path).String())), ".")
		if message == "stream must be set to true" || message == "streaming must be set to true" {
			return true
		}
	}
	return false
}

// This 400 is a protocol rejection before generation. Retry once in the
// accepted mode, and remember the limitation so later requests do not probe it.
func (s *OpenAIGatewayService) retryUnsupportedOpenAINonStream(request *http.Request, proxyURL string, account *Account, resp *http.Response) (*http.Response, error) {
	body, err := ReadUpstreamResponseBody(resp.Body, s.cfg, nil, nil)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if !openAINonStreamExplicitlyUnsupported(body) {
		return resp, nil
	}
	if s.accountRepo != nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(request.Context()), 5*time.Second)
		err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{OpenAIForceNonStreamExtraKey: false, OpenAINonStreamUnsupportedExtraKey: true})
		cancel()
		if err != nil {
			slog.Warn("failed to save OpenAI non-stream capability", "account_id", account.ID, "error", err)
		}
	}
	reader, err := request.GetBody()
	if err != nil {
		return nil, err
	}
	retryBody, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return nil, err
	}
	retryBody, err = sjson.SetBytes(retryBody, "stream", true)
	if err != nil {
		return nil, err
	}
	retry := request.Clone(request.Context())
	retry.Body = io.NopCloser(bytes.NewReader(retryBody))
	retry.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(retryBody)), nil }
	retry.ContentLength = int64(len(retryBody))
	retry.Header.Del("Content-Length")
	retry.Header.Set("Accept", "text/event-stream")
	return s.doUpstreamWithProxyFallback(retry.Context(), retry, account, proxyURL)
}

// Convert on first Read, retaining cancellation/body ownership and the ordinary
// streaming parsers, tool restoration, usage accounting and protocol adapters.
type openAIForcedNonStreamBody struct {
	io.ReadCloser
	s       *OpenAIGatewayService
	reader  io.Reader
	readErr error
	once    sync.Once
}

func (b *openAIForcedNonStreamBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		body, err := ReadUpstreamResponseBody(b.ReadCloser, b.s.cfg, nil, nil)
		if err != nil {
			b.readErr = err
			return
		}
		if bodyHasSSEFraming(body) {
			b.reader = bytes.NewReader(body)
			return
		}
		payload, err := buildOpenAINonStreamResponsesSSE(body)
		if err != nil {
			b.readErr = err
			return
		}
		b.reader = bytes.NewReader(payload)
	})
	if b.readErr != nil {
		return 0, b.readErr
	}
	return b.reader.Read(p)
}

// A complete Responses lifecycle includes text and tool deltas, not just a
// completed event: Chat Completions and Messages adapters consume those deltas.
func buildOpenAINonStreamResponsesSSE(body []byte) ([]byte, error) {
	if !gjson.ValidBytes(body) || !gjson.ParseBytes(body).IsObject() {
		return nil, fmt.Errorf("invalid non-streaming Responses JSON")
	}
	root := gjson.ParseBytes(body)
	if root.Get("error").IsObject() || root.Get("status").String() == "failed" {
		failed, err := json.Marshal(map[string]any{"type": "response.failed", "sequence_number": 0, "response": json.RawMessage(body)})
		if err != nil {
			return nil, err
		}
		return append(append([]byte("event: response.failed\ndata: "), failed...), []byte("\n\n")...), nil
	}
	if (root.Get("status").String() != "completed" && root.Get("status").String() != "incomplete") || !root.Get("output").IsArray() || root.Get("id").String() == "" {
		return nil, fmt.Errorf("non-streaming Responses JSON is not a completed response")
	}
	var out bytes.Buffer
	sequence := 0
	emit := func(kind string, fields map[string]any) error {
		fields["type"] = kind
		fields["sequence_number"] = sequence
		sequence++
		data, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		fmt.Fprintf(&out, "event: %s\ndata: %s\n\n", kind, data)
		return nil
	}
	progress, err := sjson.SetBytes(body, "status", "in_progress")
	if err != nil {
		return nil, err
	}
	progress, err = sjson.SetRawBytes(progress, "output", []byte("[]"))
	if err != nil {
		return nil, err
	}
	for _, kind := range []string{"response.created", "response.in_progress"} {
		if err := emit(kind, map[string]any{"response": json.RawMessage(progress)}); err != nil {
			return nil, err
		}
	}
	for i, item := range root.Get("output").Array() {
		added := []byte(item.Raw)
		added, _ = sjson.SetBytes(added, "status", "in_progress")
		if item.Get("type").String() == "message" {
			added, _ = sjson.SetRawBytes(added, "content", []byte("[]"))
		}
		if item.Get("type").String() == "function_call" {
			added, _ = sjson.SetBytes(added, "arguments", "")
		}
		if err := emit("response.output_item.added", map[string]any{"output_index": i, "item": json.RawMessage(added)}); err != nil {
			return nil, err
		}
		for j, part := range item.Get("content").Array() {
			partType := part.Get("type").String()
			if partType != "output_text" && partType != "refusal" {
				continue
			}
			textKey, eventPrefix := "text", "response.output_text"
			if partType == "refusal" {
				textKey, eventPrefix = "refusal", "response.refusal"
			}
			emptyPart, _ := sjson.SetBytes([]byte(part.Raw), textKey, "")
			base := func() map[string]any {
				return map[string]any{"item_id": item.Get("id").String(), "output_index": i, "content_index": j}
			}
			fields := base()
			fields["part"] = json.RawMessage(emptyPart)
			if err := emit("response.content_part.added", fields); err != nil {
				return nil, err
			}
			fields = base()
			fields["delta"] = part.Get(textKey).String()
			if err := emit(eventPrefix+".delta", fields); err != nil {
				return nil, err
			}
			fields = base()
			fields[textKey] = part.Get(textKey).String()
			if err := emit(eventPrefix+".done", fields); err != nil {
				return nil, err
			}
			fields = base()
			fields["part"] = json.RawMessage(part.Raw)
			if err := emit("response.content_part.done", fields); err != nil {
				return nil, err
			}
		}
		if item.Get("type").String() == "function_call" {
			for _, kind := range []string{"response.function_call_arguments.delta", "response.function_call_arguments.done"} {
				fields := map[string]any{"item_id": item.Get("id").String(), "output_index": i}
				key := "arguments"
				if strings.HasSuffix(kind, ".delta") {
					key = "delta"
				}
				fields[key] = item.Get("arguments").String()
				if err := emit(kind, fields); err != nil {
					return nil, err
				}
			}
		}
		if err := emit("response.output_item.done", map[string]any{"output_index": i, "item": json.RawMessage(item.Raw)}); err != nil {
			return nil, err
		}
	}
	terminal := "response.completed"
	if root.Get("status").String() == "incomplete" {
		terminal = "response.incomplete"
	}
	if err := emit(terminal, map[string]any{"response": json.RawMessage(body)}); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
