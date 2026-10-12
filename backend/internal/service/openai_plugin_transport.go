package service

import "net/http"

func (s *OpenAIGatewayService) SetPluginManager(manager *PluginManager) {
	s.pluginManager = manager
}

// doOpenAIUpstream 只在 OpenAI OAuth 能力绑定已启用时把真实请求交给插件。
// 插件返回标准 http.Response，响应解析、错误映射、SSE 和计费仍由现有核心链处理。
func (s *OpenAIGatewayService) doOpenAIUpstream(request *http.Request, proxyURL string, account *Account) (result *http.Response, resultErr error) {
	prepared, bridgeSSE, err := s.prepareOpenAIForcedNonStreamRequest(request, account)
	if err != nil {
		return nil, err
	}
	forcedNonStream := prepared != request
	request = prepared
	defer func() {
		if resultErr != nil || result == nil || result.Body == nil {
			return
		}
		if s.settingService != nil && account != nil && account.Platform == PlatformOpenAI && s.settingService.GetOpenAIOverloadNonStreamSettings(request.Context()).Enabled {
			result.Body = &openAIOverloadObservingBody{ReadCloser: result.Body, onOverload: func() { s.recordOpenAIOverload(request.Context(), account) }}
		}
		if bridgeSSE && result.StatusCode >= 200 && result.StatusCode < 300 {
			result.Body = &openAIForcedNonStreamBody{ReadCloser: result.Body, s: s}
			result.Header = result.Header.Clone()
			if result.Header == nil {
				result.Header = make(http.Header)
			}
			result.Header.Set("Content-Type", "text/event-stream")
			result.Header.Del("Content-Length")
			result.ContentLength = -1
		}
	}()
	request = s.stickBoundCodexTicketRequest(request, account)
	boundTicket := s.codexTicketRequestBound(request, account)
	releaseChat := func() {}
	if boundTicket {
		releaseChat = s.holdCodexTicketChat(account)
	}
	proxyURL, release, err := s.pinCodexTicketEgress(request, account, proxyURL)
	if err != nil {
		releaseChat()
		return nil, err
	}
	defer func() {
		result, resultErr = attachCodexTicketEgressRelease(result, resultErr, func() {
			release()
			releaseChat()
		})
	}()
	defer func() {
		if resultErr == nil {
			// Fast admission may have installed the ticket inside the upstream
			// adapter. Capture the binding before observation can revoke it.
			observedRequest := request
			if result != nil && result.Request != nil && s.UnifiedAstraTicketsEnabled(request.Context()) && s.codexTicketRequestBound(result.Request, account) {
				// Timing/fallback layers clone request headers before the routing
				// adapter installs a newly acquired bundle. Observe the sent copy.
				observedRequest = result.Request
			}
			responseBound := boundTicket || s.codexTicketRequestBound(observedRequest, account)
			s.observeCodexTicketResponse(observedRequest, result, account)
			if responseBound && codexResponseMismatches(observedRequest, result, account) && s.settingService.CodexTicketStrictResponse(request.Context()) {
				if result.Body != nil {
					_ = result.Body.Close()
				}
				result = nil
				resultErr = ErrCodexTicketResponseRejected
			}
		}
	}()
	// Keep ticket observation/strict validation around the final response,
	// while every egress attempt retains its own plugin routing and trace.
	result, resultErr = s.doUpstreamWithProxyFallback(request.Context(), request, account, proxyURL)
	if forcedNonStream && resultErr == nil && result != nil && result.StatusCode == http.StatusBadRequest {
		result, resultErr = s.retryUnsupportedOpenAINonStream(request, proxyURL, account, result)
	}
	return result, resultErr
}

// doOpenAIAccountTestUpstream 让 OpenAI OAuth 账号测试与真实转发使用同一插件路径。
// API Key 和未命中插件的账号保持各自原有的 HTTPUpstream 行为。
func (s *AccountTestService) doOpenAIAccountTestUpstream(
	request *http.Request,
	proxyURL string,
	account *Account,
	useTLSFallback bool,
) (*http.Response, error) {
	if s.pluginManager != nil {
		response, handled, err := s.pluginManager.RoundTripOpenAIOAuth(request.Context(), request, proxyURL, account)
		if handled {
			return response, err
		}
	}
	if useTLSFallback {
		return s.httpUpstream.DoWithTLS(
			request,
			proxyURL,
			account.ID,
			account.Concurrency,
			s.tlsFPProfileService.ResolveTLSProfile(account),
		)
	}
	return s.httpUpstream.Do(request, proxyURL, account.ID, account.Concurrency)
}
