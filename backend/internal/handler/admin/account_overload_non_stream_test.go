package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type overloadResetAdminStub struct {
	service.AdminService
	account *service.Account
	updates map[string]any
}

func (s *overloadResetAdminStub) GetAccount(context.Context, int64) (*service.Account, error) {
	return s.account, nil
}
func (s *overloadResetAdminStub) UpdateAccountExtra(_ context.Context, _ int64, updates map[string]any) error {
	s.updates = updates
	for k, v := range updates {
		s.account.Extra[k] = v
	}
	return nil
}

func TestOpenAIOverloadClearAccountOnlyResetsRuleState(t *testing.T) {
	a := &service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Extra: map[string]any{
		service.OpenAIForceNonStreamExtraKey: true, service.OpenAIOverloadCountExtraKey: 3, service.OpenAINonStreamUnsupportedExtraKey: true, "unrelated": "preserved",
	}}
	stub := &overloadResetAdminStub{account: a}
	handler := &AccountHandler{adminService: stub}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	c.Params = gin.Params{{Key: "id", Value: "1"}}
	handler.ClearForcedNonStream(c)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, false, a.Extra[service.OpenAIForceNonStreamExtraKey])
	require.Equal(t, 0, a.Extra[service.OpenAIOverloadCountExtraKey])
	require.Equal(t, false, a.Extra[service.OpenAINonStreamUnsupportedExtraKey])
	require.Equal(t, "preserved", a.Extra["unrelated"])
}

func TestOpenAIOverloadClearRejectsOtherPlatforms(t *testing.T) {
	stub := &overloadResetAdminStub{account: &service.Account{ID: 1, Platform: service.PlatformAnthropic}}
	handler := &AccountHandler{adminService: stub}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	c.Params = gin.Params{{Key: "id", Value: "1"}}
	handler.ClearForcedNonStream(c)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Nil(t, stub.updates)
}
