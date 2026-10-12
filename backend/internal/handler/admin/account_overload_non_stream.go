package admin

import (
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// ClearForcedNonStream restores the account's normal upstream streaming mode
// and starts a new overload counting cycle without changing other runtime state.
func (h *AccountHandler) ClearForcedNonStream(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	account, err := h.adminService.GetAccount(c.Request.Context(), id)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if account.Platform != service.PlatformOpenAI {
		response.BadRequest(c, "Only OpenAI accounts support forced non-streaming")
		return
	}
	if err := h.adminService.UpdateAccountExtra(c.Request.Context(), id, map[string]any{
		service.OpenAIForceNonStreamExtraKey:       false,
		service.OpenAIOverloadCountExtraKey:        0,
		service.OpenAINonStreamUnsupportedExtraKey: false,
	}); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	account, err = h.adminService.GetAccount(c.Request.Context(), id)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, h.buildAccountResponseWithRuntime(c.Request.Context(), account))
}
