// Package api is example-pg's HTTP layer: routing, request binding,
// permission checks and response mapping. Business rules live in service.
package api

import (
	"github.com/gin-gonic/gin"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/auth"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/httpserver"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/domain"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/services/example-pg/internal/service"
)

// Permissions checked by this service. The IAM super admin creates them.
const (
	PermItemRead  = "example-pg.item.read"
	PermItemWrite = "example-pg.item.write"
)

// Handler serves the /v1 API.
type Handler struct {
	authz auth.Authorizer
	items *service.ItemService
}

// NewHandler wires the handler.
func NewHandler(authz auth.Authorizer, items *service.ItemService) *Handler {
	return &Handler{authz: authz, items: items}
}

// Register mounts routes on an already authenticated, scoped group.
func (h *Handler) Register(r *gin.RouterGroup) {
	r.GET("/me", h.me)

	items := r.Group("/items", httpserver.RequireCompany())
	items.POST("", auth.RequirePermission(h.authz, PermItemWrite), h.createItem)
	items.GET("", auth.LoadAccessList(h.authz, PermItemRead), h.listItems)
	items.GET("/:id", auth.RequirePermission(h.authz, PermItemRead), h.getItem)
	items.POST("/:id/archive", auth.RequirePermission(h.authz, PermItemWrite), h.archiveItem)
}

// me echoes the caller and scope — handy to check auth wiring.
func (h *Handler) me(c *gin.Context) {
	p, _ := auth.PrincipalFrom(c.Request.Context())
	httpserver.OK(c, gin.H{"subject": p.Subject, "client_id": p.ClientID, "service_token": p.IsService()})
}

type createItemRequest struct {
	Name        string `json:"name" binding:"required,min=1,max=200"`
	Description string `json:"description" binding:"max=2000"`
}

func (h *Handler) createItem(c *gin.Context) {
	var req createItemRequest
	if !httpserver.BindJSON(c, &req) {
		return
	}
	it, err := h.items.Create(c.Request.Context(), service.CreateItemInput{Name: req.Name, Description: req.Description})
	if err != nil {
		httpserver.Fail(c, err)
		return
	}
	c.Header("Location", c.Request.URL.Path+"/"+it.ID)
	httpserver.Created(c, it)
}

type listItemsQuery struct {
	Status string `form:"status" binding:"omitempty,oneof=ACTIVE ARCHIVED"`
}

func (h *Handler) listItems(c *gin.Context) {
	var q listItemsQuery
	if !httpserver.BindQuery(c, &q) {
		return
	}
	page := httpserver.ParsePage(c)
	al, _ := auth.AccessListFrom(c.Request.Context())
	items, total, err := h.items.List(c.Request.Context(), domain.ItemFilter{
		Status:       q.Status,
		WholeCompany: al.WholeCompany,
		VerticalIDs:  al.VerticalIDs,
		TenantIDs:    al.TenantIDs,
		Offset:       page.Offset(),
		Limit:        page.Limit,
	})
	if err != nil {
		httpserver.Fail(c, err)
		return
	}
	httpserver.List(c, items, page.Meta(total))
}

func (h *Handler) getItem(c *gin.Context) {
	it, err := h.items.Get(c.Request.Context(), c.Param("id"))
	if err == nil {
		err = h.authorizeRecord(c, PermItemRead, it)
	}
	if err != nil {
		httpserver.Fail(c, err)
		return
	}
	httpserver.OK(c, it)
}

func (h *Handler) archiveItem(c *gin.Context) {
	ctx := c.Request.Context()
	it, err := h.items.Get(ctx, c.Param("id"))
	if err == nil {
		err = h.authorizeRecord(c, PermItemWrite, it)
	}
	if err == nil {
		it, err = h.items.Archive(ctx, it.ID)
	}
	if err != nil {
		httpserver.Fail(c, err)
		return
	}
	httpserver.OK(c, it)
}

// authorizeRecord re-checks the permission against the record's own
// vertical/tenant. The route check used the request headers; this stops an
// "AC Bus" manager from touching a "Non-AC" record by guessing its ID.
// It answers NOT_FOUND rather than FORBIDDEN so IDs cannot be probed.
func (h *Handler) authorizeRecord(c *gin.Context, perm string, it domain.Item) error {
	if it.VerticalID == "" && it.TenantID == "" {
		return nil
	}
	ctx := c.Request.Context()
	p, _ := auth.PrincipalFrom(ctx)
	ok, err := h.authz.Can(ctx, p, perm, reqctx.Scope{CompanyID: it.CompanyID, VerticalID: it.VerticalID, TenantID: it.TenantID})
	if err != nil {
		return apperror.Unavailable("authorization unavailable").Wrap(err)
	}
	if !ok {
		return apperror.NotFound("item not found")
	}
	return nil
}
