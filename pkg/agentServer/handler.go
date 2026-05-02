package agentserver

import (
	"log/slog"

	"github.com/gofiber/fiber/v2"

	agentclient "mytonprovider-backend/pkg/agentClient"
)

type handler struct {
	workers       *Workers
	internalToken string
	logger        *slog.Logger
}

func New(workers *Workers, internalToken string, logger *slog.Logger) *handler {
	return &handler{
		workers:       workers,
		internalToken: internalToken,
		logger:        logger,
	}
}

func (h *handler) RegisterRoutes(app *fiber.App) {
	internal := app.Group("/internal/v1/workers", h.tokenMiddleware)
	internal.Post("/ping-providers", h.pingProviders)
	internal.Post("/check-proofs", h.checkProofs)
}

func (h *handler) tokenMiddleware(c *fiber.Ctx) error {
	if c.Get("X-Internal-Token") != h.internalToken {
		return c.SendStatus(fiber.StatusUnauthorized)
	}
	return c.Next()
}

func (h *handler) pingProviders(c *fiber.Ctx) error {
	var req agentclient.PingProvidersRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	results := h.workers.PingProviders(c.Context(), req.Providers)

	return c.JSON(agentclient.PingProvidersResponse{Results: results})
}

func (h *handler) checkProofs(c *fiber.Ctx) error {
	var req agentclient.CheckProofsRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	ips, proofResults := h.workers.CheckProofs(c.Context(), req.Providers)

	return c.JSON(agentclient.CheckProofsResponse{
		IPs:          ips,
		ProofResults: proofResults,
	})
}
