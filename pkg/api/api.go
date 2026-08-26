/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package api is the service's small HTTP surface.
//
// This service has no API to speak of: it is driven by Kafka and by its own
// clock, and graphs are read from the device repository by whoever wants them.
// What is here is what an operator needs - is it alive, what is it, and please
// look again now.
package api

import (
	"log/slog"
	"net/http"

	gin_mw "github.com/SENERGY-Platform/gin-middleware"
	srv_info_hdl "github.com/SENERGY-Platform/go-service-base/srv-info-hdl"
	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/gin-gonic/gin"
)

// Requester is the reconciler, as far as this package is concerned.
//
// Both methods return immediately: a reconcile pass touches four foreign
// systems and can take a while, so a request records the wish and the loop
// carries it out. An operator waiting on an HTTP response for that would be
// waiting on a timeout.
type Requester interface {
	RequestAll()
	RequestGroup(path string)
}

// Status is what the service says about itself.
type Status struct {
	srv_info_hdl.ServiceInfo
	// Writable is false when the kill switch is set: everything runs, nothing
	// is written. Worth knowing before wondering why no graph appears.
	Writable bool `json:"writable"`
	// Pending is how much work is queued.
	Pending int `json:"pending"`
	// KafkaAlive is false once a trigger consumer has ended. Nothing restarts
	// it, so from then on only the interval sweep still finds work - the
	// difference between a healthy service and a degraded one that answers
	// every probe.
	KafkaAlive bool `json:"kafka_alive"`
	Groups     int  `json:"groups"`
	Devices    int  `json:"devices"`
}

type StatusProvider interface {
	Status() Status
}

type Config struct {
	AccessLog bool
}

// New builds the service's gin engine.
//
// The swag general API annotations live here because this is the file the
// generate directive in doc_gen.go points swag at.
//
// @title        graph-provider API
// @version      1.0
// @description  graph-provider maintains one default energy-flow graph per Keycloak group, keeps it populated with every device the group has access to, and derives its structure from meter readings. This HTTP surface is cluster-internal and carries no authentication: it exists for an operator, or another cluster-internal service, to probe liveness, read status, and ask for a reconcile pass out of turn.
// @license.name Apache 2.0
// @license.url  http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath     /
func New(logger *slog.Logger, requester Requester, status StatusProvider, cfg Config) (*gin.Engine, error) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()

	engine.Use(gin_mw.StructRecoveryHandler(logger, gin_mw.DefaultRecoveryFunc))
	if cfg.AccessLog {
		// The health probe is skipped: it is polled every few seconds and
		// would be the only thing anyone ever read in the log.
		engine.Use(gin_mw.StructLoggerHandler(logger, attributes.Provider, []string{"/health"}, nil))
	}
	engine.Use(gin_mw.ErrorHandler(func(error) int { return http.StatusInternalServerError }, ", "))

	engine.GET("/health", handleHealth)
	engine.GET("/info", handleInfo(status))
	engine.POST("/reconcile", handleReconcile(requester))
	engine.POST("/reconcile/group", handleReconcileGroup(requester))

	return engine, nil
}

// handleHealth godoc
// @Summary      liveness probe
// @Description  always answers once the process is up; checks nothing downstream.
// @Tags         health
// @Produce      plain
// @Success      200
// @Router       /health [get]
func handleHealth(gc *gin.Context) {
	gc.Status(http.StatusOK)
}

// handleInfo godoc
// @Summary      service status
// @Description  version, whether writing is enabled, whether the Kafka trigger consumers are alive, and queue and cache sizes.
// @Tags         info
// @Produce      json
// @Success      200  {object}  Status
// @Failure      500
// @Router       /info [get]
func handleInfo(status StatusProvider) gin.HandlerFunc {
	return func(gc *gin.Context) {
		gc.JSON(http.StatusOK, status.Status())
	}
}

// handleReconcile godoc
// @Summary      request a full reconcile pass
// @Description  records the wish and returns immediately. Accepted, not OK: the pass touches four foreign systems and can take a while, and the loop carries it out separately - an operator waiting on this response would be waiting on a timeout.
// @Tags         reconcile
// @Produce      plain
// @Success      202
// @Failure      500
// @Router       /reconcile [post]
func handleReconcile(requester Requester) gin.HandlerFunc {
	return func(gc *gin.Context) {
		requester.RequestAll()
		gc.Status(http.StatusAccepted)
	}
}

// handleReconcileGroup godoc
// @Summary      request a reconcile pass for one group
// @Description  records the wish and returns immediately, for the reason given at /reconcile.
// @Tags         reconcile
// @Produce      plain
// @Param        path  query     string  true  "full Keycloak group path, leading slash included, e.g. /acme/werk-nord"
// @Success      202
// @Failure      400
// @Failure      500
// @Router       /reconcile/group [post]
func handleReconcileGroup(requester Requester) gin.HandlerFunc {
	return func(gc *gin.Context) {
		path := gc.Query("path")
		if path == "" {
			gc.String(http.StatusBadRequest, "query parameter path is required")
			return
		}
		requester.RequestGroup(path)
		gc.Status(http.StatusAccepted)
	}
}
