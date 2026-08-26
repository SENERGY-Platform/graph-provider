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

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	devicerepo "github.com/SENERGY-Platform/device-repository/lib/client"
	sb_config_hdl "github.com/SENERGY-Platform/go-service-base/config-hdl"
	srv_info_hdl "github.com/SENERGY-Platform/go-service-base/srv-info-hdl"
	struct_logger "github.com/SENERGY-Platform/go-service-base/struct-logger"
	"github.com/SENERGY-Platform/graph-provider/pkg/api"
	"github.com/SENERGY-Platform/graph-provider/pkg/cache"
	"github.com/SENERGY-Platform/graph-provider/pkg/config"
	"github.com/SENERGY-Platform/graph-provider/pkg/consumption"
	"github.com/SENERGY-Platform/graph-provider/pkg/events"
	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/keycloak"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/plan"
	"github.com/SENERGY-Platform/graph-provider/pkg/reconcile"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	timescale "github.com/SENERGY-Platform/timescale-wrapper/pkg/client"
)

const serviceName = "graph-provider"

// version is set at build time.
var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "config.json", "path to the configuration file")
	dryRun := flag.Bool("dry-run", false, "run one reconciliation pass, report what it would write to stdout, and write nothing")
	flag.Parse()

	cfg, err := config.New(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "unable to load configuration:", err)
		return 1
	}
	// A dry run starts no consumers, so the Kafka settings are the one group of
	// requirements it can do without. Everything else still has to be there -
	// see config.ValidateDryRun.
	validate := cfg.Validate
	if *dryRun {
		validate = cfg.ValidateDryRun
	}
	if err = validate(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		return 1
	}

	// Logs to stderr in a dry run: the report goes to stdout, and a reader
	// piping it into a pager or a file must not have to sort log records out
	// of it first.
	logTo := os.Stdout
	if *dryRun {
		logTo = os.Stderr
	}
	logger := struct_logger.New(cfg.Logger, logTo, "github.com/SENERGY-Platform", serviceName)
	slog.SetDefault(logger)

	info := srv_info_hdl.New(serviceName, version)
	logger.Info("starting service",
		"version", version,
		"config", sb_config_hdl.StructToMap(cfg, true))

	// The internal admin token is what reaches the device repository,
	// permissions-v2 and the timescale wrapper. All three parse tokens without
	// verifying them, and the admin role sees every device. Kong rejects it, so
	// the configured addresses must be cluster-internal.
	token := permissions.InternalAdminToken

	// The configured owner is the only user-level administrator of every graph
	// this service creates, so a typo there leaves each graph administered by a
	// subject nobody can act as. Worth saying out loud at startup.
	//
	// A warning and not a refusal: permissions-v2 returns early for a token
	// carrying the admin role, so this service can manage a graph whatever its
	// owner says, and a dedicated technical user that is not this token's
	// subject is a legitimate configuration. Refusing it would forbid the setup
	// the service was asked for.
	if err = checkServiceUser(token, cfg.ServiceUserId); err != nil {
		logger.Warn("service_user_id is not the subject of the service token", "error", err)
	}

	// A dry run overrides the configuration rather than reading it: the point
	// is to be able to point it at a cluster whose ENABLED is true and still
	// write nothing.
	writable := cfg.Enabled && !*dryRun
	if *dryRun {
		logger.Info("dry run, one pass, nothing will be written")
	} else if !cfg.Enabled {
		logger.Warn("writing is disabled by configuration, the service will read and compute only")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	wg := &sync.WaitGroup{}

	repository := devicerepo.NewClient(cfg.DeviceRepositoryUrl, nil)
	permissionsClient := permissions.New(cfg.PermissionsV2Url)
	keycloakClient := keycloak.New(cfg.Keycloak, time.Duration(cfg.HttpTimeout))

	deviceCache := cache.New(repository, permissionsClient, keycloakClient, token,
		cfg.PermissionsDeviceTopic, cfg.GroupAllowed)

	store := graphs.New(repository, permissionsClient, token,
		cfg.PermissionsGraphTopic, cfg.ServiceUserId, writable)

	readings := consumption.New(timescale.NewClient(cfg.TimescaleWrapperUrl), token)

	reconcileConfig := reconcile.Config{
		ServiceUserId:     cfg.ServiceUserId,
		Window:            time.Duration(cfg.Heuristic.ReferenceWindow),
		Tolerance:         cfg.Heuristic.ContainmentTolerance,
		NoFlowNodeName:    cfg.Heuristic.NoFlowNodeName,
		ReadingTtl:        time.Duration(cfg.Heuristic.ReadingTtl),
		GroupPollInterval: time.Duration(cfg.GroupPollInterval),
		ReconcileInterval: time.Duration(cfg.ReconcileInterval),
	}

	// Collected rather than streamed, because the report needs a summary and
	// the tree of one group is not readable interleaved with the next. One pass
	// holds one change per group, so the slice is the size of the realm.
	// Appended from the pass's own goroutine, which is the only caller.
	var planned []model.PlannedChange
	if *dryRun {
		reconcileConfig.Planned = func(change model.PlannedChange) {
			planned = append(planned, change)
		}
	}

	pending := events.NewPending()
	reconciler := reconcile.New(deviceCache, store, readings, pending, reconcileConfig, logger)

	if *dryRun {
		// One pass, and neither a consumer nor a server: joining the consumer
		// group would commit offsets a dry run has no business moving, and a
		// service that answers probes is one an orchestrator will treat as the
		// running instance.
		passErr := reconciler.Full(ctx)

		// Rendered before the pass is judged: a pass that failed over one group
		// still worked out what it would have done to the others, and that is
		// the half worth reading.
		if err = plan.Render(os.Stdout, planned); err != nil {
			logger.Error("unable to write the dry-run report", "error", err)
			return 1
		}
		if passErr != nil {
			// Said on stdout as well. A reader who keeps only the report would
			// otherwise take "no changes" for a settled platform, when what
			// happened is that the pass never got to look.
			fmt.Fprintln(os.Stdout, "the pass did not complete, so this report is incomplete - see the log on stderr")
			logger.Error("dry run failed", "error", passErr)
			return 1
		}
		logger.Info("dry run complete", "changes", len(planned))
		return 0
	}

	// The two Kafka failure kinds are reported differently on purpose. A
	// malformed message is skipped, the consumer lives on and the safety net
	// picks up whatever it named, so it is a warning; an error there would
	// page somebody for a single bad record. A lost consumer is the opposite:
	// nothing restarts it, live triggers are gone for good, and every probe
	// still passes - so it pages, and the process leaves through run() with a
	// non-zero code for the orchestrator to restart it. os.Exit from the
	// callback would skip the http shutdown and wg.Wait below.
	consumerLost := make(chan struct{}, 1)
	handlers := events.Handlers{
		OnMessageError: func(err error) {
			logger.Warn("kafka trigger dropped", "error", err)
		},
		OnConsumerLost: func(err error) {
			logger.Error("kafka consumer lost, exiting for a restart", "error", err)
			select {
			case consumerLost <- struct{}{}:
			default:
				// Already reported; the first one is what ends the process.
			}
		},
	}
	consumers, err := events.Start(ctx, cfg.Kafka, wg, pending, handlers)
	if err != nil {
		logger.Error("unable to start kafka consumers", "error", err)
		return 1
	}

	reconciler.Run(ctx, wg)

	engine, err := api.New(logger, reconciler, &status{info: info, store: store, reconciler: reconciler, cache: deviceCache, consumers: consumers}, api.Config{
		AccessLog: cfg.HttpAccessLog,
	})
	if err != nil {
		logger.Error("unable to build the http api", "error", err)
		return 1
	}
	server := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.ServerPort),
		Handler:           engine,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() {
		logger.Info("listening", "port", cfg.ServerPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverError <- err
		}
	}()

	code := 0
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err = <-serverError:
		logger.Error("http server failed", "error", err)
		code = 1
		stop()
	case <-consumerLost:
		code = 1
		stop()
	}

	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = server.Shutdown(shutdown); err != nil {
		logger.Warn("http server did not shut down cleanly", "error", err)
	}
	wg.Wait()
	return code
}

// checkServiceUser reports a configured owner that is not the token's subject.
//
// Not fatal - see the caller. It is a likely typo, not an impossible setup.
func checkServiceUser(token string, serviceUserId string) error {
	parsed, err := jwt.Parse(token)
	if err != nil {
		return fmt.Errorf("unable to parse the service token: %w", err)
	}
	if parsed.GetUserId() != serviceUserId {
		return fmt.Errorf("service_user_id is %v but the token's subject is %v", serviceUserId, parsed.GetUserId())
	}
	return nil
}

type status struct {
	info       *srv_info_hdl.Handler
	store      *graphs.Store
	reconciler *reconcile.Reconciler
	cache      *cache.Cache
	consumers  *events.Consumers
}

func (this *status) Status() api.Status {
	return api.Status{
		ServiceInfo: this.info.ServiceInfo(),
		Writable:    this.store.Writable(),
		Pending:     this.reconciler.Pending(),
		KafkaAlive:  this.consumers.Alive(),
		Groups:      len(this.cache.Groups()),
		Devices:     this.cache.DeviceCount(),
	}
}
