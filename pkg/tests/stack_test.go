//go:build integration

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

// The build tag above is the guard, chosen over a Docker probe for two
// reasons. It is decided at compile time, so `go test ./...` on a machine
// without Docker cannot fail here and cannot hang - the file is not built, and
// there is no socket to dial that could hang instead of answering. And the
// suite pulls images and costs minutes, which is a thing to ask for rather
// than to discover.

package tests

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	devicerepo "github.com/SENERGY-Platform/device-repository/lib"
	devicerepoclient "github.com/SENERGY-Platform/device-repository/lib/client"
	devicerepoconfig "github.com/SENERGY-Platform/device-repository/lib/configuration"
	"github.com/SENERGY-Platform/device-repository/lib/controller/publisher"
	"github.com/SENERGY-Platform/device-repository/lib/tests/docker"
	"github.com/SENERGY-Platform/graph-provider/pkg/cache"
	"github.com/SENERGY-Platform/graph-provider/pkg/events"
	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/reconcile"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissionsv2 "github.com/SENERGY-Platform/permissions-v2/pkg"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
	permissionsconfig "github.com/SENERGY-Platform/permissions-v2/pkg/configuration"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
)

const (
	// deviceTopic and graphTopic are the permissions-v2 topic ids the device
	// repository registers at startup. The service is configured with the
	// same names, so getting them wrong here would test nothing.
	deviceTopic = "devices"
	graphTopic  = "graphs"

	// group is the one Keycloak group the faked directory reports. Its id is
	// what identifies the graph; the path is what sharing is keyed by.
	groupId   = "kc-acme"
	groupPath = "/acme"
	groupName = "acme"

	// referenceWindow and tolerance are SPEC.md's defaults.
	referenceWindow = 720 * time.Hour
	tolerance       = 0.05

	// The three consumption figures the fake readings source reports. They
	// are what the resulting tree has to be explainable by: the 600 fits
	// inside the 1000, and the 200 goes to the tighter of the two containers
	// that are left (400 unexplained against 600), which is the 1000 again.
	mainConsumption  = 1000.0
	subConsumption   = 600.0
	smallConsumption = 200.0
)

// stack is the running world: Mongo and Kafka in containers, the device
// repository and permissions-v2 in this process, and the service's own
// packages wired over them exactly as main.go wires them.
type stack struct {
	ctx context.Context

	token         string
	serviceUserId string

	repositoryUrl  string
	permissionsUrl string
	kafkaUrl       string

	repository  devicerepoclient.Interface
	permissions permissions.Client

	// Writes the service made, counted at the two calls that change
	// anything. A pass that writes the same content again leaves the stored
	// graph equal to what it was, so counting is the only way to see it.
	graphWrites      atomic.Int64
	permissionWrites atomic.Int64

	cache    *cache.Cache
	store    *graphs.Store
	readings *fakeReadings
	groups   *fakeGroupSource
	rec      *reconcile.Reconciler
}

// start brings the stack up.
//
// Mongo and Kafka come from the upstream docker helpers. The device repository
// and permissions-v2 run in this process, each through the Start its own
// repository's tests use, listening on a free port and reached over HTTP by
// the clients under test and by each other. Two reasons rather than one:
//
//   - The device repository's container helper builds the image from a
//     Dockerfile context that only exists inside that repository.
//   - permissions-v2's container helper starts, but every request through it
//     costs ten seconds on a docker host whose DNS swallows the lookup of the
//     OTLP collector instead of refusing it, which is what a Docker Desktop or
//     WSL setup does. In process the collector endpoint is a config field.
//
// What is not used is permissions-v2's client.NewTestClient: it is a
// controller over an in-memory database mock with a void Kafka producer, so it
// would answer none of the questions this suite exists to ask - not the
// paging, not the HTTP status codes, not what Mongo makes of a filter - and
// the device repository could not share it, since it speaks to permissions-v2
// over HTTP.
//
// Keycloak and the timescale wrapper stay faked; see the fakes at the bottom
// of this file.
func start(t *testing.T) *stack {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	config, err := devicerepoconfig.Load(deviceRepositoryConfigPath(t))
	if err != nil {
		t.Fatalf("unable to load the device repository's own configuration: %v", err)
	}

	// This is the topic registration the device repository does at startup:
	// without it permissions-v2 knows no "devices" and no "graphs" topic and
	// the resources this service reads do not exist at all.
	config.InitPermissionsTopics = true
	config.SyncLockDuration = time.Second.String()

	port, err := docker.GetFreePort()
	if err != nil {
		t.Fatalf("unable to find a free port: %v", err)
	}
	config.ServerPort = fmt.Sprint(port)

	// Addressed through published ports rather than container addresses: a
	// container ip is not routable from the host on every docker setup
	// (Docker Desktop, rootless, WSL), and both services run on the host.
	mongoPort, _, err := docker.MongoDB(ctx, wg)
	if err != nil {
		t.Fatalf("unable to start mongodb: %v", err)
	}
	kafkaUrl, err := docker.Kafka(ctx, wg)
	if err != nil {
		t.Fatalf("unable to start kafka: %v", err)
	}
	config.KafkaUrl = kafkaUrl
	config.MongoUrl = "mongodb://localhost:" + mongoPort

	// The topics the device repository publishes its own commands to. It does
	// not create them itself, and permissions-v2 publishes the device rights
	// commands onto the same "devices" topic.
	//
	// The graph topic is deliberately absent: the device repository registers
	// it in permissions-v2 without a Kafka target and publishes no graph
	// commands, which is SPEC.md's "graph events are a precondition, not
	// something this service can arrange" seen from the other side.
	err = publisher.InitTopic(config.KafkaUrl,
		config.DeviceTopic,
		config.DeviceTypeTopic,
		config.DeviceGroupTopic,
		config.HubTopic,
		config.ProtocolTopic,
		config.ConceptTopic,
		config.CharacteristicTopic,
		config.AspectTopic,
		config.FunctionTopic,
		config.DeviceClassTopic,
		config.LocationTopic)
	if err != nil {
		t.Fatalf("unable to create the kafka topics: %v", err)
	}

	config.PermissionsV2Url = "http://localhost:" + startPermissionsV2(t, ctx, config.MongoUrl, config.KafkaUrl)

	if err = devicerepo.Start(ctx, wg, config); err != nil {
		t.Fatalf("unable to start the device repository: %v", err)
	}

	// The internal admin token, as main.go uses it, and the service user it
	// commits this service to. main.go refuses to start when the two differ,
	// so deriving one from the other is the only honest fixture.
	token := permissions.InternalAdminToken
	parsed, err := jwt.Parse(token)
	if err != nil {
		t.Fatalf("unable to parse the internal admin token: %v", err)
	}

	this := &stack{
		ctx:            ctx,
		token:          token,
		serviceUserId:  parsed.GetUserId(),
		repositoryUrl:  "http://localhost:" + config.ServerPort,
		permissionsUrl: config.PermissionsV2Url,
		kafkaUrl:       config.KafkaUrl,
	}
	this.repository = devicerepoclient.NewClient(this.repositoryUrl, nil)
	this.permissions = permissions.New(this.permissionsUrl)

	this.groups = &fakeGroupSource{groups: []model.Group{{Id: groupId, Path: groupPath, Name: groupName}}}
	this.readings = &fakeReadings{values: map[model.ReadingKey]float64{}}

	// The service's own packages get the same clients behind a counter, so a
	// write is observable without a fake in the way.
	countedRepository := countingRepository{Interface: this.repository, writes: &this.graphWrites}
	countedPermissions := countingPermissions{Client: this.permissions, writes: &this.permissionWrites}

	this.cache = cache.New(countedRepository, countedPermissions, this.groups, token, deviceTopic, nil)
	this.store = graphs.New(countedRepository, countedPermissions, token, graphTopic, this.serviceUserId, true)
	this.rec = reconcile.New(this.cache, this.store, this.readings, events.NewPending(), reconcile.Config{
		ServiceUserId: this.serviceUserId,
		Window:        referenceWindow,
		Tolerance:     tolerance,
		ReadingTtl:    24 * time.Hour,
		// Never fired: the suite drives Full itself, so no ticker decides
		// when a scenario's assertions are allowed to look.
		GroupPollInterval: time.Hour,
		ReconcileInterval: time.Hour,
	}, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	this.waitForRepository(t)
	t.Logf("device-repository %v, permissions-v2 %v, kafka %v",
		this.repositoryUrl, this.permissionsUrl, this.kafkaUrl)
	return this
}

// deviceRepositoryConfigPath is the config.json the device repository ships.
func deviceRepositoryConfigPath(t *testing.T) string {
	t.Helper()
	return moduleFile(t, "github.com/SENERGY-Platform/device-repository", "config.json")
}

// startPermissionsV2 runs permissions-v2 in this process and answers with the
// port it listens on.
//
// The Kafka producer is the real one, so the device rights commands this suite
// causes really are produced onto the devices topic - which is what the broker
// is here for besides being a thing both services refuse to start without.
func startPermissionsV2(t *testing.T, ctx context.Context, mongoUrl string, kafkaUrl string) string {
	t.Helper()
	config, err := permissionsconfig.Load(permissionsV2ConfigPath(t))
	if err != nil {
		t.Fatalf("unable to load permissions-v2's own configuration: %v", err)
	}
	port, err := docker.GetFreePort()
	if err != nil {
		t.Fatalf("unable to find a free port: %v", err)
	}
	config.Port = fmt.Sprint(port)
	config.MongoUrl = mongoUrl
	config.KafkaUrl = kafkaUrl

	// Neither is reachable from a test machine, and both are dialled with a
	// timeout rather than refused when the name does not resolve.
	config.DevNotifierUrl = ""
	config.OtelEndpoint = "127.0.0.1:4317"

	if err = permissionsv2.Start(ctx, config); err != nil {
		t.Fatalf("unable to start permissions-v2: %v", err)
	}
	return config.Port
}

// moduleFile locates a file the given module ships.
//
// Asked of the toolchain rather than copied: a copy would be this suite's
// opinion about a foreign service's defaults, and it would go stale on the
// next version bump without anything saying so.
func moduleFile(t *testing.T, module string, name string) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", module).Output()
	if err != nil {
		t.Fatalf("unable to locate the %v module: %v", module, err)
	}
	path := filepath.Join(strings.TrimSpace(string(out)), name)
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("%v of module %v not found at %v: %v", name, module, path, err)
	}
	return path
}

func permissionsV2ConfigPath(t *testing.T) string {
	t.Helper()
	return moduleFile(t, "github.com/SENERGY-Platform/permissions-v2", "config.json")
}

// waitForRepository blocks until the repository answers, so a slow start is
// not reported as a broken assumption.
func (this *stack) waitForRepository(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		_, _, err, _ := this.repository.ListGraphs(this.token, devicerepoclient.GraphListOptions{Limit: 1})
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("device repository did not become ready: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// --- seeding ------------------------------------------------------------------

// meter is one device as the test refers to it afterwards.
type meter struct {
	id    string
	name  string
	value float64
}

// seed creates what a graph can be built from: a protocol, the measuring
// function that identifies electricity, a device type annotated with it, and
// the devices themselves.
//
// The function has to exist before the device type: the repository rejects a
// content variable whose function_id it does not know, which is a dependency
// no fake has ever had to have.
func (this *stack) seed(t *testing.T) (deviceTypeId string, meters []meter) {
	t.Helper()

	protocol, err, code := this.repository.SetProtocol(this.token, platform.Protocol{
		Name:             "test-protocol",
		Handler:          "test-protocol",
		ProtocolSegments: []platform.ProtocolSegment{{Name: "body"}},
	})
	if err != nil {
		t.Fatalf("unable to create a protocol (%v): %v", code, err)
	}

	_, err, code = this.repository.SetFunction(this.token, platform.Function{
		Id:      model.CarrierFunctionId[model.Electricity],
		Name:    "getEnergyConsumptionFunction",
		RdfType: "https://senergy.infai.org/ontology/MeasuringFunction",
	})
	if err != nil {
		t.Fatalf("unable to create the electricity measuring function (%v): %v", code, err)
	}

	deviceType, err, code := this.repository.SetDeviceType(this.token, platform.DeviceType{
		Name: "electricity-meter",
		Services: []platform.Service{{
			Name:        "getReading",
			LocalId:     "getReading",
			ProtocolId:  protocol.Id,
			Interaction: platform.REQUEST,
			Outputs: []platform.Content{{
				Serialization:     platform.JSON,
				ProtocolSegmentId: protocol.ProtocolSegments[0].Id,
				ContentVariable: platform.ContentVariable{
					Name: "reading",
					Type: platform.Structure,
					SubContentVariables: []platform.ContentVariable{{
						Name:       "energy",
						Type:       platform.Float,
						FunctionId: model.CarrierFunctionId[model.Electricity],
					}},
				},
			}},
		}},
	}, devicerepoclient.DeviceTypeUpdateOptions{})
	if err != nil {
		t.Fatalf("unable to create a device type (%v): %v", code, err)
	}

	for _, seeded := range []meter{
		{name: "acme-main", value: mainConsumption},
		{name: "acme-sub", value: subConsumption},
		{name: "acme-small", value: smallConsumption},
	} {
		device, err, code := this.repository.CreateDevice(this.token, platform.Device{
			LocalId:      seeded.name,
			Name:         seeded.name,
			DeviceTypeId: deviceType.Id,
		})
		if err != nil {
			t.Fatalf("unable to create device %v (%v): %v", seeded.name, code, err)
		}
		seeded.id = device.Id
		this.readings.set(device.Id, model.Electricity, seeded.value)
		this.shareDevice(t, device.Id, groupPath)
		meters = append(meters, seeded)
	}
	return deviceType.Id, meters
}

// shareDevice gives the named group paths read, write and execute on a device
// and leaves every other permission alone. This is what makes a device part of
// a company, and it is what pkg/cache reads group membership out of.
func (this *stack) shareDevice(t *testing.T, deviceId string, groupPaths ...string) {
	t.Helper()
	resource, err, code := this.permissions.GetResource(this.token, deviceTopic, deviceId)
	if err != nil {
		t.Fatalf("unable to read the permissions of device %v (%v): %v", deviceId, code, err)
	}
	next := resource.ResourcePermissions
	next.GroupPermissions = map[string]permissions.PermissionsMap{}
	for _, path := range groupPaths {
		next.GroupPermissions[path] = permissions.PermissionsMap{Read: true, Write: true, Execute: true}
	}
	if _, err, code = this.permissions.SetPermission(this.token, deviceTopic, deviceId, next); err != nil {
		t.Fatalf("unable to write the permissions of device %v (%v): %v", deviceId, code, err)
	}
}

// --- driving and reading back -------------------------------------------------

// full runs one reconciliation pass, the way every trigger source ends up
// running it.
func (this *stack) full(t *testing.T) {
	t.Helper()
	if err := this.rec.Full(this.ctx); err != nil {
		t.Fatalf("reconciliation failed: %v", err)
	}
}

// defaultGraph is the group's graph as the repository has it, read back
// through the service's own listing so the assertions are about what the
// service can see and not about what the test remembers writing.
func (this *stack) defaultGraph(t *testing.T) platform.Graph {
	t.Helper()
	defaults, err := this.store.Defaults(this.ctx)
	if err != nil {
		t.Fatalf("unable to list the default graphs: %v", err)
	}
	graph, found := defaults.ById[groupId]
	if !found {
		t.Fatalf("no default graph for group %v (%v graphs by id, %v by path)",
			groupId, len(defaults.ById), len(defaults.ByPath))
	}
	return graph
}

// writes is how many times the service has changed something since the
// counters were last reset.
func (this *stack) writes() (graphWrites int64, permissionWrites int64) {
	return this.graphWrites.Load(), this.permissionWrites.Load()
}

func (this *stack) resetWrites() {
	this.graphWrites.Store(0)
	this.permissionWrites.Store(0)
}

// countingRepository is the real client with the one call that changes a graph
// counted.
type countingRepository struct {
	devicerepoclient.Interface
	writes *atomic.Int64
}

func (this countingRepository) SetGraph(token string, graph platform.Graph) (platform.Graph, error, int) {
	this.writes.Add(1)
	return this.Interface.SetGraph(token, graph)
}

// countingPermissions is the real client with the one call that changes a
// sharing counted.
type countingPermissions struct {
	permissions.Client
	writes *atomic.Int64
}

func (this countingPermissions) SetPermission(token string, topicId string, id string, perms permissions.ResourcePermissions) (permissions.ResourcePermissions, error, int) {
	this.writes.Add(1)
	return this.Client.SetPermission(token, topicId, id, perms)
}

// --- fakes --------------------------------------------------------------------

// fakeGroupSource stands in for the Keycloak admin API: the realm this suite
// would need has no provisioning, and the group tree is one list of paths.
type fakeGroupSource struct {
	mux    sync.Mutex
	groups []model.Group
}

func (this *fakeGroupSource) Groups(context.Context) ([]model.Group, error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	return append([]model.Group{}, this.groups...), nil
}

// fakeReadings stands in for the timescale wrapper: it drags Postgres and
// Timescale behind it, and the arithmetic between a query and a consumption
// figure is what pkg/consumption's unit tests are about. What this suite
// needs from it is figures it can name.
type fakeReadings struct {
	mux    sync.Mutex
	values map[model.ReadingKey]float64
}

func (this *fakeReadings) set(deviceId string, carrier model.Carrier, value float64) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.values[model.ReadingKey{DeviceId: deviceId, Carrier: carrier}] = value
}

func (this *fakeReadings) Fetch(_ context.Context, devices []model.Device, window model.Window) (map[model.ReadingKey]model.Reading, error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	result := map[model.ReadingKey]model.Reading{}
	for _, device := range devices {
		for _, carrier := range device.CarriersOf() {
			key := model.ReadingKey{DeviceId: device.Id, Carrier: carrier}
			value, known := this.values[key]
			if !known {
				// Absent is not zero, here as everywhere: a missing entry is
				// what attaches a device to the root instead of placing it.
				continue
			}
			result[key] = model.Reading{
				DeviceId:  device.Id,
				Carrier:   carrier,
				Value:     value,
				Window:    window,
				FetchedAt: time.Now(),
			}
		}
	}
	return result, nil
}
