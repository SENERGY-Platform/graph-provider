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

package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sb_config_types "github.com/SENERGY-Platform/go-service-base/config-hdl/types"
)

func complete() *Config {
	cfg, err := New("")
	if err != nil {
		panic(err)
	}
	cfg.DeviceRepositoryUrl = "http://device-repository"
	cfg.PermissionsV2Url = "http://permissions-v2"
	cfg.TimescaleWrapperUrl = "http://timescale-wrapper"
	cfg.Kafka.Url = "kafka:9092"
	cfg.Keycloak.Url = "http://keycloak"
	cfg.Keycloak.ClientId = "graph-provider"
	cfg.Keycloak.ClientSecret = sb_config_types.Secret("secret")
	cfg.ServiceUserId = "dd69ea0d-f553-4336-80f3-7f4567f85c7b"
	return cfg
}

func TestDefaultsAreUsable(t *testing.T) {
	cfg := complete()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the defaults plus the required addresses must validate: %v", err)
	}
	if time.Duration(cfg.Heuristic.ReferenceWindow) != 30*24*time.Hour {
		t.Errorf("reference window: %v", cfg.Heuristic.ReferenceWindow)
	}
	if cfg.Kafka.ConsumerGroup == "" {
		t.Error("the consumer group must have a default; an empty one would make every replica its own reader")
	}
	if !cfg.Enabled {
		t.Error("the kill switch is off by default")
	}
}

func TestValidateNamesEveryMissingField(t *testing.T) {
	cfg, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("an empty configuration must not validate")
	}
	for _, name := range []string{
		"device_repository_url", "permissions_v2_url", "timescale_wrapper_url",
		"kafka.url", "keycloak.url", "keycloak.client_id", "keycloak.client_secret",
		"service_user_id",
	} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("expected %v to be named, got: %v", name, err)
		}
	}
}

func TestValidateRejectsNonsensicalNumbers(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"negative tolerance":  func(c *Config) { c.Heuristic.ContainmentTolerance = -0.1 },
		"tolerance of one":    func(c *Config) { c.Heuristic.ContainmentTolerance = 1 },
		"empty window":        func(c *Config) { c.Heuristic.ReferenceWindow = 0 },
		"no group poll":       func(c *Config) { c.GroupPollInterval = 0 },
		"no reconcile ticker": func(c *Config) { c.ReconcileInterval = sb_config_types.Duration(-time.Second) },
	} {
		cfg := complete()
		mutate(cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%v must be rejected", name)
		}
	}
}

func TestGroupAllowedExcludeWinsOverInclude(t *testing.T) {
	cfg := complete()
	cfg.GroupInclude = []string{"/customers"}
	cfg.GroupExclude = []string{"/customers/internal"}

	cases := map[string]bool{
		"/customers/acme":         true,
		"/customers/acme/werk":    true,
		"/customers/internal/ops": false,
		"/platform/ops":           false,
		"/customers":              true,
	}
	for path, want := range cases {
		if got := cfg.GroupAllowed(path); got != want {
			t.Errorf("%v: want %v, got %v", path, want, got)
		}
	}
}

func TestGroupAllowedWithoutIncludeAllowsEverythingNotExcluded(t *testing.T) {
	cfg := complete()
	cfg.GroupExclude = []string{"/internal"}
	if !cfg.GroupAllowed("/acme") {
		t.Error("an empty include list means every group")
	}
	if cfg.GroupAllowed("/internal/ops") {
		t.Error("exclude must still apply")
	}
}

// The client secret is the one value in here that must never reach a log line
// or a diagnostic dump. config-hdl's secret type is what guarantees that, so
// the guarantee is worth a test of its own.
func TestClientSecretIsMaskedEverywhere(t *testing.T) {
	const secret = "super-secret-value"
	cfg := complete()
	cfg.Keycloak.ClientSecret = sb_config_types.Secret(secret)
	if cfg.Keycloak.ClientSecret.Value() != secret {
		t.Fatal("the secret must still be readable by the code that needs it")
	}

	rendered, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"json":   string(rendered),
		"%v":     fmt.Sprintf("%v", cfg),
		"%+v":    fmt.Sprintf("%+v", cfg),
		"%s":     fmt.Sprintf("%s", cfg.Keycloak.ClientSecret),
		"nested": fmt.Sprintf("%+v", cfg.Keycloak),
	} {
		if strings.Contains(text, secret) {
			t.Errorf("the client secret leaked into %v output", name)
		}
	}
}

// Two topics under one name collapse into a single entry of the topic-to-kind
// map, and the kind that loses has its triggers silently reclassified - every
// device event answered as if it named a graph, and dropped.
func TestValidateRejectsDuplicateTopics(t *testing.T) {
	cfg := complete()
	cfg.Kafka.GraphTopic = cfg.Kafka.DeviceTopic
	err := cfg.Validate()
	if err == nil {
		t.Fatal("two topics with the same name must be rejected")
	}
	if !strings.Contains(err.Error(), "three different topics") {
		t.Errorf("the error should say what is wrong, got: %v", err)
	}
}

func TestValidateRequiresEveryTopic(t *testing.T) {
	for name, clear := range map[string]func(*Config){
		"kafka.device_topic":      func(c *Config) { c.Kafka.DeviceTopic = "" },
		"kafka.device_type_topic": func(c *Config) { c.Kafka.DeviceTypeTopic = "" },
		"kafka.graph_topic":       func(c *Config) { c.Kafka.GraphTopic = "" },
	} {
		cfg := complete()
		clear(cfg)
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("expected %v to be required, got: %v", name, err)
		}
	}
}

// The committed config.json writes durations as strings ("720h"). A plain
// time.Duration field cannot read those - the whole file fails to unmarshal and
// the service never starts. This pins the type that can.
func TestDurationsLoadFromStringsAndSurviveARoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"heuristic":{"reference_window":"720h","reading_ttl":"24h"},` +
		`"group_poll_interval":"5m","reconcile_interval":"1h","http_timeout":"30s"}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := New(path)
	if err != nil {
		t.Fatalf("a config with string durations must load: %v", err)
	}
	for name, got := range map[string]struct {
		have sb_config_types.Duration
		want time.Duration
	}{
		"reference_window":    {cfg.Heuristic.ReferenceWindow, 720 * time.Hour},
		"reading_ttl":         {cfg.Heuristic.ReadingTtl, 24 * time.Hour},
		"group_poll_interval": {cfg.GroupPollInterval, 5 * time.Minute},
		"reconcile_interval":  {cfg.ReconcileInterval, time.Hour},
		"http_timeout":        {cfg.HttpTimeout, 30 * time.Second},
	} {
		if time.Duration(got.have) != got.want {
			t.Errorf("%v: want %v, got %v", name, got.want, time.Duration(got.have))
		}
	}

	// And back out as a string, so a dumped config is readable rather than a
	// pile of nanoseconds.
	rendered, err := json.Marshal(cfg.Heuristic)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rendered), `"720h0m0s"`) {
		t.Errorf("a duration must marshal as a string, got %v", string(rendered))
	}
}

// The env override has to parse "720h" too, which needs the type parser
// registered in New. Without it the value is read as bare nanoseconds.
func TestDurationEnvOverrideParsesAHumanValue(t *testing.T) {
	t.Setenv("REFERENCE_WINDOW", "168h")
	cfg, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(cfg.Heuristic.ReferenceWindow) != 168*time.Hour {
		t.Errorf("want 168h, got %v", time.Duration(cfg.Heuristic.ReferenceWindow))
	}
}

// The secret's only intended source is the environment. Without its env parser
// registered, the loader reflects a plain string onto the types.Secret field
// and panics - the process dies on startup, on the one path that is meant to
// be used.
func TestSecretLoadsFromTheEnvironment(t *testing.T) {
	t.Setenv("KEYCLOAK_CLIENT_SECRET", "from-the-environment")
	cfg, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Keycloak.ClientSecret.Value(); got != "from-the-environment" {
		t.Errorf("want the value from the environment, got %q", got)
	}
}

// Every field the launch configuration sets, loaded together from the
// environment on top of the committed file. This is the combination that
// actually runs, and the one that panicked before the parsers were registered.
func TestTheEnvironmentTheLaunchConfigurationSetsLoads(t *testing.T) {
	for name, value := range map[string]string{
		"DEVICE_REPOSITORY_URL":  "http://api.device-repository.svc.cluster.local:8080",
		"PERMISSIONS_V2_URL":     "http://permv2.permissions.svc.cluster.local:8080",
		"TIMESCALE_WRAPPER_URL":  "http://timescale-wrapper.timescale.svc.cluster.local:8080",
		"KAFKA_URL":              "kafka.kafka.svc.cluster.local:9092",
		"KEYCLOAK_URL":           "http://keycloak.keycloak.svc.cluster.local:8080/auth",
		"KEYCLOAK_REALM":         "master",
		"KEYCLOAK_CLIENT_ID":     "graph-provider",
		"KEYCLOAK_CLIENT_SECRET": "s3cr3t",
		"SERVICE_USER_ID":        "dd69ea0d-f553-4336-80f3-7f4567f85c7b",
		"LOGGER_HANDLER":         "text",
		"LOGGER_LEVEL":           "info",
		"ENABLED":                "false",
	} {
		t.Setenv(name, value)
	}

	cfg, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	if err = cfg.Validate(); err != nil {
		t.Fatalf("this environment must validate: %v", err)
	}
	if cfg.Enabled {
		t.Error("ENABLED=false must switch writing off")
	}
	if cfg.Keycloak.ClientSecret.Value() != "s3cr3t" {
		t.Error("the secret must arrive")
	}
	if !cfg.GroupAllowed("/anything") {
		t.Error("an empty include list means every group")
	}
}

// An env file sets a variable to the empty string, and the loader parses that
// rather than treating it as unset. Three of this service's variables fail on
// it, and the messages point nowhere useful - so the template comments those
// lines out instead of setting them empty. This is what stops someone
// "tidying" the template by uncommenting them.
func TestAnEmptyEnvValueIsNotTheSameAsUnset(t *testing.T) {
	for _, name := range []string{"GROUP_INCLUDE", "GROUP_EXCLUDE", "ENABLED"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "")
			if _, err := New(""); err == nil {
				t.Errorf("%v= is expected to fail to load; if this now works, "+
					"the template in .vscode/dev.env.example can set it empty", name)
			}
		})
	}
}
