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

// Package config loads the service configuration in one place.
//
// A JSON file supplies the defaults, environment variables override them. No
// other package reads the environment.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	sb_config_hdl "github.com/SENERGY-Platform/go-service-base/config-hdl"
	sb_config_types "github.com/SENERGY-Platform/go-service-base/config-hdl/types"
	struct_logger "github.com/SENERGY-Platform/go-service-base/struct-logger"
)

// KeycloakConfig is the access to the admin API, the one place that needs real
// credentials - the internal admin token does not reach Keycloak.
type KeycloakConfig struct {
	Url          string                 `json:"url" env_var:"KEYCLOAK_URL"`
	Realm        string                 `json:"realm" env_var:"KEYCLOAK_REALM"`
	ClientId     string                 `json:"client_id" env_var:"KEYCLOAK_CLIENT_ID"`
	ClientSecret sb_config_types.Secret `json:"client_secret" env_var:"KEYCLOAK_CLIENT_SECRET"`

	// PageSize is the admin API's max parameter. Its group endpoints paginate
	// and default to a small page.
	PageSize int `json:"page_size" env_var:"KEYCLOAK_PAGE_SIZE"`
}

// KafkaConfig configures the trigger consumers.
//
// ConsumerGroup must be stable across restarts and identical on every replica:
// committed offsets are what decides where reading resumes, and a group per
// instance would turn every replica into its own reader of the whole topic.
type KafkaConfig struct {
	Url             string `json:"url" env_var:"KAFKA_URL"`
	ConsumerGroup   string `json:"consumer_group" env_var:"KAFKA_CONSUMER_GROUP"`
	DeviceTopic     string `json:"device_topic" env_var:"DEVICE_TOPIC"`
	DeviceTypeTopic string `json:"device_type_topic" env_var:"DEVICE_TYPE_TOPIC"`
	GraphTopic      string `json:"graph_topic" env_var:"GRAPH_TOPIC"`
}

// HeuristicConfig steers the structure detection.
type HeuristicConfig struct {
	// ReferenceWindow is how far back the readings that decide a structure are
	// read. Long enough that a meter reporting daily has said something.
	ReferenceWindow sb_config_types.Duration `json:"reference_window" env_var:"REFERENCE_WINDOW"`

	// ContainmentTolerance is how much a child may exceed its parent's
	// remaining capacity and still be placed under it, as a fraction. Reading
	// intervals and rounding do not line up exactly.
	ContainmentTolerance float64 `json:"containment_tolerance" env_var:"CONTAINMENT_TOLERANCE"`

	// ReadingTtl is how long a fetched consumption value stays usable.
	ReadingTtl sb_config_types.Duration `json:"reading_ttl" env_var:"READING_TTL"`

	// NoFlowNodeName labels the node that devices measuring no medium at all
	// collect under - contacts, motion sensors, remotes. A real site has many
	// of them and hanging them off the root buries the two or three meters
	// the flow view is about.
	//
	// Empty switches the collector off: those devices then hang off the root,
	// which is the older behaviour. The label is user-facing, hence
	// configurable; the graph view can also localise it through the fallback
	// translation key this service writes alongside.
	NoFlowNodeName string `json:"no_flow_node_name" env_var:"NO_FLOW_NODE_NAME"`
}

type Config struct {
	ServerPort int                  `json:"server_port" env_var:"SERVER_PORT"`
	Logger     struct_logger.Config `json:"logger"`

	// Cluster-internal addresses. These must not point at the API gateway: the
	// internal admin token this service authenticates with is rejected there.
	DeviceRepositoryUrl string `json:"device_repository_url" env_var:"DEVICE_REPOSITORY_URL"`
	PermissionsV2Url    string `json:"permissions_v2_url" env_var:"PERMISSIONS_V2_URL"`
	TimescaleWrapperUrl string `json:"timescale_wrapper_url" env_var:"TIMESCALE_WRAPPER_URL"`

	Kafka     KafkaConfig     `json:"kafka"`
	Keycloak  KeycloakConfig  `json:"keycloak"`
	Heuristic HeuristicConfig `json:"heuristic"`

	// PermissionsDeviceTopic and PermissionsGraphTopic are the topic ids
	// permissions-v2 keys rights under. They happen to match the Kafka topic
	// names but are a different namespace.
	PermissionsDeviceTopic string `json:"permissions_device_topic" env_var:"PERMISSIONS_DEVICE_TOPIC"`
	PermissionsGraphTopic  string `json:"permissions_graph_topic" env_var:"PERMISSIONS_GRAPH_TOPIC"`

	// ServiceUserId owns every graph this service creates.
	//
	// It must be the subject of the token the service authenticates with.
	// Creating a graph makes the calling user its owner and sole administrator,
	// and permissions-v2 grants no bypass for the admin role when writing
	// permissions - a mismatch leaves the service unable to change the sharing
	// of a graph it created itself. Validate() enforces that it is set; that it
	// matches the token is checked at startup against the parsed token.
	ServiceUserId string `json:"service_user_id" env_var:"SERVICE_USER_ID"`

	GroupPollInterval sb_config_types.Duration `json:"group_poll_interval" env_var:"GROUP_POLL_INTERVAL"`
	ReconcileInterval sb_config_types.Duration `json:"reconcile_interval" env_var:"RECONCILE_INTERVAL"`

	// GroupInclude and GroupExclude filter the realm's groups by path prefix.
	// Include empty means every group; a path matching Exclude is skipped even
	// when Include allows it.
	GroupInclude []string `json:"group_include" env_var:"GROUP_INCLUDE" env_params:"sep=,"`
	GroupExclude []string `json:"group_exclude" env_var:"GROUP_EXCLUDE" env_params:"sep=,"`

	// Enabled false is the kill switch: read, compute and log, write nothing.
	Enabled bool `json:"enabled" env_var:"ENABLED"`

	HttpTimeout   sb_config_types.Duration `json:"http_timeout" env_var:"HTTP_TIMEOUT"`
	HttpAccessLog bool                     `json:"http_access_log" env_var:"HTTP_ACCESS_LOG"`
}

func New(path string) (*Config, error) {
	cfg := Config{
		ServerPort: 8080,
		Logger: struct_logger.Config{
			Handler:    struct_logger.JsonHandlerSelector,
			Level:      struct_logger.LevelInfo,
			TimeFormat: time.RFC3339Nano,
			TimeUtc:    true,
			// Organization, project and level on every record. Required of
			// new services, and only attached when this is on.
			AddMeta: true,
		},
		Kafka: KafkaConfig{
			ConsumerGroup:   "graph-provider",
			DeviceTopic:     "devices",
			DeviceTypeTopic: "device-types",
			GraphTopic:      "graphs",
		},
		Keycloak: KeycloakConfig{
			Realm:    "master",
			PageSize: 100,
		},
		Heuristic: HeuristicConfig{
			ReferenceWindow:      sb_config_types.Duration(30 * 24 * time.Hour),
			ContainmentTolerance: 0.05,
			ReadingTtl:           sb_config_types.Duration(24 * time.Hour),
			NoFlowNodeName:       "Ohne Energiefluss",
		},
		PermissionsDeviceTopic: "devices",
		PermissionsGraphTopic:  "graphs",
		GroupPollInterval:      sb_config_types.Duration(5 * time.Minute),
		ReconcileInterval:      sb_config_types.Duration(time.Hour),
		Enabled:                true,
		HttpTimeout:            sb_config_types.Duration(30 * time.Second),
	}
	// Both custom types need their env parser registered. Without them the
	// loader reflects a plain string onto the field and panics - so the secret,
	// whose only intended source IS the environment, would take the process
	// down on startup. A duration would parse as bare nanoseconds.
	err := sb_config_hdl.Load(&cfg, nil,
		[]sb_config_hdl.EnvTypeParser{
			sb_config_types.SecretEnvTypeParser,
			sb_config_types.DurationEnvTypeParser,
		},
		nil, path)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate reports the configuration errors that would otherwise surface as a
// confusing failure much later.
func (this *Config) Validate() error {
	return this.validate(true)
}

// ValidateDryRun is Validate for a run that starts no Kafka consumers.
//
// Exactly the Kafka requirements are dropped, and nothing else. A dry run
// still reads groups, devices, readings and graphs, so every address and the
// Keycloak credential stay required: a dry run that silently read nothing
// would report an empty plan, which is indistinguishable from a platform with
// nothing to do and is the one wrong answer it must not give.
func (this *Config) ValidateDryRun() error {
	return this.validate(false)
}

func (this *Config) validate(kafka bool) error {
	var errs []error
	required := map[string]string{
		"device_repository_url": this.DeviceRepositoryUrl,
		"permissions_v2_url":    this.PermissionsV2Url,
		"timescale_wrapper_url": this.TimescaleWrapperUrl,
		"keycloak.url":          this.Keycloak.Url,
		"keycloak.realm":        this.Keycloak.Realm,
		"keycloak.client_id":    this.Keycloak.ClientId,
		"service_user_id":       this.ServiceUserId,

		// The one credential the service needs. Without it the process starts,
		// answers every probe and fails the group poll once an hour with a 401
		// - and no graph is ever created. Cheaper to refuse at startup.
		"keycloak.client_secret": this.Keycloak.ClientSecret.Value(),
	}
	if kafka {
		required["kafka.url"] = this.Kafka.Url
		required["kafka.consumer_group"] = this.Kafka.ConsumerGroup
		required["kafka.device_topic"] = this.Kafka.DeviceTopic
		required["kafka.device_type_topic"] = this.Kafka.DeviceTypeTopic
		required["kafka.graph_topic"] = this.Kafka.GraphTopic
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			errs = append(errs, fmt.Errorf("%v is required", name))
		}
	}

	// Two topics sharing a name would collapse into one entry of the
	// topic-to-kind map, and the kind that lost would have its triggers
	// silently reclassified - every device event answered as if it named a
	// graph, and dropped.
	topics := map[string]string{
		this.Kafka.DeviceTopic:     "kafka.device_topic",
		this.Kafka.DeviceTypeTopic: "kafka.device_type_topic",
		this.Kafka.GraphTopic:      "kafka.graph_topic",
	}
	if kafka && len(topics) != 3 {
		errs = append(errs, errors.New("kafka.device_topic, kafka.device_type_topic and kafka.graph_topic must name three different topics"))
	}
	if this.Heuristic.ContainmentTolerance < 0 || this.Heuristic.ContainmentTolerance >= 1 {
		errs = append(errs, errors.New("heuristic.containment_tolerance must be in [0, 1)"))
	}
	if this.Heuristic.ReferenceWindow <= 0 {
		errs = append(errs, errors.New("heuristic.reference_window must be positive"))
	}
	if this.GroupPollInterval <= 0 {
		errs = append(errs, errors.New("group_poll_interval must be positive"))
	}
	if this.ReconcileInterval <= 0 {
		errs = append(errs, errors.New("reconcile_interval must be positive"))
	}
	return errors.Join(errs...)
}

// GroupAllowed reports whether a group path passes the include and exclude
// filters. Exclude wins.
func (this *Config) GroupAllowed(path string) bool {
	for _, prefix := range this.GroupExclude {
		if prefix != "" && strings.HasPrefix(path, prefix) {
			return false
		}
	}
	if len(this.GroupInclude) == 0 {
		return true
	}
	for _, prefix := range this.GroupInclude {
		if prefix != "" && strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}
