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

package api

// This file carries nothing but the swagger generate directive, kept out of
// api.go so that file stays about routes rather than tooling.
//
// The instance name is specific to this service so the generated docs
// package cannot collide if it is ever vendored alongside another service's
// generated docs in the same binary.
//
//go:generate go tool swag init --instanceName graphprovider -o ../../docs --parseDependency -d . -g api.go
