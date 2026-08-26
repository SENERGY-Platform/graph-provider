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

package keycloak

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// expiryMargin is subtracted from a token's lifetime.
//
// A token that is still valid when the request leaves can be expired by the
// time Keycloak looks at it, and the failure would surface as a 401 in the
// middle of a poll.
const expiryMargin = 30 * time.Second

// credential holds a value that must never be printed: the client secret and
// the access token.
//
// A function rather than a string, because fmt walks a struct by reflection
// and reaches unexported fields as well. A masked string type does not help
// there - fmt may not call String on a value it cannot interface, so it prints
// the raw characters instead - and hiding the value behind a pointer only
// works for some verbs. A closure is the one shape reflection cannot walk
// into: every verb prints the function's address, at any nesting depth. Value
// is the single way out and is called exactly where the value goes into a
// request.
type credential func() string

// hold wraps a value so that only Value gets it back out.
func hold(value string) credential {
	return func() string { return value }
}

// Value returns the held value, or the empty string when nothing is held.
func (this credential) Value() string {
	if this == nil {
		return ""
	}
	return this()
}

// maxLifetimeSeconds is the largest lifetime the expiry arithmetic can carry.
//
// A server-supplied number is multiplied by time.Second, and that overflows an
// int64 duration well below the range of what a JSON number can hold. An
// overflow would wrap into a negative or an absurdly distant expiry, so the
// value is capped instead - 292 years is not a lifetime anyone loses anything
// by rounding down.
const maxLifetimeSeconds = int64(math.MaxInt64) / int64(time.Second)

// tokenResponse is the part of the token endpoint's answer this client reads.
//
// The token is a plain string here because that is what the decoder writes.
// The value lives in this local just long enough to be wrapped; nothing formats
// it and nothing keeps it.
//
// ExpiresIn stays raw so a badly typed lifetime does not fail the whole decode.
// The answer is machine-written but not always by Keycloak: a proxy that
// re-serialises it can turn the integer into a float or a string, and the
// generic decode error that produced would repeat once per poll with nothing an
// operator could act on - this package does not log, and the decoder's own
// message may not be shown because it can quote a token answer.
type tokenResponse struct {
	AccessToken string          `json:"access_token"`
	ExpiresIn   json.RawMessage `json:"expires_in"`
}

// tokenGrant is one client credentials grant in flight.
//
// done is closed when token and err are final; neither is read before that.
type tokenGrant struct {
	done  chan struct{}
	token credential
	err   error
}

// accessToken returns a usable access token, fetching one when the cached token
// is gone or about to expire.
//
// Concurrent callers share one grant rather than each starting their own, but
// they wait on a channel and not on the mutex: sync.Mutex cannot be waited on
// with a context, so a caller queued behind a grant would ignore its own
// deadline and return only when the http timeout fired - which delays a
// shutdown by up to that timeout per caller.
func (this *Client) accessToken(ctx context.Context) (string, error) {
	this.tokenMux.Lock()
	if this.token != nil && this.now().Before(this.tokenExpiry) {
		token := this.token
		this.tokenMux.Unlock()
		return token.Value(), nil
	}
	// Checked before a grant is started rather than at the top: a caller that
	// is already gone must not cost a request, and one that arrives with a
	// cached token still gets served.
	if err := ctx.Err(); err != nil {
		this.tokenMux.Unlock()
		return "", err
	}
	grant := this.grant
	if grant == nil {
		grant = &tokenGrant{done: make(chan struct{})}
		this.grant = grant
		go this.runGrant(ctx, grant)
	}
	this.tokenMux.Unlock()

	select {
	case <-grant.done:
	case <-ctx.Done():
		// The grant keeps running for the callers that are still waiting.
		return "", ctx.Err()
	}
	if grant.err != nil {
		return "", grant.err
	}
	return grant.token.Value(), nil
}

// runGrant performs the grant of one tokenGrant and caches its result.
//
// The context is detached from the caller that happened to start it. That
// caller may give up while others are still waiting, and a grant that died with
// it would leave them to start another one. What bounds this goroutine is the
// http client's own timeout, which covers the request including the body read.
func (this *Client) runGrant(ctx context.Context, grant *tokenGrant) {
	token, expiresIn, err := this.requestToken(context.WithoutCancel(ctx))

	this.tokenMux.Lock()
	// Cleared before done is closed, so a caller arriving in between either
	// sees the fresh token or starts a grant of its own - never waits on a
	// grant that is already over.
	this.grant = nil
	if err == nil {
		lifetime := time.Duration(expiresIn)*time.Second - expiryMargin
		if lifetime < 0 {
			// Either a very short-lived token or an answer without expires_in.
			// It is used for this request and not cached for the next one.
			lifetime = 0
		}
		this.token = token
		this.tokenExpiry = this.now().Add(lifetime)
	}
	this.tokenMux.Unlock()

	grant.token = token
	grant.err = err
	close(grant.done)
}

// requestToken runs one client credentials grant.
func (this *Client) requestToken(ctx context.Context) (credential, int64, error) {
	path := "/realms/" + url.PathEscape(this.realm) + "/protocol/openid-connect/token"

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", this.clientId)
	form.Set("client_secret", this.secret.Value())

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, this.baseUrl+path, strings.NewReader(form.Encode()))
	if err != nil {
		// Deliberately not wrapped: what a request that cannot even be built
		// fails on is the URL or the body, and the body is the secret.
		return nil, 0, fmt.Errorf("unable to build keycloak token request for %v", path)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")

	response, err := this.http.Do(request)
	if err != nil {
		return nil, 0, transportError(path, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, 0, statusError{Status: response.StatusCode, Path: path}
	}

	body := tokenResponse{}
	err = json.NewDecoder(response.Body).Decode(&body)
	if err != nil {
		// The decoder's message can quote what it choked on, and what it choked
		// on is a token answer.
		return nil, 0, fmt.Errorf("unable to decode keycloak token answer of %v", path)
	}
	if body.AccessToken == "" {
		return nil, 0, errors.New("keycloak token answer carries no access token")
	}
	expiresIn, err := lifetimeSeconds(body.ExpiresIn)
	if err != nil {
		return nil, 0, err
	}
	return hold(body.AccessToken), expiresIn, nil
}

// lifetimeSeconds reads expires_in out of the raw field.
//
// A float and a numeric string are both accepted - json.Number covers either -
// and truncating a fractional second only refreshes earlier. Absent and null
// are not errors: they say nothing about the lifetime, and 0 is the fail-safe
// answer to that, one grant per poll and no caching. Anything else is an error
// that names the field and its JSON type and never its content: the same body
// carries the access token, and a decoder message that quotes what it choked on
// has quoted a token before.
func lifetimeSeconds(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	number := json.Number("")
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, fmt.Errorf("keycloak token answer carries expires_in as %v, want a number", jsonKind(raw))
	}
	seconds, err := number.Float64()
	if err != nil {
		return 0, fmt.Errorf("keycloak token answer carries expires_in as %v out of range", jsonKind(raw))
	}
	if seconds > float64(maxLifetimeSeconds) {
		return maxLifetimeSeconds, nil
	}
	if seconds < float64(-maxLifetimeSeconds) {
		return -maxLifetimeSeconds, nil
	}
	return int64(seconds), nil
}

// jsonKind names the type of a raw JSON value without revealing the value.
func jsonKind(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "nothing"
	}
	switch raw[0] {
	case '"':
		return "a string"
	case '{':
		return "an object"
	case '[':
		return "an array"
	case 't', 'f':
		return "a boolean"
	case 'n':
		return "null"
	default:
		return "a number"
	}
}
