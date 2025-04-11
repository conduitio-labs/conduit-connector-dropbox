// Copyright © 2025 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dropbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var (
	ErrEmptyAccessToken = errors.New("access token is required")
	ErrExpiredCursor    = errors.New("dropbox: expired cursor")
	ErrDropboxAPI       = errors.New("dropbox API error")
)

const (
	apiURL     = "https://api.dropboxapi.com/2"
	contentURL = "https://content.dropboxapi.com/2"
	notifyURL  = "https://notify.dropboxapi.com/2"
)

type HTTPClient struct {
	accessToken string
	httpClient  *http.Client
}

// NewHTTPClient creates a new Dropbox Client with the given access token and longpoll timeout.
// It adds a 90-second buffer to account for Dropbox's jitter in longpoll requests.
func NewHTTPClient(accessToken string, longpollTimeout int) (*HTTPClient, error) {
	if accessToken == "" {
		return nil, ErrEmptyAccessToken
	}

	return &HTTPClient{
		accessToken: accessToken,
		httpClient:  &http.Client{Timeout: time.Duration(longpollTimeout+90) * time.Second},
	}, nil
}

func (c *HTTPClient) List(ctx context.Context, path string, recursive bool) ([]Entry, string, bool, error) {
	req := struct {
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
	}{path, recursive}

	var resp struct {
		Entries []Entry `json:"entries"`
		Cursor  string  `json:"cursor"`
		HasMore bool    `json:"has_more"`
	}

	headers := map[string]string{
		"Authorization": "Bearer " + c.accessToken,
		"Content-Type":  "application/json",
	}

	if err := c.makeRequest(ctx, http.MethodPost, apiURL+"/files/list_folder", headers, req, &resp); err != nil {
		return nil, "", false, err
	}

	return resp.Entries, resp.Cursor, resp.HasMore, nil
}

func (c *HTTPClient) ListContinue(ctx context.Context, cursor string) ([]Entry, string, bool, error) {
	req := struct {
		Cursor string `json:"cursor"`
	}{cursor}

	var resp struct {
		Entries []Entry `json:"entries"`
		Cursor  string  `json:"cursor"`
		HasMore bool    `json:"has_more"`
	}

	headers := map[string]string{
		"Authorization": "Bearer " + c.accessToken,
		"Content-Type":  "application/json",
	}

	if err := c.makeRequest(ctx, http.MethodPost, apiURL+"/files/list_folder/continue", headers, req, &resp); err != nil {
		return nil, "", false, err
	}

	return resp.Entries, resp.Cursor, resp.HasMore, nil
}

func (c *HTTPClient) Longpoll(ctx context.Context, cursor string, timeoutSec int) (bool, error) {
	req := struct {
		Cursor  string `json:"cursor"`
		Timeout int    `json:"timeout"`
	}{cursor, timeoutSec}

	var resp struct {
		Changes bool `json:"changes"`
	}

	header := map[string]string{
		"Content-Type": "application/json",
	}

	if err := c.makeRequest(ctx, http.MethodPost, notifyURL+"/files/list_folder/longpoll", header, req, &resp); err != nil {
		return false, err
	}

	return resp.Changes, nil
}

func (c *HTTPClient) DownloadRange(ctx context.Context, path string, start, length uint64) (io.ReadCloser, error) {
	reqBody := struct {
		Path string `json:"path"`
	}{
		Path: path,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request failed: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		contentURL+"/files/download",
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	req.Header.Set("Dropbox-API-Arg", string(bodyBytes))

	// Set Range header.
	if length > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, parseError(resp)
	}

	return resp.Body, nil
}

func (c *HTTPClient) makeRequest(ctx context.Context, method, url string, headers map[string]string, reqBody, respBody any) error {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request failed: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	for header, value := range headers {
		req.Header.Set(header, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return parseError(resp)
	}

	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("decode response failed: %w", err)
	}

	return nil
}

func parseError(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read error response: %w", err)
	}

	var jsonErr struct {
		ErrorSummary string `json:"error_summary"`
	}
	if err := json.Unmarshal(body, &jsonErr); err == nil && jsonErr.ErrorSummary != "" {
		switch {
		case strings.HasPrefix(jsonErr.ErrorSummary, "reset/"):
			return ErrExpiredCursor
		default:
			return fmt.Errorf("%w: %s", ErrDropboxAPI, jsonErr.ErrorSummary)
		}
	}

	return fmt.Errorf("%w (status %d): %s", ErrDropboxAPI, resp.StatusCode, string(body))
}
