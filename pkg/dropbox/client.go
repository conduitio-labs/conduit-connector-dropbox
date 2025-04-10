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
	"fmt"
	"io"
	"net/http"
	"time"
)

var ErrEmptyAccessToken = fmt.Errorf("access token is required")

const (
	apiURL     = "https://api.dropboxapi.com/2"
	contentURL = "https://content.dropboxapi.com/2"
	notifyURL  = "https://notify.dropboxapi.com/2"
)

type DropboxClient struct {
	accessToken string
	httpClient  *http.Client
}

func NewDropboxClient(accessToken string, longpollTimeout int) (*DropboxClient, error) {
	if accessToken == "" {
		return nil, ErrEmptyAccessToken
	}

	return &DropboxClient{
		accessToken: accessToken,
		httpClient:  &http.Client{Timeout: time.Hour},
	}, nil
}

func (c *DropboxClient) ListFolder(ctx context.Context, path string, recursive bool) ([]Entry, string, bool, error) {
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

func (c *DropboxClient) ListFolderContinue(ctx context.Context, cursor string) ([]Entry, string, bool, error) {
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

func (c *DropboxClient) ListFolderLongpoll(ctx context.Context, cursor string, timeoutSec int) (bool, string, error) {
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
		return false, "", err
	}

	return resp.Changes, cursor, nil
}

func (c *DropboxClient) DownloadRange(ctx context.Context, path string, start, length uint64) (io.ReadCloser, error) {
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

func (c *DropboxClient) makeRequest(ctx context.Context, method, url string, headers map[string]string, reqBody, respBody any) error {
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
	var errResp struct {
		ErrorSummary string `json:"error_summary"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		return fmt.Errorf("API error (status %d)", resp.StatusCode)
	}

	return fmt.Errorf("API error: %s", errResp.ErrorSummary)
}
