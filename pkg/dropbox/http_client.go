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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

var (
	ErrEmptyAccessToken = errors.New("access token is required")
	ErrExpiredCursor    = errors.New("dropbox: expired cursor")
	ErrDropboxAPI       = errors.New("dropbox API error")
	ErrNotAFolder       = errors.New("path must point to a directory, not a file")
)

const (
	apiURL     = "https://api.dropboxapi.com/2"
	contentURL = "https://content.dropboxapi.com/2"
	notifyURL  = "https://notify.dropboxapi.com/2"
	tagFolder  = "folder"
)

type HTTPClient struct {
	accessToken string
	httpClient  *http.Client
}

// NewHTTPClient creates a new Dropbox Client with the given access token and longpoll timeout.
// It adds a 90-second buffer to account for Dropbox's jitter in longpoll requests.
// Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder-longpoll
func NewHTTPClient(accessToken string, longpollTimeout time.Duration) (*HTTPClient, error) {
	if accessToken == "" {
		return nil, ErrEmptyAccessToken
	}

	return &HTTPClient{
		accessToken: accessToken,
		httpClient:  &http.Client{Timeout: longpollTimeout + 90*time.Second},
	}, nil
}

func (c *HTTPClient) List(ctx context.Context, path string, recursive bool, limit int) ([]Entry, string, bool, error) {
	reqBody := struct {
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
		Limit     int    `json:"limit"`
	}{path, recursive, limit}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, "", false, fmt.Errorf("marshal request failed: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}

	resp, err := c.makeRequest(ctx, http.MethodPost, apiURL+"/files/list_folder", headers, bytes.NewReader(body), false)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()

	var parsed struct {
		Entries []Entry `json:"entries"`
		Cursor  string  `json:"cursor"`
		HasMore bool    `json:"has_more"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, "", false, fmt.Errorf("decode response failed: %w", err)
	}

	return parsed.Entries, parsed.Cursor, parsed.HasMore, nil
}

func (c *HTTPClient) ListContinue(ctx context.Context, cursor string) ([]Entry, string, bool, error) {
	reqBody := struct {
		Cursor string `json:"cursor"`
	}{cursor}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, "", false, fmt.Errorf("marshal request failed: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}

	resp, err := c.makeRequest(ctx, http.MethodPost, apiURL+"/files/list_folder/continue", headers, bytes.NewReader(body), false)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()

	var parsed struct {
		Entries []Entry `json:"entries"`
		Cursor  string  `json:"cursor"`
		HasMore bool    `json:"has_more"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, "", false, fmt.Errorf("decode response failed: %w", err)
	}

	return parsed.Entries, parsed.Cursor, parsed.HasMore, nil
}

func (c *HTTPClient) Longpoll(ctx context.Context, cursor string, timeout time.Duration) (bool, error) {
	reqBody := struct {
		Cursor  string `json:"cursor"`
		Timeout int    `json:"timeout"`
	}{cursor, int(timeout.Seconds())}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return false, fmt.Errorf("marshal request failed: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json", // No Authorization required for this endpoint
	}

	resp, err := c.makeRequest(ctx, http.MethodPost, notifyURL+"/files/list_folder/longpoll", headers, bytes.NewReader(body), true)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var parsed struct {
		Changes bool `json:"changes"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return false, fmt.Errorf("decode response failed: %w", err)
	}

	return parsed.Changes, nil
}

func (c *HTTPClient) DownloadRange(ctx context.Context, path string, start, length uint64) (io.ReadCloser, error) {
	argHeader, err := json.Marshal(struct {
		Path string `json:"path"`
	}{
		Path: path,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal dropbox-api-arg failed: %w", err)
	}

	headers := map[string]string{
		"Dropbox-API-Arg": string(argHeader),
	}

	// Add Range header if partial download is requested
	if length > 0 {
		headers["Range"] = fmt.Sprintf("bytes=%d-%d", start, start+length-1)
	}

	resp, err := c.makeRequest(ctx, http.MethodPost, contentURL+"/files/download", headers, nil, false)
	if err != nil {
		return nil, err
	}

	// Accept both full and partial responses
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		defer resp.Body.Close()
		return nil, parseError(resp)
	}

	return resp.Body, nil
}

func (c *HTTPClient) VerifyPath(ctx context.Context, path string) (bool, error) {
	reqBody := struct {
		Path string `json:"path"`
	}{path}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return false, fmt.Errorf("marshal request failed: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}

	resp, err := c.makeRequest(ctx, http.MethodPost, apiURL+"/files/get_metadata", headers, bytes.NewReader(body), false)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var parsed struct {
		Tag string `json:".tag"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return false, fmt.Errorf("decode response failed: %w", err)
	}

	if parsed.Tag != tagFolder {
		return false, ErrNotAFolder
	}

	return true, nil
}

// UploadFile uploads a file to remote dropbox filepath.
func (c *HTTPClient) UploadFile(ctx context.Context, filepath string, content []byte) (*UploadFileResponse, error) {
	response := &UploadFileResponse{}
	headers := map[string]string{
		"Content-Type":    "application/octet-stream",
		"Dropbox-API-Arg": fmt.Sprintf(`{"path": "%s","mode": "overwrite","mute": false}`, filepath),
	}
	resp, err := c.makeRequest(ctx, http.MethodPost, contentURL+"/files/upload", headers, bytes.NewReader(content), false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	return response, nil
}

// CreateSession uploads the first chunk of the file and creates a session for remaining chunks.
func (c *HTTPClient) CreateSession(ctx context.Context, content []byte) (*SessionResponse, error) {
	response := &SessionResponse{}
	headers := map[string]string{
		"Content-Type":    "application/octet-stream",
		"Dropbox-API-Arg": `{"close": false}`,
	}
	resp, err := c.makeRequest(ctx, http.MethodPost, contentURL+"/files/upload_session/start", headers, bytes.NewReader(content), false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	return response, nil
}

func (c *HTTPClient) UploadChunk(ctx context.Context, sessionID string, content []byte, offset uint) error {
	cursor := map[string]interface{}{
		"session_id": sessionID,
		"offset":     offset,
	}
	arg := map[string]interface{}{
		"cursor": cursor,
		"close":  false,
	}
	argJSON, err := json.Marshal(arg)
	if err != nil {
		return fmt.Errorf("error marshalling headers: %w", err)
	}

	headers := map[string]string{
		"Content-Type":    "application/octet-stream",
		"Dropbox-API-Arg": string(argJSON),
	}
	resp, err := c.makeRequest(ctx, http.MethodPost, contentURL+"/files/upload_session/append_v2", headers, bytes.NewReader(content), false)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *HTTPClient) CloseSession(ctx context.Context, filepath, sessionid string, offset uint) (*UploadFileResponse, error) {
	cursor := map[string]interface{}{
		"session_id": sessionid,
		"offset":     offset,
	}
	commit := map[string]interface{}{
		"path": filepath,
		"mode": "overwrite",
		"mute": false,
	}
	finishArg := map[string]interface{}{
		"cursor": cursor,
		"commit": commit,
	}
	argJSON, err := json.Marshal(finishArg)
	if err != nil {
		return nil, fmt.Errorf("error marshalling headers: %w", err)
	}

	headers := map[string]string{
		"Content-Type":    "application/octet-stream",
		"Dropbox-API-Arg": string(argJSON),
	}
	response := &UploadFileResponse{}
	resp, err := c.makeRequest(ctx, http.MethodPost, contentURL+"/files/upload_session/finish", headers, nil, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	return response, nil
}

func (c *HTTPClient) Delete(ctx context.Context, filepath string) error {
	req := struct {
		Path string `json:"path"`
	}{
		Path: filepath,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("error marshalling request body: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}
	resp, err := c.makeRequest(ctx, http.MethodPost, apiURL+"/files/delete_v2", headers, bytes.NewReader(body), false)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

//nolint:unparam // method may be used for non-POST in future
func (c *HTTPClient) makeRequest(ctx context.Context, method, url string, headers map[string]string, reqBody io.Reader, skipAuth bool) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	if !skipAuth {
		req.Header.Set("Authorization", "Bearer "+c.accessToken)
	}

	for header, value := range headers {
		req.Header.Set(header, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, parseError(resp)
	}

	return resp, nil
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
