package gologger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
    BaseURL string
    APIKey  string
    HTTP    *http.Client
}

type LogEntry struct {
    Level       string         `json:"level"`
    ServiceName string         `json:"serviceName"`
    Data        map[string]any `json:"data"`
}

func (client *Client) Send(ctx context.Context, entry LogEntry) error {
    body, err := json.Marshal(entry)
    if err != nil {
        return err
    }

    request, err := http.NewRequestWithContext(
        ctx,
        http.MethodPost,
        client.BaseURL+"/logs",
        bytes.NewReader(body),
    )
    if err != nil {
        return err
    }

    request.Header.Set("Content-Type", "application/json")
    request.Header.Set("X-API-Key", client.APIKey)

    httpClient := client.HTTP
    if httpClient == nil {
        httpClient = &http.Client{Timeout: 10 * time.Second}
    }

    response, err := httpClient.Do(request)
    if err != nil {
        return err
    }
    defer response.Body.Close()

    if response.StatusCode == http.StatusTooManyRequests {
        return fmt.Errorf("gologger rate limit exceeded")
    }
    if response.StatusCode < 200 || response.StatusCode >= 300 {
        return fmt.Errorf("gologger returned status %d", response.StatusCode)
    }

    return nil
}