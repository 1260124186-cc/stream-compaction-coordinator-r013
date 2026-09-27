package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
)

func jsonReader(value any) io.Reader {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(data)
}

type testEnvelope struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	} `json:"error"`
}

func decodeEnvelope(t *testing.T, data []byte) testEnvelope {
	t.Helper()
	var envelope testEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("decode error envelope %q: %v", string(data), err)
	}
	return envelope
}
