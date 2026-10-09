package adminui

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

const (
	maxPayloadBytes       = 1 << 20
	previewBytes          = 120
	previewFetchBytes     = previewBytes + 1
	workflowQueuePrefix   = "pgworkflow:"
	payloadEncodingJSON   = "json"
	payloadEncodingText   = "text"
	payloadEncodingBase64 = "base64"
)

type encodedPayload struct {
	text      string
	encoding  string
	size      int
	truncated bool
}

func encodePayload(head []byte, totalBytes int) encodedPayload {
	truncated := totalBytes > maxPayloadBytes
	shown := head[:min(len(head), maxPayloadBytes)]
	text := shown
	if truncated {
		text = trimPartialRune(shown)
	}
	result := encodedPayload{size: totalBytes, truncated: truncated}
	switch {
	case !truncated && json.Valid(text):
		result.encoding = payloadEncodingJSON
		result.text = string(text)
	case isPrintableText(text):
		result.encoding = payloadEncodingText
		result.text = string(text)
	default:
		result.encoding = payloadEncodingBase64
		result.text = base64.StdEncoding.EncodeToString(shown)
	}
	return result
}

func previewPayload(head []byte, totalBytes int) string {
	if totalBytes == 0 {
		return ""
	}
	shown := head[:min(len(head), previewBytes)]
	text := shown
	suffix := ""
	if totalBytes > previewBytes {
		text = trimPartialRune(shown)
		suffix = "..."
	}
	if isPrintableText(text) {
		return string(text) + suffix
	}
	return "base64:" + base64.StdEncoding.EncodeToString(shown) + suffix
}

func truncateText(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return string(trimPartialRune([]byte(text)[:limit])) + "…"
}

func isPrintableText(value []byte) bool {
	if !utf8.Valid(value) {
		return false
	}
	for _, r := range string(value) {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	return true
}

func trimPartialRune(value []byte) []byte {
	for back := 1; back < utf8.UTFMax && back <= len(value); back++ {
		start := len(value) - back
		if utf8.RuneStart(value[start]) {
			if utf8.FullRune(value[start:]) {
				return value
			}
			return value[:start]
		}
	}
	return value
}

func workflowRunIDOfJob(queueName string, payload []byte) *string {
	if !strings.HasPrefix(queueName, workflowQueuePrefix) {
		return nil
	}
	var decoded struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded.RunID == "" {
		return nil
	}
	return &decoded.RunID
}
