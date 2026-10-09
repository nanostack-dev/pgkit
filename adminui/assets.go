package adminui

import (
	"bytes"
	"compress/gzip"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
)

const minGzipSize = 1024

var compressibleExtensions = map[string]bool{
	".css":  true,
	".html": true,
	".js":   true,
	".json": true,
	".svg":  true,
	".txt":  true,
}

type staticAssets struct {
	files   fs.FS
	mu      sync.Mutex
	gzipped map[string][]byte
}

func newStaticAssets(files fs.FS) *staticAssets {
	return &staticAssets{files: files, gzipped: map[string][]byte{}}
}

func (a *staticAssets) serve(w http.ResponseWriter, r *http.Request, name, cacheControl string) {
	content, err := fs.ReadFile(a.files, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	extension := path.Ext(name)
	if contentType := mime.TypeByExtension(extension); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if cacheControl != "" {
		w.Header().Set("Cache-Control", cacheControl)
	}
	compressible := compressibleExtensions[extension] && len(content) >= minGzipSize
	if compressible {
		w.Header().Add("Vary", "Accept-Encoding")
	}
	if compressible && acceptsGzip(r) {
		if compressed, ok := a.gzip(name, content); ok {
			w.Header().Set("Content-Encoding", "gzip")
			content = compressed
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(content)))
	if r.Method != http.MethodHead {
		_, _ = w.Write(content)
	}
}

func (a *staticAssets) gzip(name string, content []byte) ([]byte, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if compressed, ok := a.gzipped[name]; ok {
		return compressed, true
	}
	var buffer bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buffer, gzip.BestCompression)
	if err != nil {
		return nil, false
	}
	if _, err := writer.Write(content); err != nil {
		return nil, false
	}
	if err := writer.Close(); err != nil {
		return nil, false
	}
	a.gzipped[name] = buffer.Bytes()
	return a.gzipped[name], true
}

func acceptsGzip(r *http.Request) bool {
	for part := range strings.SplitSeq(r.Header.Get("Accept-Encoding"), ",") {
		coding, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if !strings.EqualFold(strings.TrimSpace(coding), "gzip") {
			continue
		}
		quality := strings.ReplaceAll(strings.TrimSpace(params), " ", "")
		return quality != "q=0" && quality != "q=0.0" && quality != "q=0.00" && quality != "q=0.000"
	}
	return false
}
