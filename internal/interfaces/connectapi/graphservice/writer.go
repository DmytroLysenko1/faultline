package graphservice

import "net/http"

const (
	headerContentEncoding = "Content-Encoding"
	headerGRPCEncoding    = "Grpc-Encoding"
	encodingIdentity      = "identity"
)

type shrinkingWriter struct {
	http.ResponseWriter
	held *reservation
}

func (w *shrinkingWriter) Write(body []byte) (int, error) {
	w.held.shrink(compressed(w.Header()))
	return w.ResponseWriter.Write(body)
}

func (w *shrinkingWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *shrinkingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func compressed(header http.Header) bool {
	for _, name := range []string{headerContentEncoding, headerGRPCEncoding} {
		if encoding := header.Get(name); encoding != "" && encoding != encodingIdentity {
			return true
		}
	}
	return false
}
