package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// httpServer owns the in-memory state shared by its HTTP handlers.
type httpServer struct {
	Log *Log
}

// newHTTPServer creates independent state for one HTTP server.
func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// ProduceRequest carries a record to append; the server assigns its offset.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse reports the assigned offset.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest selects an absolute offset, including for the GET request body.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse contains the requested record.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

// handleProduce decodes JSON, appends the record, and returns its assigned offset.
// Malformed input returns 400; append or response encoding failures return 500.
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleConsume reads an offset from the JSON body and returns its record.
// Missing offsets return 404; malformed JSON returns 400.
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err != nil {
		if err == ErrOffsetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ConsumeResponse{Record: record}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// NewHTTPServer routes POST / to append and GET / to read using JSON bodies.
// The caller starts the returned server; this demo has no TLS or authorization.
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()

	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}
