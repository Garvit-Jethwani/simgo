package hosted

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// API serves the hosted service over HTTP:
//
//	POST /jobs        submit a job (JSON Job; id, created are ignored)
//	GET  /jobs/{id}   job status and grouped failures
//	GET  /healthz     liveness
//
// Every request except /healthz needs "Authorization: Bearer <Token>".
type API struct {
	Service *Service
	Token   string
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /jobs", a.auth(a.submit))
	mux.HandleFunc("GET /jobs/{id}", a.auth(a.status))
	return mux
}

func (a *API) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.Token == "" {
			writeError(w, http.StatusServiceUnavailable, errors.New("API token is not configured"))
			return
		}
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(a.Token)) != 1 {
			writeError(w, http.StatusUnauthorized, errors.New("missing or wrong bearer token"))
			return
		}
		next(w, r)
	}
}

func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	var j Job
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&j); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	// The server assigns the ID and creation time.
	j.ID, j.Created = "", time.Time{}
	job, err := a.Service.Submit(r.Context(), j)
	if errors.Is(err, ErrInvalid) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	st, err := a.Service.Status(r.Context(), r.PathValue("id"))
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
