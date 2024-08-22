package driver

import (
	"net/http"
	"net/http/httptrace"
	"time"
)

// SelfVersion is the git tag version or git short hash
var SelfVersion string

var CurrentTest string

type Tracker struct {
	Start    time.Time
	ReqWrote time.Time
	Method   string
}

// GotFirstResponseByte is called when the first byte of the response
// headers is available.
func (tr *Tracker) GotFirstResponseByte() {
	if tr.ReqWrote.After(tr.Start) {
		promTTFBLatency.WithLabelValues(CurrentTest, tr.Method).Observe(float64(time.Since(tr.ReqWrote).Milliseconds()))
	}
}

// WroteRequest is called with the result of writing the
// request and any body. It may be called multiple times
// in the case of retried requests.
func (tr *Tracker) WroteRequest(wroteRequest httptrace.WroteRequestInfo) {
	tr.ReqWrote = time.Now()
}

type GBTransport struct {
	Base http.RoundTripper
}

func (gt GBTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", "Cloudian-Gosbench/"+SelfVersion)
	track := &Tracker{
		Start:  time.Now(),
		Method: req.Method,
	}
	//Create HttpTrace
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: track.GotFirstResponseByte,
		WroteRequest:         track.WroteRequest,
	}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))
	return gt.Base.RoundTrip(req)
}

// Housekeeping Transport
type HKTransport struct {
	Base http.RoundTripper
}

func (ht HKTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", "Cloudian-Gosbench/"+SelfVersion)
	return ht.Base.RoundTrip(req)
}
