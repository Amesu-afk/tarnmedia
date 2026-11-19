//go:build simulcastloss

package sfu

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
)

// This file is compiled only with `-tags simulcastloss`. It is deliberately
// excluded from normal and release builds: the endpoint and the drop path exist
// solely for the end-to-end congestion run described in docs/sfu-simulcast.md.
var testLossPercent atomic.Int64

var testLossControl = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Percent int `json:"percent"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&body); err != nil {
		http.Error(w, "invalid loss request", http.StatusBadRequest)
		return
	}
	if body.Percent < 0 {
		body.Percent = 0
	}
	if body.Percent > 100 {
		body.Percent = 100
	}
	testLossPercent.Store(int64(body.Percent))
	w.WriteHeader(http.StatusNoContent)
})

func testDropForwardedPacket() bool {
	percent := testLossPercent.Load()
	return percent > 0 && rand.IntN(100) < int(percent)
}

// lossInterceptor sits at the innermost position of the local-stream writer
// chain: the transport-wide CC header and the GCC send history are applied by
// outer interceptors first, then this one discards the packet instead of
// handing it to DTLS. The subscriber therefore reports a real gap in TWCC and
// receiver reports, while the estimator has a matching send record.
type lossInterceptor struct {
	interceptor.NoOp
}

func (i *lossInterceptor) BindLocalStream(_ *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
		if testDropForwardedPacket() {
			return header.MarshalSize() + len(payload), nil
		}
		return writer.Write(header, payload, attributes)
	})
}

type lossFactory struct{}

func (lossFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &lossInterceptor{}, nil
}

func lossInterceptorFactory() interceptor.Factory { return lossFactory{} }
