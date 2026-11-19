//go:build !simulcastloss

package sfu

import (
	"net/http"

	"github.com/pion/interceptor"
)

// testLossControl is nil in normal builds. The `simulcastloss` build tag
// installs a loopback-only control endpoint and a packet-loss interceptor so
// the browser smoke harness can exercise real congestion without OS-level
// shapers.
var testLossControl http.Handler

type noopLossFactory struct{}

func (noopLossFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &interceptor.NoOp{}, nil
}

func lossInterceptorFactory() interceptor.Factory { return noopLossFactory{} }
