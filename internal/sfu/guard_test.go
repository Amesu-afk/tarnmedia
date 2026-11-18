package sfu

import "testing"

// Pion invokes track, ICE and state callbacks on goroutines we do not own. A
// panic there used to crash the process together with every active call; the
// guard must swallow it and keep the rest of the SFU running.
func TestGuardPanicRecovers(t *testing.T) {
	func() {
		defer guardPanic("test")
		panic("boom")
	}()
}
